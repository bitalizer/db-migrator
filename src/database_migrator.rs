use crate::config::SettingsConfig;
use crate::database_extractor::{fetch_table_data, DatabaseExtractor};
use crate::database_inserter::DatabaseInserter;
use crate::helpers::format_snake_case;
use crate::mappings::Mappings;
use crate::schema::{ColumnSchema, Constraint};
use anyhow::{bail, Context, Result};

use futures::future::join_all;
use futures::TryStreamExt;
use log::info;
use std::sync::Arc;

use tokio::spawn;
use tokio::sync::Semaphore;

use tokio::time::Instant;

pub struct DatabaseMigrator {
    extractor: DatabaseExtractor,
    inserter: DatabaseInserter,
    settings: SettingsConfig,
    mappings: Mappings,
    max_concurrent_tasks: usize,
}

struct MigrationResult {
    table_name: String,
    schema: Vec<ColumnSchema>,
}

impl DatabaseMigrator {
    pub fn new(
        extractor: DatabaseExtractor,
        inserter: DatabaseInserter,
        settings: SettingsConfig,
        mappings: Mappings,
        max_concurrent_tasks: usize,
    ) -> Self {
        DatabaseMigrator {
            extractor,
            inserter,
            settings,
            mappings,
            max_concurrent_tasks,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Running table migrator");

        let config_send_packet_size = self.settings.max_packet_bytes;
        let max_allowed_packet = self.inserter.get_max_allowed_packet().await?;

        check_packet_size(config_send_packet_size, max_allowed_packet).await?;

        migrate_tables(
            &mut self.extractor,
            &mut self.inserter,
            &mut self.mappings,
            &mut self.settings,
            self.max_concurrent_tasks,
        )
        .await?;

        Ok(())
    }
}

pub async fn migrate_tables(
    extractor: &mut DatabaseExtractor,
    inserter: &mut DatabaseInserter,
    mappings: &mut Mappings,
    settings: &mut SettingsConfig,
    max_concurrent_tasks: usize,
) -> Result<()> {
    // Fetch the list of tables
    let mut tables = extractor.fetch_tables().await?;

    if tables.is_empty() {
        bail!("No tables to process");
    }

    // Filter and keep only the whitelisted tables
    tables.retain(|table| settings.whitelisted_tables.contains(table));

    if tables.is_empty() {
        bail!("No tables to process after filtering whitelisted tables");
    }

    info!("Tables to migrate: {}", tables.join(", "));

    let start_time = Instant::now();

    // Create a semaphore to limit the number of concurrent tasks
    let semaphore = Arc::new(Semaphore::new(max_concurrent_tasks));

    // Spawn a task for each table to fetch the rows concurrently
    let mut migration_tasks = vec![];

    // Spawn a task for each table to fetch the rows concurrently
    for table in tables {
        // Clone the shared semaphore for each task
        let semaphore_clone = Arc::clone(&semaphore);

        let extractor = extractor.clone();
        let mut inserter = inserter.clone();
        let mappings = mappings.clone();
        let settings = settings.clone();

        // Spawn a task for each table
        let task = spawn(async move {
            // Acquire a semaphore permit before starting the task
            let permit = semaphore_clone
                .acquire()
                .await
                .expect("Failed to acquire semaphore permit");

            let result = migrate_table(&extractor, &mut inserter, &mappings, &settings, &table)
                .await
                .expect("Failed to migrate");

            // Release the semaphore permit when the task is done successfully
            drop(permit);
            Ok(result)
        });

        migration_tasks.push(task);
    }

    // Wait for all tasks to complete
    let migration_results: Vec<Result<MigrationResult>> = join_all(migration_tasks).await;

    // Step 3: Run alter table constraints for each migration result using map
    let alter_table_tasks: Vec<_> = migration_results
        .iter()
        .map(|migration_result| {
            inserter.alter_table_constraints(&migration_result.table_name, &migration_result.schema)
        })
        .collect();

    // Step 4: Wait for all alter table tasks to complete using join_all
    join_all(alter_table_tasks).await;

    let end_time = Instant::now();

    info!(
        "Migration finished, total time took: {}s",
        end_time.saturating_duration_since(start_time).as_secs_f32()
    );

    Ok(())
}

async fn migrate_table(
    extractor: &DatabaseExtractor,
    inserter: &mut DatabaseInserter,
    mappings: &Mappings,
    settings: &SettingsConfig,
    table_name: &String,
) -> Result<MigrationResult> {
    info!("Migrating table: {}", table_name);

    let start_time = Instant::now();

    // Fetch table schema
    let table_schema = extractor.clone().get_table_schema(table_name).await?;

    let output_table_name: String = if settings.format_snake_case {
        format_snake_case(table_name)
    } else {
        table_name.clone()
    };

    let mapped_schema = map_table_schema(mappings, &table_schema, settings.format_snake_case);

    // Create or truncate in the output database
    inserter
        .create_or_truncate_table(&output_table_name, &mapped_schema, settings.reset_tables)
        .await?;

    // Migrate rows from the table
    let migrated_count = migrate_table_rows(
        extractor.clone(),
        inserter,
        settings.clone(),
        table_name,
        &output_table_name,
        &mapped_schema,
    )
    .await?;

    let end_time = Instant::now();
    info!(
        "Table {} migrated, rows: {}, took: {}s",
        table_name,
        migrated_count,
        end_time.saturating_duration_since(start_time).as_secs_f32()
    );

    Ok(MigrationResult {
        table_name: output_table_name,
        schema: mapped_schema,
    })
}

async fn migrate_table_rows(
    extractor: DatabaseExtractor,
    inserter: &mut DatabaseInserter,
    settings: SettingsConfig,
    input_table: &str,
    output_table: &str,
    mapped_schema: &[ColumnSchema],
) -> Result<usize> {
    info!("Migrating {} rows", input_table);

    const RESERVED_BYTES: usize = 10;

    let insert_statement = generate_insert_statement(output_table, mapped_schema);

    let mut conn = extractor.pool.get().await?;
    let mut stream = fetch_table_data(&mut conn, input_table).await?;

    let mut insert_query = String::with_capacity(settings.max_packet_bytes);
    let mut total_bytes = insert_statement.len();
    let mut transaction_count = 0;
    let mut total_transaction_count = 0; //Track the row count

    while let Some(row_values) = stream.try_next().await? {
        let values = row_values.join(", ");
        let value_set = format!("({})", values);
        let value_set_bytes = value_set.len();

        if RESERVED_BYTES + total_bytes + value_set_bytes > settings.max_packet_bytes {
            execute_batch(inserter, &insert_query, transaction_count).await?;
            total_transaction_count += transaction_count;

            insert_query.clear();
            total_bytes = insert_statement.len();
            transaction_count = 0;
        }

        if !insert_query.is_empty() {
            insert_query.push(',');
            total_bytes += 1;
        }

        if transaction_count == 0 {
            insert_query.push_str(&insert_statement);
        }

        insert_query.push_str(&value_set);
        total_bytes += value_set_bytes;
        transaction_count += 1;
    }

    if transaction_count > 0 {
        // If there are remaining rows in the insert_query, execute them
        execute_batch(inserter, &insert_query, transaction_count).await?;
        total_transaction_count += transaction_count;
    }

    Ok(total_transaction_count)
}

async fn execute_batch(
    inserter: &mut DatabaseInserter,
    insert_query: &String,
    transaction_count: usize,
) -> Result<()> {
    if !insert_query.is_empty() {
        let cloned_insert_query = Arc::new(insert_query.clone());

        let start_time = Instant::now();

        let query_str = cloned_insert_query.as_str();
        debug!(
            "Sending {} bytes batch with {} transactions",
            query_str.len(),
            transaction_count
        );

        let execution_result = inserter
            .execute_transactional_query(query_str)
            .await
            .context("Transaction execution");

        match execution_result {
            Ok(_) => {
                let end_time = Instant::now();
                debug!(
                    "Executed batch with {} transactions, bytes: {}, took: {}s",
                    transaction_count,
                    query_str.len(),
                    end_time.saturating_duration_since(start_time).as_secs_f32()
                );
            }
            Err(err) => {
                error!("Transaction execution failed: {}", err);
            }
        }
    }

    Ok(())
}

fn map_table_schema(
    mappings: &Mappings,
    table_schema: &[ColumnSchema],
    format: bool,
) -> Vec<ColumnSchema> {
    table_schema
        .iter()
        .map(|column| {
            let mapping = mappings
                .get(&column.data_type)
                .unwrap_or_else(|| panic!("Mapping not found for data type: {}", column.data_type));

            let new_column_name = if format {
                format_snake_case(&column.column_name)
            } else {
                column.column_name.clone()
            };

            let new_constraints = column.constraints.clone();
            let new_data_type = mapping.to_type.clone();

            // Check if new_constraints contain foreign key and format snake case
            let updated_constraints = if let Some(new_constraints) = new_constraints {
                match new_constraints {
                    Constraint::ForeignKey {
                        referenced_table,
                        referenced_column,
                    } if format => Some(Constraint::ForeignKey {
                        referenced_table: format_snake_case(&referenced_table),
                        referenced_column: format_snake_case(&referenced_column),
                    }),
                    other_constraint => Some(other_constraint),
                }
            } else {
                None
            };

            let (new_characters_maximum_length, new_numeric_precision, new_numeric_scale) =
                if !mapping.type_parameters {
                    (None, None, None)
                } else {
                    let new_characters_maximum_length = column
                        .character_maximum_length
                        .and_then(|length| {
                            if length == -1 {
                                Some(65535)
                            } else if (1..=65535).contains(&length) {
                                Some(length)
                            } else {
                                None
                            }
                        })
                        .or_else(|| mapping.max_characters_length.map(|value| value as i32));

                    let new_numeric_precision =
                        column.numeric_precision.or(mapping.numeric_precision);
                    let new_numeric_scale = if column.numeric_scale == Some(0) {
                        None
                    } else {
                        column
                            .numeric_scale
                            .or(mapping.numeric_scale.map(|value| value as i32))
                    };

                    (
                        new_characters_maximum_length,
                        new_numeric_precision,
                        new_numeric_scale,
                    )
                };

            ColumnSchema {
                column_name: new_column_name,
                data_type: new_data_type,
                character_maximum_length: new_characters_maximum_length,
                numeric_precision: new_numeric_precision,
                numeric_scale: new_numeric_scale,
                is_nullable: column.is_nullable,
                constraints: updated_constraints,
            }
        })
        .collect()
}

fn generate_insert_statement(table_name: &str, schema: &[ColumnSchema]) -> String {
    let column_names_string = schema
        .iter()
        .map(|column| column.column_name.as_str())
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "INSERT INTO `{}` ({}) VALUES",
        table_name, column_names_string
    )
}

async fn check_packet_size(
    config_send_packet_size: usize,
    max_allowed_packet: usize,
) -> Result<()> {
    debug!(
        "Max allowed packet size - Current: {} MB | Maximum {} MB",
        config_send_packet_size as f64 / 1_048_576.0,
        max_allowed_packet as f64 / 1_048_576.0
    );

    if config_send_packet_size > max_allowed_packet {
        bail!("Configured send packet size exceeds maximum allowed packet size")
    }

    Ok(())
}
