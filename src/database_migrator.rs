use std::sync::Arc;

use anyhow::{bail, Context, Error, Result};
use futures::future::join_all;
use futures::TryStreamExt;
use log::info;
use tokio::spawn;
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::database_extractor::{open_row_stream, DatabaseExtractor};
use crate::database_inserter::DatabaseInserter;
use crate::helpers::{format_snake_case, print_error_chain};
use crate::mappings::Mappings;
use crate::query::{build_insert_statement, TableAction};
use crate::schema::{ColumnSchema, Constraint};

pub struct DatabaseMigrator {
    extractor: DatabaseExtractor,
    inserter: DatabaseInserter,
    mappings: Mappings,
    options: MigrationOptions,
}

#[derive(Debug, Clone)]
pub struct MigrationOptions {
    pub(crate) drop: bool,
    pub(crate) constraints: bool,
    pub(crate) format_snake_case: bool,
    pub(crate) max_concurrent_tasks: usize,
    pub(crate) max_packet_bytes: usize,
    pub(crate) whitelisted_tables: Vec<String>,
}

#[derive(Debug, Clone)]
struct MigrationResult {
    table_name: String,
    schema: Vec<ColumnSchema>,
    created: bool,
}

impl DatabaseMigrator {
    pub fn new(
        extractor: DatabaseExtractor,
        inserter: DatabaseInserter,
        mappings: Mappings,
        options: MigrationOptions,
    ) -> Self {
        DatabaseMigrator {
            extractor,
            inserter,
            mappings,
            options,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Running table migrator");

        let config_send_packet_size = self.options.max_packet_bytes;
        let max_allowed_packet = self.inserter.get_max_allowed_packet().await?;

        check_packet_size(config_send_packet_size, max_allowed_packet).await?;

        self.migrate_tables().await?;

        Ok(())
    }

    pub async fn migrate_tables(&mut self) -> Result<()> {
        let start_time = Instant::now();

        let (tables, formatted_tables) = self.fetch_and_format_tables().await?;

        let action = if self.options.drop {
            TableAction::Drop
        } else {
            TableAction::Truncate
        };

        self.inserter
            .reset_tables(&formatted_tables, action)
            .await?;

        let migration_results = self.run_migration(tables).await;
        let (successful_results, errors) = process_migration_results(migration_results).await;

        // Handle errors
        for err in errors {
            print_error_chain(&err);
        }

        if self.options.constraints {
            self.create_constraints(successful_results).await;
        }

        let end_time = Instant::now();

        info!(
            "Migration finished, total time took: {}s",
            end_time.saturating_duration_since(start_time).as_secs_f32()
        );

        Ok(())
    }

    async fn fetch_and_format_tables(&mut self) -> Result<(Vec<String>, Vec<String>)> {
        let mut tables = self.extractor.fetch_tables().await?; // Fetch the list of tables from input database
        let formatted_tables = format_table_names(&tables, self.options.format_snake_case); // Format to snake case if required

        if tables.is_empty() {
            bail!("No tables to process");
        }

        check_missing_tables(&tables, &self.options.whitelisted_tables);

        // Filter and keep only the whitelisted tables
        tables.retain(|table| self.options.whitelisted_tables.contains(table));

        if tables.is_empty() {
            bail!("No tables to process after filtering whitelisted tables");
        }

        info!("Tables to migrate: {}", tables.join(", "));

        Ok((tables, formatted_tables))
    }

    async fn run_migration(&mut self, tables: Vec<String>) -> Vec<Result<MigrationResult, Error>> {
        // Create a semaphore to limit the number of concurrent tasks
        let semaphore = Arc::new(Semaphore::new(self.options.max_concurrent_tasks));

        // Create a Vec to store the JoinHandles for tasks
        let mut migration_tasks = Vec::new();

        // Spawn a task for each table to fetch the rows concurrently
        for table in tables {
            // Clone the shared semaphore for each task
            let semaphore_clone = Arc::clone(&semaphore);

            let mut extractor = self.extractor.clone();
            let mut inserter = self.inserter.clone();
            let mappings = self.mappings.clone();
            let options = self.options.clone();

            // Spawn a task for each table
            let task = spawn(async move {
                // Acquire a semaphore permit before starting the task
                let permit = semaphore_clone
                    .acquire()
                    .await
                    .expect("Failed to acquire semaphore permit");

                let result =
                    migrate_table(&mut extractor, &mut inserter, &mappings, &options, &table)
                        .await
                        .with_context(|| format!("Error while migrating table: {}", table));

                // Release the semaphore permit when the task is done (whether successful or not)
                drop(permit);
                result
            });

            migration_tasks.push(task);
        }

        let migration_results: Vec<Result<MigrationResult, Error>> = join_all(migration_tasks)
            .await
            .into_iter()
            .map(|join_handle_result| join_handle_result.expect("Error in JoinHandle"))
            .collect();

        migration_results
    }

    async fn create_constraints(&mut self, successful_results: Vec<MigrationResult>) {
        let tasks = successful_results
            .into_iter()
            .filter(|migration_result| migration_result.created) // Filter only MigrationResult items where created is true
            .map(|migration_result| {
                let mut inserter = self.inserter.clone(); // Clone the inserter for each task
                let table_name = migration_result.table_name.clone(); // Clone the table name to move into the task
                let schema = migration_result.schema; // Clone the schema to move into the task

                spawn(async move {
                    if let Err(err) = inserter
                        .create_constraints(&table_name, &schema)
                        .await
                        .with_context(|| {
                            format!("Error while creating constraints for table: {}", table_name)
                        })
                    {
                        print_error_chain(&err);
                    }
                })
            })
            .collect::<Vec<_>>();

        join_all(tasks).await;
    }
}

async fn migrate_table(
    extractor: &mut DatabaseExtractor,
    inserter: &mut DatabaseInserter,
    mappings: &Mappings,
    options: &MigrationOptions,
    table_name: &str,
) -> Result<MigrationResult, Error> {
    let output_table_name: String = if options.format_snake_case {
        format_snake_case(table_name)
    } else {
        table_name.to_string()
    };

    info!("Migrating table: {}", &output_table_name);

    let start_time = Instant::now();

    // Fetch and map table schema
    let table_schema = extractor
        .get_table_schema(table_name)
        .await
        .with_context(|| "Failed to get table schema".to_string())?;

    let mapped_schema = map_table_schema(mappings, &table_schema, options.format_snake_case);

    let table_exists = inserter
        .table_exists(&output_table_name)
        .await
        .with_context(|| "Failed to check table existence".to_string())?;

    if !table_exists {
        // Create table in the output database
        inserter
            .create_table(&output_table_name, &mapped_schema)
            .await
            .with_context(|| "Failed to create table".to_string())?;
    }

    // Migrate rows from input table to output table
    let migrated_count = migrate_table_rows(
        extractor.clone(),
        inserter,
        options,
        table_name,
        &output_table_name,
        &mapped_schema,
    )
    .await
    .with_context(|| "Failed to migrate rows".to_string())?;

    if migrated_count > 0 {
        let end_time = Instant::now();
        info!(
            "Table {} migrated, rows: {}, took: {}s",
            &output_table_name,
            migrated_count,
            end_time.saturating_duration_since(start_time).as_secs_f32()
        );
    } else {
        info!("Table {} contains latest data", &output_table_name)
    }

    Ok(MigrationResult {
        table_name: output_table_name,
        schema: mapped_schema,
        created: !table_exists,
    })
}

async fn migrate_table_rows(
    extractor: DatabaseExtractor,
    inserter: &mut DatabaseInserter,
    settings: &MigrationOptions,
    input_table: &str,
    output_table: &str,
    mapped_schema: &[ColumnSchema],
) -> Result<usize, Error> {
    info!("Migrating {} rows", output_table);

    const RESERVED_BYTES: usize = 10;

    let offset_index = inserter.table_rows_count(output_table).await?;

    if offset_index > 0 {
        info!(
            "Rows already exists in table {}, continue from offset {}",
            output_table, offset_index
        )
    }

    let insert_statement = build_insert_statement(output_table, mapped_schema);

    let mut conn = extractor.pool.get().await?;
    let mut stream = open_row_stream(&mut conn, input_table, offset_index).await?;

    let mut insert_query = String::with_capacity(settings.max_packet_bytes);
    let mut total_bytes = insert_statement.len();
    let mut transaction_count = 0;
    let mut total_transaction_count = 0; //Track the row count

    while let Some(row_values) = stream.try_next().await? {
        let values = row_values.join(", ");
        let value_set = format!("({}) ", values);
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
) -> Result<(), Error> {
    if !insert_query.is_empty() {
        let cloned_insert_query = Arc::new(insert_query.clone());

        let start_time = Instant::now();

        let query_str = cloned_insert_query.as_str();
        debug!(
            "Sending {} bytes batch with {} transactions",
            query_str.len(),
            transaction_count
        );

        inserter
            .execute_transactional_query(query_str)
            .await
            .with_context(|| "Failed to execute transactional query batch".to_string())?;

        let end_time = Instant::now();

        debug!(
            "Executed batch with {} transactions, bytes: {}, took: {}s",
            transaction_count,
            query_str.len(),
            end_time.saturating_duration_since(start_time).as_secs_f32()
        );
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

fn check_missing_tables(tables: &[String], whitelisted_tables: &[String]) {
    // Check for missing tables in whitelisted_tables
    let missing_tables: Vec<_> = whitelisted_tables
        .iter()
        .filter(|table| !tables.contains(table))
        .cloned()
        .collect();

    // If there are missing tables, print a warning
    if !missing_tables.is_empty() {
        let missing_tables_str = missing_tables.join(", ");
        warn!(
            "The following whitelisted tables were not found in the database: {}",
            missing_tables_str
        );
    }
}

fn format_table_names(tables: &[String], format: bool) -> Vec<String> {
    if format {
        tables
            .iter()
            .map(|table_name| format_snake_case(table_name))
            .collect()
    } else {
        tables.to_vec()
    }
}

// Helper function to process migration results and separate successful results from errors
async fn process_migration_results(
    migration_results: Vec<Result<MigrationResult, Error>>,
) -> (Vec<MigrationResult>, Vec<Error>) {
    let (successful_results, errors): (Vec<_>, Vec<_>) =
        migration_results.into_iter().partition(Result::is_ok);

    let successful_results: Vec<MigrationResult> =
        successful_results.into_iter().map(Result::unwrap).collect();

    (
        successful_results,
        errors.into_iter().map(Result::unwrap_err).collect(),
    )
}
