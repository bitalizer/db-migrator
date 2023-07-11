use crate::config::SettingsConfig;
use crate::database_extractor::DatabaseExtractor;
use crate::database_inserter::DatabaseInserter;
use crate::helpers::{format_snake_case, print_schema_info};
use crate::mappings::Mappings;
use crate::schema::ColumnSchema;
use anyhow::{bail, Context, Result};

use futures::{StreamExt, TryStreamExt};
use log::info;
use std::sync::Arc;
use tokio::spawn;

use tokio::sync::mpsc;
use tokio::time::Instant;

pub struct DatabaseMigrator {
    extractor: DatabaseExtractor,
    inserter: DatabaseInserter,
    settings: SettingsConfig,
    mappings: Mappings,
}

impl DatabaseMigrator {
    pub fn new(
        extractor: DatabaseExtractor,
        inserter: DatabaseInserter,
        settings: SettingsConfig,
        mappings: Mappings,
    ) -> Self {
        DatabaseMigrator {
            extractor,
            inserter,
            settings,
            mappings,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Running table migrator");

        let config_send_packet_size = self.settings.max_packet_bytes;
        let max_allowed_packet = self.inserter.get_max_allowed_packet().await?;

        check_packet_size(config_send_packet_size, max_allowed_packet).await?;

        run_parallel_queries(&mut self.extractor);

        //self.migrate_tables().await?;
        Ok(())
    }

    /*async fn migrate_tables(&mut self) -> Result<()> {
        // Fetch all tables from the database
        let mut tables = self.extractor.fetch_tables().await?;

        if tables.is_empty() {
            bail!("No tables to process");
        }

        // Filter and keep only the whitelisted tables
        tables.retain(|table| self.settings.whitelisted_tables.contains(table));

        if tables.is_empty() {
            bail!("No tables to process after filtering whitelisted tables");
        }

        info!("Tables to migrate: {}", tables.join(", "));

        let start_time = Instant::now();

        // Process each table
        for table_name in &tables {
            let extractor = self.extractor.clone();
            let inserter = self.inserter.clone();
            let mappings = self.mappings.clone();
            let settings = self.settings.clone();

            migrate_table(extractor, inserter, mappings, settings, table_name.clone()).await?;
        }

        let end_time = Instant::now();

        info!(
            "Migration finished, total time took: {}s",
            end_time.saturating_duration_since(start_time).as_secs_f32()
        );

        Ok(())
    }*/
}

pub async fn run_parallel_queries(extractor: &mut DatabaseExtractor) -> Result<()> {
    // Fetch the list of tables
    let tables = extractor.fetch_tables().await?;

    // Create a channel to receive results from spawned tasks
    let (sender, mut receiver) = mpsc::channel(10);

    // Spawn a task for each table to fetch the rows concurrently
    for table in tables {
        let sender = sender.clone();
        let mut extractor = extractor.clone();

        // Spawn a task for each table

        //let mut extractor = extractor.clone();

        spawn(async move {
            let mut rows = extractor.fetch_rows_from_table(&table).await.unwrap();

            // Collect the rows and send the result through the channel
            let result: Vec<Result<Vec<String>, _>> = rows.collect().await;
            sender.send(result).await.unwrap();
        });
    }

    // Process the results received from the tasks
    while let Ok(results) = receiver.try_recv() {
        for result in results {
            match result {
                Ok(rows) => {
                    for row in rows {
                        // Process each row here
                        println!("{:?}", row);
                    }
                }
                Err(err) => {
                    // Handle the error
                    eprintln!("Error: {:?}", err);
                }
            }
        }
    }

    Ok(())
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

fn map_table_schema(
    mappings: Mappings,
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

            let new_data_type = mapping.to_type.clone();

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

async fn execute_batch(
    mut inserter: DatabaseInserter,
    insert_query: &String,
    transaction_count: usize,
) -> Result<()> {
    if !insert_query.is_empty() {
        let cloned_insert_query = Arc::new(insert_query.clone());

        let start_time = Instant::now();

        let query_str = cloned_insert_query.as_str();
        info!(
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
                info!(
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

async fn migrate_rows(
    mut extractor: DatabaseExtractor,
    mut inserter: DatabaseInserter,
    settings: SettingsConfig,
    input_table: &str,
    output_table: &str,
    mapped_schema: &[ColumnSchema],
) -> Result<()> {
    info!("Migrating {} rows", output_table);

    const RESERVED_BYTES: usize = 10;

    let insert_statement = generate_insert_statement(output_table, mapped_schema);
    let mut rows_stream = extractor.fetch_rows_from_table(input_table).await?;

    let mut insert_query = String::with_capacity(settings.max_packet_bytes);
    let mut total_bytes = insert_statement.len();
    let mut transaction_count = 0;

    while let Some(row_values) = rows_stream.try_next().await? {
        let values = row_values.join(", ");
        let value_set = format!("({})", values);
        let value_set_bytes = value_set.len();

        if RESERVED_BYTES + total_bytes + value_set_bytes > settings.max_packet_bytes {
            execute_batch(inserter.clone(), &insert_query, transaction_count).await?;

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

    execute_batch(inserter.clone(), &insert_query, transaction_count).await?;

    Ok(())
}

async fn migrate_table(
    mut extractor: DatabaseExtractor,
    mut inserter: DatabaseInserter,
    mappings: Mappings,
    settings: SettingsConfig,
    table_name: String,
) -> Result<()> {
    info!("Migrating table: {}", table_name);

    let start_time = Instant::now();

    // Fetch table schema
    let table_schema = extractor.get_table_schema(&table_name).await?;

    let output_table_name: String = if settings.format_snake_case {
        format_snake_case(&table_name)
    } else {
        table_name.clone()
    };

    /*println!("\nInput schema");
    print_schema_info(&table_schema);*/

    let mapped_schema = map_table_schema(mappings, &table_schema, settings.format_snake_case);

    /*println!("\nTarget schema");
    print_schema_info(&mapped_schema);*/

    // Create or truncate in the output database

    inserter
        .create_or_truncate_table(&output_table_name, &mapped_schema, settings.reset_tables)
        .await?;

    // Migrate rows from the table
    migrate_rows(
        extractor,
        inserter,
        settings,
        &table_name,
        &output_table_name,
        &mapped_schema,
    )
    .await?;

    let end_time = Instant::now();
    info!(
        "Table {} migrated, took: {}s",
        table_name,
        end_time.saturating_duration_since(start_time).as_secs_f32()
    );

    Ok(())
}
