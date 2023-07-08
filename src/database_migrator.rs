use crate::config::SettingsConfig;
use crate::database_extractor::DatabaseExtractor;
use crate::database_inserter::DatabaseInserter;
use crate::helpers::{format_snake_case, print_schema_info};
use crate::mappings::Mappings;
use crate::schema::ColumnSchema;
use anyhow::{bail, Context, Result};
use futures::TryStreamExt;
use log::info;
use std::sync::Arc;
use tokio::spawn;
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

        let config_send_packet_size = self.settings.send_packet_size;
        let max_allowed_packet = self.inserter.get_max_allowed_packet().await?;

        check_packet_size(config_send_packet_size, max_allowed_packet).await?;

        self.migrate_tables().await?;
        Ok(())
    }

    async fn migrate_tables(&mut self) -> Result<()> {
        // Fetch all tables from the database
        let mut tables = self.extractor.fetch_tables().await?;

        if tables.is_empty() {
            bail!("No tables to process");
            //return Err("[-] No tables to process".into());
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
            self.migrate_table(table_name.clone()).await?;
        }

        let end_time = Instant::now();

        info!(
            "Migration finished, total time took: {}s",
            end_time.saturating_duration_since(start_time).as_secs_f32()
        );

        Ok(())
    }

    async fn migrate_table(&mut self, table_name: String) -> Result<()> {
        info!("Migrating table: {}", table_name);

        let start_time = Instant::now();

        // Fetch table schema
        let table_schema = self.extractor.get_table_schema(&table_name).await?;

        let output_table_name: String = if self.settings.format_snake_case {
            format_snake_case(&table_name)
        } else {
            table_name.clone()
        };

        /*println!("\nInput schema");
        print_schema_info(&table_schema);*/

        let mapped_schema = self.map_table_schema(&table_schema);

        /*println!("\nTarget schema");
        print_schema_info(&mapped_schema);*/

        // Create or truncate in the output database
        let drop_table = self.settings.reset_tables;

        self.inserter
            .create_or_truncate_table(&output_table_name, &mapped_schema, drop_table)
            .await?;

        // Migrate rows from the table
        self.migrate_rows(&table_name, &output_table_name, &mapped_schema)
            .await?;

        let end_time = Instant::now();
        info!(
            "Table {} migrated, took: {}s",
            table_name,
            end_time.saturating_duration_since(start_time).as_secs_f32()
        );

        Ok(())
    }

    async fn migrate_rows(
        &mut self,
        input_table: &str,
        output_table: &str,
        mapped_schema: &[ColumnSchema],
    ) -> Result<()> {
        info!("Migrating {} rows", output_table);

        let max_send_packet_bytes: usize = self.settings.send_packet_size;

        let insert_statement = Self::generate_insert_statement(output_table, mapped_schema);
        let mut rows_stream = self.extractor.fetch_rows_from_table(input_table).await?;

        let mut insert_query = String::with_capacity(max_send_packet_bytes);
        let mut total_bytes = insert_statement.len();
        let mut transaction_count = 0;

        while let Some(row_values) = rows_stream.try_next().await? {
            let values = row_values.join(", ");
            let value_set = format!("({})", values);
            let value_set_bytes = value_set.len();

            if total_bytes + value_set_bytes > max_send_packet_bytes {
                Self::execute_batch(self.inserter.clone(), &insert_query, transaction_count)
                    .await?;

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

        Self::execute_batch(self.inserter.clone(), &insert_query, transaction_count).await?;

        Ok(())
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

    fn map_table_schema(&self, table_schema: &[ColumnSchema]) -> Vec<ColumnSchema> {
        table_schema
            .iter()
            .map(|column| {
                let mapping = self.mappings.get(&column.data_type).unwrap_or_else(|| {
                    panic!("Mapping not found for data type: {}", column.data_type)
                });

                let new_column_name = if self.settings.format_snake_case {
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
}

async fn check_packet_size(
    config_send_packet_size: usize,
    max_allowed_packet: usize,
) -> Result<()> {
    info!(
        "Max allowed packet size - Current: {} MB | Maximum {} MB",
        config_send_packet_size as f64 / 1_048_576.0,
        max_allowed_packet as f64 / 1_048_576.0
    );

    if config_send_packet_size > max_allowed_packet {
        bail!("Configured send packet size exceeds maximum allowed packet size")
    }

    Ok(())
}
