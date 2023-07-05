use crate::config::SettingsConfig;
use crate::database_extractor::DatabaseExtractor;
use crate::database_inserter::DatabaseInserter;
use crate::helpers::{format_snake_case, print_schema_info};
use crate::mappings::Mappings;
use crate::schema::ColumnSchema;
use std::error::Error;
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

    pub async fn run(&mut self) -> Result<(), Box<dyn Error>> {
        println!("Running table migrator");
        self.migrate_tables().await?;
        Ok(())
    }

    async fn migrate_tables(&mut self) -> Result<(), Box<dyn Error>> {
        // Fetch all tables from the database
        let mut tables = self.extractor.fetch_tables().await?;

        if tables.is_empty() {
            return Err("[-] No tables to process".into());
        }

        // Filter and keep only the whitelisted tables
        tables.retain(|table| self.settings.whitelisted_tables.contains(&table));

        if tables.is_empty() {
            return Err("[-] No tables to process after filtering whitelisted tables".into());
        }

        println!("Tables to migrate: {}", tables.join(", "));

        // Process each table
        for table_name in &tables {
            println!("--------------------------------------");
            self.migrate_table(table_name.clone()).await?;
        }

        println!("[+] Migration finished");

        Ok(())
    }

    async fn migrate_table(&mut self, table_name: String) -> Result<(), Box<dyn Error>> {
        println!("[!] Migrating table: {}", table_name);

        let start_time = Instant::now();

        // Fetch table schema
        let table_schema = self.extractor.get_table_schema(&table_name).await?;

        let output_table_name: String = if self.settings.format_snake_case {
            format_snake_case(&table_name)
        } else {
            table_name.clone()
        };

        println!("Input schema");
        print_schema_info(&table_schema);

        let mapped_schema = self.map_table_schema(&table_schema);

        println!("Target schema");
        print_schema_info(&mapped_schema);

        //Drop table in output database
        self.inserter.drop_table(&output_table_name).await?;

        //Create table in output database
        self.inserter
            .create_table(&output_table_name, &mapped_schema)
            .await?;

        // Fetch rows from the table
        let rows = self.extractor.fetch_rows_from_table(&table_name).await?;

        if !rows.is_empty() {
            // Generate and print INSERT queries
            let insert_queries =
                self.extractor
                    .generate_insert_queries(&output_table_name, rows, &mapped_schema);

            self.inserter
                .execute_transactional_queries(&insert_queries)
                .await?;
        }

        let end_time = Instant::now();
        println!(
            "[+] Table {} migrated, took: {}s",
            table_name,
            end_time.saturating_duration_since(start_time).as_secs_f32()
        );

        Ok(())
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
