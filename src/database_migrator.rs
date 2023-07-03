use crate::config::SettingsConfig;
use crate::database_extractor::DatabaseExtractor;
use crate::database_inserter::DatabaseInserter;
use crate::schema::ColumnSchema;
use std::error::Error;

pub struct DatabaseMigrator {
    extractor: DatabaseExtractor,
    inserter: DatabaseInserter,
    settings: SettingsConfig,
}

impl DatabaseMigrator {
    pub fn new(
        extractor: DatabaseExtractor,
        inserter: DatabaseInserter,
        settings: SettingsConfig,
    ) -> Self {
        DatabaseMigrator {
            extractor,
            inserter,
            settings,
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
            return Err("No tables to process".into());
        }

        // Filter and keep only the whitelisted tables
        tables.retain(|table| self.settings.whitelisted_tables().contains(&table));

        if tables.is_empty() {
            return Err("No tables to process after filtering whitelisted tables".into());
        }

        println!("Tables to migrate: {}", tables.join(", "));

        // Process each table
        for table_name in &tables {
            println!("-----------------------------------");
            self.migrate_table(&table_name).await?;
        }

        println!("Migration finished");

        Ok(())
    }

    async fn migrate_table(&mut self, table_name: &&String) -> Result<(), Box<dyn Error>> {
        println!("Migrating table: {}", table_name);

        // Fetch table schema
        let table_schema = self.extractor.get_table_schema(&table_name).await?;
        println!("Table Schema:");
        Self::print_schema_info(&table_schema);

        self.inserter.create_table(&table_name).await?;

        // Fetch rows from the table
        let rows = self.extractor.fetch_rows_from_table(&table_name).await?;

        // Generate and print INSERT queries
        let insert_queries =
            self.extractor
                .generate_insert_queries(&table_name, rows, &table_schema);
        for query in insert_queries {
            println!("{}", query);
        }

        println!("---");
        Ok(())
    }

    fn print_schema_info(table_schema: &Vec<ColumnSchema>) {
        let output: Vec<String> = table_schema
            .iter()
            .map(|column| {
                let mut column_info = vec![
                    format!("Column Name: {}", column.column_name),
                    format!("Data Type: {}", column.data_type),
                ];

                if let Some(character_maximum_length) = column.character_maximum_length {
                    column_info.push(format!(
                        "Character Maximum Length: {:?}",
                        character_maximum_length
                    ));
                }

                if let Some(precision) = column.numeric_precision {
                    column_info.push(format!("Numeric Precision: {:?}", precision));
                }

                if let Some(scale) = column.numeric_scale {
                    column_info.push(format!("Numeric Scale: {:?}", scale));
                }

                column_info.push("-------------------------".to_owned());

                column_info.join("\n")
            })
            .collect();

        println!("{}", output.join("\n"));
    }
}
