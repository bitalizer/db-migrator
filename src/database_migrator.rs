use crate::config::SettingsConfig;
use crate::database_extractor::DatabaseExtractor;
use crate::database_inserter::DatabaseInserter;
use crate::schema::ColumnSchema;
use prettytable::{format, row, Table};
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
            println!("-----------------------------------");
            self.migrate_table(&table_name).await?;
        }

        println!("[+] Migration finished");

        Ok(())
    }

    async fn migrate_table(&mut self, table_name: &&String) -> Result<(), Box<dyn Error>> {
        println!("Migrating table: {}", table_name);

        // Fetch table schema
        let table_schema = self.extractor.get_table_schema(&table_name).await?;

        Self::print_schema_info(&table_schema);

        //Drop table in output database
        self.inserter.drop_table(&table_name).await?;

        //Create table in output database
        self.inserter
            .create_table(&table_name, &table_schema)
            .await?;

        // Fetch rows from the table
        let rows = self.extractor.fetch_rows_from_table(&table_name).await?;

        // Generate and print INSERT queries
        let insert_queries =
            self.extractor
                .generate_insert_queries(&table_name, rows, &table_schema);

        for query in insert_queries {
            println!("{}", query);
        }

        Ok(())
    }

    fn print_schema_info(table_schema: &[ColumnSchema]) {
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_BORDERS_ONLY);

        table.add_row(row![bFg => "Column Name", "Data Type", "Character Maximum Length", "Numeric Precision", "Numeric Scale"]);

        for column in table_schema {
            let character_maximum_length = column
                .character_maximum_length
                .map(|length| format!("{:?}", length));
            let precision = column.numeric_precision.map(|p| format!("{:?}", p));
            let scale = column.numeric_scale.map(|s| format!("{:?}", s));

            table.add_row(row![
                bFg => column.column_name,
                column.data_type,
                character_maximum_length.unwrap_or_else(|| "-".to_owned()),
                precision.unwrap_or_else(|| "-".to_owned()),
                scale.unwrap_or_else(|| "-".to_owned())
            ]);
        }

        table.printstd();
    }
}
