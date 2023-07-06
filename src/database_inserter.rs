use crate::schema::ColumnSchema;

use sqlx::{Executor, MySqlPool};
use std::error::Error;

pub struct DatabaseInserter {
    pool: MySqlPool,
}

impl DatabaseInserter {
    pub fn new(pool: MySqlPool) -> Self {
        DatabaseInserter { pool }
    }

    pub async fn create_table(
        &mut self,
        table_name: &str,
        schema: &[ColumnSchema],
    ) -> Result<(), Box<dyn Error>> {
        let create_table_query = &self.build_create_table_query(table_name, schema)?;

        println!(
            "[!] Creating table {}, query: \n      {}",
            table_name, create_table_query
        );

        sqlx::query(create_table_query).execute(&self.pool).await?;

        println!("\n[+] Table {} created successfully", table_name);

        Ok(())
    }

    pub async fn execute_transactional_query(&mut self, query: &str) -> Result<(), Box<dyn Error>> {
        let mut transaction = self.pool.begin().await?;

        if let Err(err) = transaction.execute(query).await {
            eprintln!("Error details: {}", err);
        }

        transaction.commit().await?;

        Ok(())
    }

    fn build_create_table_query(
        &self,
        table_name: &str,
        schema: &[ColumnSchema],
    ) -> Result<String, Box<dyn Error>> {
        let columns: Result<Vec<String>, Box<dyn Error>> = schema
            .iter()
            .map(|column| {
                let mut type_properties = String::new();

                if let Some(max_length) = column.character_maximum_length {
                    type_properties.push_str(&format!("({})", max_length));
                } else if let Some(precision) = column.numeric_precision {
                    if let Some(scale) = column.numeric_scale {
                        type_properties.push_str(&format!("({}, {})", precision, scale));
                    } else {
                        type_properties.push_str(&format!("({})", precision));
                    }
                }
                Ok(format!(
                    "{} {}{}",
                    column.column_name, column.data_type, type_properties
                ))
            })
            .collect();

        let columns = columns?;

        let create_table_query = format!("CREATE TABLE `{}` ({})", table_name, columns.join(", "));

        Ok(create_table_query)
    }

    pub async fn get_max_allowed_packet(&mut self) -> Result<usize, Box<dyn Error>> {
        let query = "SELECT @@max_allowed_packet";

        let max_allowed_packet: u32 = sqlx::query_scalar(query).fetch_one(&self.pool).await?;

        Ok(max_allowed_packet as usize)
    }

    pub async fn drop_table(&mut self, table_name: &str) -> Result<(), Box<dyn Error>> {
        let table_exists = self.table_exists(table_name).await?;

        if !table_exists {
            return Ok(());
        }

        let drop_table_query = format!("DROP TABLE `{}`", table_name);

        sqlx::query(&drop_table_query).execute(&self.pool).await?;

        println!("\n[+] Table {} dropped successfully", table_name);

        Ok(())
    }

    async fn table_exists(&mut self, table_name: &str) -> Result<bool, Box<dyn Error>> {
        let query = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '{}'",
            table_name
        );

        let count: i64 = sqlx::query_scalar(&query).fetch_one(&self.pool).await?;

        Ok(count > 0)
    }
}
