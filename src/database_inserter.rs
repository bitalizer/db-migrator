use crate::schema::ColumnSchema;

use sqlx::{Executor, MySqlPool};
use std::error::Error;
use tokio::time::Instant;

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
            "\n[!] Creating table {}, query: \n      {}",
            table_name, create_table_query
        );

        sqlx::query(create_table_query).execute(&self.pool).await?;

        println!("[+] Table {} created successfully", table_name);

        Ok(())
    }

    pub async fn execute_transactional_queries(
        &mut self,
        queries: &[String],
    ) -> Result<(), Box<dyn Error>> {
        let start_time = Instant::now();
        let mut transaction = self.pool.begin().await?;

        for query in queries {
            transaction.execute(query.as_str()).await?;
        }

        transaction.commit().await?;

        let end_time = Instant::now();
        println!(
            "[+] Executed {} transactional queries, took: {}s",
            queries.len(),
            end_time.saturating_duration_since(start_time).as_secs_f32()
        );

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

    pub async fn drop_table(&mut self, table_name: &str) -> Result<(), Box<dyn Error>> {
        let table_exists = self.table_exists(table_name).await?;

        if !table_exists {
            return Ok(());
        }

        let drop_table_query = format!("DROP TABLE `{}`", table_name);

        sqlx::query(&drop_table_query).execute(&self.pool).await?;

        println!("[+] Table {} dropped successfully", table_name);

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
