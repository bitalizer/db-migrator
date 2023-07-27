use crate::schema::ColumnSchema;

use anyhow::Result;
use sqlx::{Acquire, Executor, MySqlPool};

#[derive(Clone)]
pub struct DatabaseInserter {
    pool: MySqlPool,
}

impl DatabaseInserter {
    pub fn new(pool: MySqlPool) -> Self {
        DatabaseInserter { pool }
    }

    pub(crate) async fn create_or_truncate_table(
        &mut self,
        table_name: &str,
        schema: &[ColumnSchema],
        drop: bool,
    ) -> Result<()> {
        if drop {
            self.drop_table(table_name).await?;
        }

        let table_exists = self.table_exists(table_name).await?;

        if table_exists {
            self.truncate_table(table_name).await?;
        } else {
            self.create_table(table_name, schema).await?;
        }

        Ok(())
    }

    async fn create_table(&mut self, table_name: &str, schema: &[ColumnSchema]) -> Result<()> {
        let create_table_query = &self.build_create_table_query(table_name, schema)?;

        info!("Creating table {}", table_name);

        sqlx::query(create_table_query).execute(&self.pool).await?;

        info!("Table {} created successfully", table_name);

        Ok(())
    }

    pub async fn execute_transactional_query(&mut self, query: &str) -> Result<()> {
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin().await?;

        if let Err(err) = transaction.execute(query).await {
            error!("Transaction execution failed: {}", err);
        }

        transaction.commit().await?;

        Ok(())
    }

    fn build_create_table_query(
        &self,
        table_name: &str,
        schema: &[ColumnSchema],
    ) -> Result<String> {
        let columns: Result<Vec<String>> = schema
            .iter()
            .map(|column| {
                let mut result_str = String::new();

                result_str.push_str(&column.column_name);
                result_str.push(' '); // Add a space after column_name

                result_str.push_str(&column.data_type);
                if let Some(max_length) = column.character_maximum_length {
                    result_str.push_str(&format!("({})", max_length));
                } else if let Some(precision) = column.numeric_precision {
                    if let Some(scale) = column.numeric_scale {
                        result_str.push_str(&format!("({}, {})", precision, scale));
                    } else {
                        result_str.push_str(&format!("({})", precision));
                    }
                }

                result_str.push(' '); // Add a space after data_type and type_properties

                let nullable_property = if column.is_nullable {
                    "NULL"
                } else {
                    "NOT NULL"
                };
                result_str.push_str(nullable_property);

                Ok(result_str)
            })
            .collect();

        let columns = columns?;

        let create_table_query = format!("CREATE TABLE `{}` ({})", table_name, columns.join(", "));

        println!("{}", create_table_query);

        Ok(create_table_query)
    }

    pub async fn get_max_allowed_packet(&mut self) -> Result<usize> {
        let query = "SELECT @@max_allowed_packet";

        let max_allowed_packet: u32 = sqlx::query_scalar(query).fetch_one(&self.pool).await?;

        Ok(max_allowed_packet as usize)
    }

    pub async fn drop_table(&mut self, table_name: &str) -> Result<()> {
        let table_exists = self.table_exists(table_name).await?;

        if !table_exists {
            return Ok(());
        }

        let drop_table_query = format!("DROP TABLE `{}`", table_name);

        sqlx::query(&drop_table_query).execute(&self.pool).await?;

        info!("Table {} dropped successfully", table_name);

        Ok(())
    }

    async fn truncate_table(&mut self, table_name: &str) -> Result<()> {
        let drop_table_query = format!("TRUNCATE TABLE `{}`", table_name);

        sqlx::query(&drop_table_query).execute(&self.pool).await?;

        info!("Table {} truncated successfully", table_name);

        Ok(())
    }

    async fn table_exists(&mut self, table_name: &str) -> Result<bool> {
        let query = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '{}'",
            table_name
        );

        let count: i64 = sqlx::query_scalar(&query).fetch_one(&self.pool).await?;

        Ok(count > 0)
    }
}
