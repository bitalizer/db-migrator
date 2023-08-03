use anyhow::Result;
use sqlx::{Acquire, Executor, MySqlPool, Row};

use crate::query::{
    build_create_constraints, build_create_table_query, build_reset_query, TableAction,
};
use crate::schema::ColumnSchema;

#[derive(Clone)]
pub struct DatabaseInserter {
    pool: MySqlPool,
}

impl DatabaseInserter {
    pub fn new(pool: MySqlPool) -> Self {
        DatabaseInserter { pool }
    }

    pub(crate) async fn create_table(
        &mut self,
        table_name: &str,
        schema: &[ColumnSchema],
    ) -> Result<()> {
        let create_table_query = build_create_table_query(table_name, schema);

        debug!("Creating table {}", table_name);

        sqlx::query(create_table_query.as_str())
            .execute(&self.pool)
            .await?;

        info!("Table {} created successfully", table_name);

        Ok(())
    }

    pub(crate) async fn create_constraints(
        &mut self,
        table_name: &str,
        schema: &[ColumnSchema],
    ) -> Result<()> {
        let alter_table_query = build_create_constraints(table_name, schema);

        if let Some(query) = &alter_table_query {
            debug!("Creating constraints for table {}", table_name);

            let mut connection = self.pool.acquire().await?;
            let mut transaction = connection.begin().await?;

            transaction.execute("SET FOREIGN_KEY_CHECKS=0".to_string().as_str());

            if let Err(err) = transaction.execute(query.as_str()).await {
                warn!(
                    "Constraints creation failed for table: {},  query: '{}'. Error: {}",
                    table_name, query, err
                );
                transaction.execute("SET FOREIGN_KEY_CHECKS=1".to_string().as_str());
                transaction.rollback().await?; // Rollback if the transaction fails
            } else {
                transaction.commit().await?;
                info!("Table {} constraints created successfully", table_name);
            }
        }

        Ok(())
    }

    pub async fn execute_transactional_query(&mut self, query: &str) -> Result<()> {
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin().await?;

        transaction.execute("SET FOREIGN_KEY_CHECKS=0").await?;

        if let Err(err) = transaction.execute(query).await {
            transaction.rollback().await?;
            return Err(err.into());
        }

        transaction.execute("SET FOREIGN_KEY_CHECKS=1").await?;
        transaction.commit().await?;

        Ok(())
    }

    pub async fn get_max_allowed_packet(&mut self) -> Result<usize> {
        let query = "SELECT @@max_allowed_packet";

        let max_allowed_packet: u32 = sqlx::query_scalar(query).fetch_one(&self.pool).await?;

        Ok(max_allowed_packet as usize)
    }

    pub async fn reset_tables(&mut self, tables: &[String], action: TableAction) -> Result<()> {
        debug!("Resetting tables");

        let mut all_tables = self.get_all_tables().await?;

        // Filter and keep only the tables that exist in the database and are also present in the `tables` slice
        all_tables.retain(|table| tables.contains(table));

        let reset_tables_query = build_reset_query(&all_tables, &action);
        self.execute_transactional_query(reset_tables_query.as_str())
            .await?;

        match action {
            TableAction::Drop => info!("Tables dropped successfully"),
            TableAction::Truncate => info!("Tables truncated successfully"),
        }

        Ok(())
    }

    async fn get_all_tables(&mut self) -> Result<Vec<String>> {
        let rows = sqlx::query("SHOW TABLES").fetch_all(&self.pool).await?;

        let table_names: Vec<String> = rows
            .iter()
            .map(|row| row.get::<String, _>(0)) // Get the first column value as a String
            .collect();

        Ok(table_names)
    }

    pub(crate) async fn table_exists(&mut self, table_name: &str) -> Result<bool> {
        let query = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '{}'",
            table_name
        );

        let count: i64 = sqlx::query_scalar(&query).fetch_one(&self.pool).await?;

        Ok(count > 0)
    }

    pub(crate) async fn table_rows_count(&mut self, table_name: &str) -> Result<i64> {
        let query = format!("SELECT COUNT(*) FROM `{}`", table_name);

        let count: i64 = sqlx::query_scalar(&query).fetch_one(&self.pool).await?;

        Ok(count)
    }
}
