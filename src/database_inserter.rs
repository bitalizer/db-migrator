use anyhow::Result;
use sqlx::{Acquire, Executor, MySqlPool};

use crate::query::{build_create_constraints, build_create_table_query, build_drop_query};
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
            debug!(
                "Creating constraints table {} with query: {}",
                table_name, query
            );

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
                info!("Table {} constraints altered successfully", table_name);
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

    pub async fn drop_tables(&mut self, tables: &[String]) -> Result<()> {
        let drop_tables_query = build_drop_query(tables);
        self.execute_transactional_query(drop_tables_query.as_str())
            .await?;
        Ok(())
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
