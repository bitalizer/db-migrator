use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use sqlx::{Acquire, Executor, MySqlPool, Row};

use crate::common::sql::{escape_mysql_identifier, escape_sql_string};

use crate::common::schema::ColumnSchema;
use crate::insert::query::{build_create_constraints, build_create_table_query, build_reset_query};
use crate::insert::table_action::TableAction;
use crate::insert::traits::Inserter;

#[derive(Clone)]
pub struct DatabaseInserter {
    pool: MySqlPool,
}

impl DatabaseInserter {
    pub fn new(pool: MySqlPool) -> Self {
        DatabaseInserter { pool }
    }

    async fn get_all_tables(&self) -> Result<Vec<String>> {
        let rows = sqlx::query("SHOW TABLES").fetch_all(&self.pool).await?;

        let table_names: Vec<String> = rows.iter().map(|row| row.get::<String, _>(0)).collect();

        Ok(table_names)
    }
}

#[async_trait]
impl Inserter for DatabaseInserter {
    async fn create_table(&self, table_name: &str, schema: &[ColumnSchema]) -> Result<()> {
        let create_table_query = build_create_table_query(table_name, schema);

        debug!("Creating table {}", table_name);

        self.execute_transactional_query(create_table_query.as_str())
            .await
            .with_context(|| format!("Encountered an error while creating table {}", table_name))?;

        info!("Table {} created successfully", table_name);

        Ok(())
    }

    async fn create_constraints(
        &self,
        table_name: &str,
        schema: &[ColumnSchema],
        formatted_tables: &[String],
    ) -> Result<()> {
        let alter_table_query = build_create_constraints(table_name, schema, formatted_tables);

        if let Some(query) = &alter_table_query {
            debug!("Creating constraints for table {}", table_name);

            let mut connection = self.pool.acquire().await?;
            let mut transaction = connection.begin().await?;

            transaction
                .execute("SET FOREIGN_KEY_CHECKS=0")
                .await
                .with_context(|| {
                    format!(
                        "Failed to disable foreign key checks for table {}",
                        table_name
                    )
                })?;

            if let Err(err) = transaction.execute(query.as_str()).await {
                warn!(
                    "Constraints creation failed for table: {}, query: '{}'. Error: {}",
                    table_name, query, err
                );
                // Best-effort re-enable FK checks before rollback
                let _ = transaction.execute("SET FOREIGN_KEY_CHECKS=1").await;
                transaction.rollback().await?;
            } else {
                transaction.commit().await?;
                info!("Table {} constraints created successfully", table_name);
            }
        }

        Ok(())
    }

    async fn execute_transactional_query(&self, query: &str) -> Result<()> {
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin().await?;

        transaction.execute("SET FOREIGN_KEY_CHECKS=0").await?;

        if let Err(err) = transaction.execute(query).await {
            transaction.rollback().await?;
            let preview = if query.is_empty() {
                "EMPTY QUERY".to_string()
            } else {
                query.chars().take(100).collect()
            };
            return Err(anyhow!(
                "Cannot execute transaction query: {}. Error: {}",
                preview,
                err
            ));
        }

        transaction.execute("SET FOREIGN_KEY_CHECKS=1").await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn get_max_allowed_packet(&self) -> Result<usize> {
        let query = "SELECT @@max_allowed_packet";

        let max_allowed_packet: u32 = sqlx::query_scalar(query).fetch_one(&self.pool).await?;

        Ok(max_allowed_packet as usize)
    }

    async fn reset_tables(&self, tables: &[String], action: TableAction) -> Result<()> {
        let mut all_tables = self.get_all_tables().await.with_context(
            || "Resetting tables encountered an error, cannot obtain existing tables",
        )?;

        // Filter and keep only the tables that exist in the database and are also present in the `tables` slice
        all_tables.retain(|table| {
            tables
                .iter()
                .any(|t| t.to_lowercase() == table.to_lowercase())
        });

        if all_tables.is_empty() {
            debug!("No tables to reset");
        } else {
            debug!("Resetting tables");
            let reset_tables_query = build_reset_query(&all_tables, &action);

            self.execute_transactional_query(reset_tables_query.as_str())
                .await
                .with_context(|| "Resetting tables encountered an error")?;

            match action {
                TableAction::Drop => info!("Tables dropped successfully"),
                TableAction::Truncate => info!("Tables truncated successfully"),
            }
        }

        Ok(())
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool> {
        let query = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '{}'",
            escape_sql_string(table_name)
        );

        let count: i64 = sqlx::query_scalar(&query).fetch_one(&self.pool).await?;

        Ok(count > 0)
    }

    async fn table_rows_count(&self, table_name: &str) -> Result<i64> {
        let query = format!(
            "SELECT COUNT(*) FROM {}",
            escape_mysql_identifier(table_name)
        );

        let count: i64 = sqlx::query_scalar(&query).fetch_one(&self.pool).await?;

        Ok(count)
    }
}
