use anyhow::Result;
use async_trait::async_trait;

use crate::common::target_schema::TargetColumn;
use crate::insert::table_action::TableAction;

#[async_trait]
pub trait Inserter: Clone + Send + Sync + 'static {
    async fn create_table(&self, name: &str, schema: &[TargetColumn]) -> Result<()>;
    async fn create_constraints(
        &self,
        name: &str,
        schema: &[TargetColumn],
        tables: &[String],
    ) -> Result<()>;
    async fn execute_transactional_query(&self, query: &str) -> Result<()>;
    async fn get_max_allowed_packet(&self) -> Result<usize>;
    async fn reset_tables(&self, tables: &[String], action: TableAction) -> Result<()>;
    async fn table_exists(&self, name: &str) -> Result<bool>;
    async fn table_rows_count(&self, name: &str) -> Result<i64>;
}
