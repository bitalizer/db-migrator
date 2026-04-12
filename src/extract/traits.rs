use anyhow::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::common::schema::ColumnSchema;

#[async_trait]
pub trait Extractor: Clone + Send + Sync + 'static {
    async fn fetch_tables(&self) -> Result<Vec<String>>;
    async fn get_table_schema(&self, table: &str) -> Result<Vec<ColumnSchema>>;
    async fn stream_rows(&self, table: &str) -> Result<BoxStream<'static, Result<Vec<String>>>>;
}
