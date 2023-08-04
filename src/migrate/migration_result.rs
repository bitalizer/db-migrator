use crate::common::schema::ColumnSchema;

#[derive(Debug, Clone)]
pub struct MigrationResult {
    pub table_name: String,
    pub schema: Vec<ColumnSchema>,
    pub created: bool,
}
