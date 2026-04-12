use crate::common::target_schema::TargetColumn;

#[derive(Debug, Clone)]
pub struct MigrationResult {
    pub table_name: String,
    pub schema: Vec<TargetColumn>,
    pub created: bool,
}
