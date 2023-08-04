#[derive(Debug, Clone)]
pub struct MigrationOptions {
    pub(crate) drop: bool,
    pub(crate) constraints: bool,
    pub(crate) format_snake_case: bool,
    pub(crate) max_concurrent_tasks: usize,
    pub(crate) max_packet_bytes: usize,
    pub(crate) whitelisted_tables: Vec<String>,
}
