use std::fmt;

/// Errors that can occur during the migration process.
#[derive(Debug)]
pub enum MigrationError {
    /// A required type mapping was not found in mappings.toml
    MappingNotFound { data_type: String },

    /// The target table already contains rows and cannot be migrated into
    TableAlreadyHasRows { table: String, count: i64 },

    /// The configured packet size exceeds MySQL's max_allowed_packet
    PacketSizeTooLarge { configured: usize, maximum: usize },

    /// Failed to parse a column value from the source database
    ColumnParseFailed {
        column: String,
        reason: String,
    },

    /// Failed to parse a constraint definition
    ConstraintParseFailed { value: String, reason: String },

    /// Failed to construct a valid date/time value from source data
    InvalidDateTimeValue { reason: String },

    /// A spawned migration task panicked
    TaskPanicked { table: String },
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrationError::MappingNotFound { data_type } => {
                write!(
                    f,
                    "No type mapping found for MSSQL data type '{}'. Add it to mappings.toml",
                    data_type
                )
            }
            MigrationError::TableAlreadyHasRows { table, count } => {
                write!(
                    f,
                    "Table '{}' already contains {} rows. Use --drop to replace or truncate first",
                    table, count
                )
            }
            MigrationError::PacketSizeTooLarge {
                configured,
                maximum,
            } => {
                write!(
                    f,
                    "Configured packet size ({} bytes) exceeds MySQL max_allowed_packet ({} bytes)",
                    configured, maximum
                )
            }
            MigrationError::ColumnParseFailed { column, reason } => {
                write!(f, "Failed to parse column '{}': {}", column, reason)
            }
            MigrationError::ConstraintParseFailed { value, reason } => {
                write!(
                    f,
                    "Failed to parse constraint from '{}': {}",
                    value, reason
                )
            }
            MigrationError::InvalidDateTimeValue { reason } => {
                write!(f, "Invalid date/time value: {}", reason)
            }
            MigrationError::TaskPanicked { table } => {
                write!(f, "Migration task for table '{}' panicked", table)
            }
        }
    }
}

impl std::error::Error for MigrationError {}
