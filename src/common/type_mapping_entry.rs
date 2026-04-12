use crate::common::mysql_type::MySqlBaseType;

/// A single MSSQL-to-MySQL type mapping entry.
/// Lives in common/ because both the built-in registry and user overrides need it.
#[derive(Debug, Clone)]
pub struct TypeMappingEntry {
    pub mysql_type: MySqlBaseType,
    pub carry_length: bool,
    pub carry_precision: bool,
    pub default_length: Option<u32>,
    pub default_precision: Option<u8>,
    pub default_scale: Option<u8>,
    pub unsigned: bool,
    pub zerofill: bool,
}
