use anyhow::{Result, anyhow};

use crate::common::constraints::Constraint;
use crate::common::helpers::format_snake_case;
use crate::common::mysql_type::MySqlType;
use crate::common::schema::ColumnSchema;
use crate::common::target_schema::TargetColumn;
use crate::migrate::type_registry::TypeRegistry;

pub struct TableSchemaMapper;

impl TableSchemaMapper {
    pub fn map_schema(
        registry: &TypeRegistry,
        source_schema: &[ColumnSchema],
        format: bool,
    ) -> Result<Vec<TargetColumn>> {
        source_schema
            .iter()
            .map(|column| {
                let entry = registry.get(column.data_type);

                let column_name = if format {
                    format_snake_case(&column.column_name)
                } else {
                    column.column_name.clone()
                };

                // Build length
                let length = if entry.carry_length {
                    let source_length = column.character_maximum_length;
                    match source_length {
                        Some(-1) => {
                            return Err(anyhow!(
                                "Column '{}' has MAX length (-1) but is mapped to '{}' which requires a fixed length. \
                                 Use an override to map '{}' to longtext or longblob instead.",
                                column.column_name, entry.mysql_type, column.data_type
                            ));
                        }
                        Some(len) if len > 0 => {
                            let len = len as u32;
                            if let Some(max) = entry.mysql_type.max_length() {
                                if len > max {
                                    return Err(anyhow!(
                                        "Column '{}' length {} exceeds MySQL {} max length {}. \
                                         Use an override to map to longtext/longblob.",
                                        column.column_name, len, entry.mysql_type, max
                                    ));
                                }
                            }
                            Some(len)
                        }
                        _ => entry.default_length,
                    }
                } else {
                    None
                };

                // Build precision
                let precision = if entry.carry_precision {
                    column.numeric_precision.or(entry.default_precision)
                } else {
                    None
                };

                // Build scale (safe i32 → u8 conversion)
                let scale = if entry.carry_precision {
                    let source_scale = column.numeric_scale
                        .and_then(|s| u8::try_from(s).ok());
                    source_scale.or(entry.default_scale)
                } else {
                    None
                };

                let data_type = MySqlType {
                    base_type: entry.mysql_type,
                    length,
                    precision,
                    scale,
                    unsigned: entry.unsigned,
                    zerofill: entry.zerofill,
                };

                // Format FK references if snake_case enabled
                let constraints = column.constraints.clone().map(|c| match c {
                    Constraint::ForeignKey { referenced_table, referenced_column } if format => {
                        Constraint::ForeignKey {
                            referenced_table: format_snake_case(&referenced_table),
                            referenced_column: format_snake_case(&referenced_column),
                        }
                    }
                    other => other,
                });

                Ok(TargetColumn {
                    column_name,
                    data_type,
                    is_nullable: column.is_nullable,
                    constraints,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::constraints::Constraint;
    use crate::common::mssql_type::MssqlType;
    use crate::common::mysql_type::MySqlBaseType;
    use crate::common::schema::ColumnSchema;
    use crate::migrate::type_registry::TypeRegistry;

    fn default_registry() -> TypeRegistry {
        TypeRegistry::with_defaults()
    }

    fn make_source(name: &str, data_type: MssqlType) -> ColumnSchema {
        ColumnSchema {
            column_name: name.to_string(),
            data_type,
            character_maximum_length: None,
            numeric_precision: None,
            numeric_scale: None,
            is_nullable: true,
            constraints: None,
        }
    }

    #[test]
    fn test_map_int() {
        let registry = default_registry();
        let source = vec![make_source("id", MssqlType::Int)];
        let result = TableSchemaMapper::map_schema(&registry, &source, false).unwrap();
        assert_eq!(result[0].data_type.base_type, MySqlBaseType::Int);
        assert!(!result[0].data_type.unsigned);
    }

    #[test]
    fn test_map_decimal_carries_precision() {
        let registry = default_registry();
        let mut col = make_source("price", MssqlType::Decimal);
        col.numeric_precision = Some(10);
        col.numeric_scale = Some(2);
        let result = TableSchemaMapper::map_schema(&registry, &[col], false).unwrap();
        assert_eq!(result[0].data_type.precision, Some(10));
        assert_eq!(result[0].data_type.scale, Some(2));
    }

    #[test]
    fn test_map_money_uses_defaults() {
        let registry = default_registry();
        let col = make_source("amount", MssqlType::Money);
        let result = TableSchemaMapper::map_schema(&registry, &[col], false).unwrap();
        assert_eq!(result[0].data_type.precision, Some(19));
        assert_eq!(result[0].data_type.scale, Some(4));
    }

    #[test]
    fn test_map_varchar_carries_length() {
        let registry = default_registry();
        let mut col = make_source("name", MssqlType::Varchar);
        col.character_maximum_length = Some(100);
        let result = TableSchemaMapper::map_schema(&registry, &[col], false).unwrap();
        assert_eq!(result[0].data_type.length, Some(100));
    }

    #[test]
    fn test_map_varchar_uses_default_length() {
        let registry = default_registry();
        let col = make_source("name", MssqlType::Varchar);
        let result = TableSchemaMapper::map_schema(&registry, &[col], false).unwrap();
        assert_eq!(result[0].data_type.length, Some(255));
    }

    #[test]
    fn test_map_nvarchar_becomes_longtext() {
        let registry = default_registry();
        let col = make_source("desc", MssqlType::NVarchar);
        let result = TableSchemaMapper::map_schema(&registry, &[col], false).unwrap();
        assert_eq!(result[0].data_type.base_type, MySqlBaseType::LongText);
        assert_eq!(result[0].data_type.length, None);
    }

    #[test]
    fn test_map_varchar_max_errors() {
        let registry = default_registry();
        let mut col = make_source("data", MssqlType::Varchar);
        col.character_maximum_length = Some(-1); // MSSQL MAX
        let result = TableSchemaMapper::map_schema(&registry, &[col], false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("MAX"));
    }

    #[test]
    fn test_map_snake_case() {
        let registry = default_registry();
        let col = make_source("UserName", MssqlType::Varchar);
        let result = TableSchemaMapper::map_schema(&registry, &[col], true).unwrap();
        assert_eq!(result[0].column_name, "user_name");
    }

    #[test]
    fn test_map_foreign_key_snake_case() {
        let registry = default_registry();
        let mut col = make_source("UserId", MssqlType::Int);
        col.constraints = Some(Constraint::ForeignKey {
            referenced_table: "UserAccounts".to_string(),
            referenced_column: "AccountId".to_string(),
        });
        let result = TableSchemaMapper::map_schema(&registry, &[col], true).unwrap();
        if let Some(Constraint::ForeignKey { referenced_table, referenced_column }) = &result[0].constraints {
            assert_eq!(referenced_table, "user_accounts");
            assert_eq!(referenced_column, "account_id");
        } else {
            panic!("Expected ForeignKey");
        }
    }

    #[test]
    fn test_map_preserves_nullable() {
        let registry = default_registry();
        let mut col = make_source("id", MssqlType::Int);
        col.is_nullable = false;
        let result = TableSchemaMapper::map_schema(&registry, &[col], false).unwrap();
        assert!(!result[0].is_nullable);
    }

    #[test]
    fn test_map_preserves_primary_key() {
        let registry = default_registry();
        let mut col = make_source("id", MssqlType::Int);
        col.constraints = Some(Constraint::PrimaryKey);
        let result = TableSchemaMapper::map_schema(&registry, &[col], false).unwrap();
        assert_eq!(result[0].constraints, Some(Constraint::PrimaryKey));
    }

    #[test]
    fn test_map_length_exceeds_max_errors() {
        let registry = default_registry();
        let mut col = make_source("data", MssqlType::Varchar);
        col.character_maximum_length = Some(70000);
        let result = TableSchemaMapper::map_schema(&registry, &[col], false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds"));
    }

    #[test]
    fn test_map_scale_safe_conversion() {
        let registry = default_registry();
        let mut col = make_source("val", MssqlType::Decimal);
        col.numeric_precision = Some(10);
        col.numeric_scale = Some(300); // exceeds u8 range
        let result = TableSchemaMapper::map_schema(&registry, &[col], false).unwrap();
        // Falls back to default scale since 300 doesn't fit in u8
        assert_eq!(result[0].data_type.scale, Some(2)); // decimal default
    }
}
