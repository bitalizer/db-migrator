use anyhow::{Result, anyhow};

use crate::common::constraints::Constraint;
use crate::common::errors::MigrationError;
use crate::common::helpers::format_snake_case;
use crate::common::schema::ColumnSchema;
use crate::mappings::Mappings;

pub struct TableSchemaMapper;

impl TableSchemaMapper {
    pub fn map_schema(
        mappings: &Mappings,
        table_schema: &[ColumnSchema],
        format: bool,
    ) -> Result<Vec<ColumnSchema>> {
        table_schema
            .iter()
            .map(|column| {
                let mapping = mappings.get(&column.data_type).ok_or_else(|| {
                    anyhow!(MigrationError::MappingNotFound {
                        data_type: column.data_type.clone(),
                    })
                })?;

                let new_column_name = if format {
                    format_snake_case(&column.column_name)
                } else {
                    column.column_name.clone()
                };

                let new_constraints = column.constraints.clone();
                let new_data_type = mapping.to_type.clone();

                // Check if new_constraints contain foreign key and format snake case
                let updated_constraints = if let Some(new_constraints) = new_constraints {
                    match new_constraints {
                        Constraint::ForeignKey {
                            referenced_table,
                            referenced_column,
                        } if format => Some(Constraint::ForeignKey {
                            referenced_table: format_snake_case(&referenced_table),
                            referenced_column: format_snake_case(&referenced_column),
                        }),
                        other_constraint => Some(other_constraint),
                    }
                } else {
                    None
                };

                let (new_characters_maximum_length, new_numeric_precision, new_numeric_scale) =
                    if !mapping.type_parameters {
                        (None, None, None)
                    } else {
                        let new_characters_maximum_length = column
                            .character_maximum_length
                            .and_then(|length| {
                                if length == -1 {
                                    Some(65535)
                                } else if (1..=65535).contains(&length) {
                                    Some(length)
                                } else {
                                    None
                                }
                            })
                            .or_else(|| mapping.max_characters_length.map(|value| value as i32));

                        let new_numeric_precision =
                            column.numeric_precision.or(mapping.numeric_precision);
                        let new_numeric_scale = if column.numeric_scale == Some(0) {
                            None
                        } else {
                            column
                                .numeric_scale
                                .or(mapping.numeric_scale.map(|value| value as i32))
                        };

                        (
                            new_characters_maximum_length,
                            new_numeric_precision,
                            new_numeric_scale,
                        )
                    };

                Ok(ColumnSchema {
                    column_name: new_column_name,
                    data_type: new_data_type,
                    character_maximum_length: new_characters_maximum_length,
                    numeric_precision: new_numeric_precision,
                    numeric_scale: new_numeric_scale,
                    is_nullable: column.is_nullable,
                    constraints: updated_constraints,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mappings::Mapping;

    fn make_mappings(entries: Vec<(&str, &str, bool)>) -> Mappings {
        Mappings::from_entries(
            entries
                .into_iter()
                .map(|(from, to, params)| {
                    (
                        from.to_string(),
                        Mapping {
                            to_type: to.to_string(),
                            type_parameters: params,
                            numeric_precision: None,
                            numeric_scale: None,
                            max_characters_length: None,
                        },
                    )
                })
                .collect(),
        )
    }

    fn make_column(name: &str, data_type: &str) -> ColumnSchema {
        ColumnSchema {
            column_name: name.to_string(),
            data_type: data_type.to_string(),
            character_maximum_length: None,
            numeric_precision: None,
            numeric_scale: None,
            is_nullable: true,
            constraints: None,
        }
    }

    #[test]
    fn test_map_schema_basic() {
        let mappings = make_mappings(vec![("int", "int", false)]);
        let schema = vec![make_column("id", "int")];

        let result = TableSchemaMapper::map_schema(&mappings, &schema, false).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column_name, "id");
        assert_eq!(result[0].data_type, "int");
    }

    #[test]
    fn test_map_schema_missing_mapping() {
        let mappings = make_mappings(vec![("int", "int", false)]);
        let schema = vec![make_column("data", "xml")];

        let result = TableSchemaMapper::map_schema(&mappings, &schema, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("xml"));
    }

    #[test]
    fn test_map_schema_snake_case() {
        let mappings = make_mappings(vec![("varchar", "varchar", true)]);
        let schema = vec![make_column("UserName", "varchar")];

        let result = TableSchemaMapper::map_schema(&mappings, &schema, true).unwrap();
        assert_eq!(result[0].column_name, "user_name");
    }

    #[test]
    fn test_map_schema_preserves_nullable() {
        let mappings = make_mappings(vec![("int", "int", false)]);
        let mut col = make_column("id", "int");
        col.is_nullable = false;

        let result = TableSchemaMapper::map_schema(&mappings, &[col], false).unwrap();
        assert!(!result[0].is_nullable);
    }

    #[test]
    fn test_map_schema_type_parameters_with_length() {
        let mappings = make_mappings(vec![("varchar", "varchar", true)]);
        let mut col = make_column("name", "varchar");
        col.character_maximum_length = Some(255);

        let result = TableSchemaMapper::map_schema(&mappings, &[col], false).unwrap();
        assert_eq!(result[0].character_maximum_length, Some(255));
    }

    #[test]
    fn test_map_schema_type_parameters_max_length_becomes_65535() {
        let mappings = make_mappings(vec![("nvarchar", "longtext", true)]);
        let mut col = make_column("description", "nvarchar");
        col.character_maximum_length = Some(-1); // MSSQL uses -1 for MAX

        let result = TableSchemaMapper::map_schema(&mappings, &[col], false).unwrap();
        assert_eq!(result[0].character_maximum_length, Some(65535));
    }

    #[test]
    fn test_map_schema_no_type_parameters_clears_precision() {
        let mappings = make_mappings(vec![("text", "text", false)]);
        let mut col = make_column("body", "text");
        col.numeric_precision = Some(10);
        col.numeric_scale = Some(2);
        col.character_maximum_length = Some(500);

        let result = TableSchemaMapper::map_schema(&mappings, &[col], false).unwrap();
        assert_eq!(result[0].numeric_precision, None);
        assert_eq!(result[0].numeric_scale, None);
        assert_eq!(result[0].character_maximum_length, None);
    }

    #[test]
    fn test_map_schema_foreign_key_snake_case() {
        let mappings = make_mappings(vec![("int", "int", false)]);
        let mut col = make_column("UserId", "int");
        col.constraints = Some(Constraint::ForeignKey {
            referenced_table: "UserAccounts".to_string(),
            referenced_column: "AccountId".to_string(),
        });

        let result = TableSchemaMapper::map_schema(&mappings, &[col], true).unwrap();
        if let Some(Constraint::ForeignKey {
            referenced_table,
            referenced_column,
        }) = &result[0].constraints
        {
            assert_eq!(referenced_table, "user_accounts");
            assert_eq!(referenced_column, "account_id");
        } else {
            panic!("Expected ForeignKey constraint");
        }
    }

    #[test]
    fn test_map_schema_foreign_key_no_format() {
        let mappings = make_mappings(vec![("int", "int", false)]);
        let mut col = make_column("UserId", "int");
        col.constraints = Some(Constraint::ForeignKey {
            referenced_table: "UserAccounts".to_string(),
            referenced_column: "AccountId".to_string(),
        });

        let result = TableSchemaMapper::map_schema(&mappings, &[col], false).unwrap();
        if let Some(Constraint::ForeignKey {
            referenced_table,
            referenced_column,
        }) = &result[0].constraints
        {
            assert_eq!(referenced_table, "UserAccounts");
            assert_eq!(referenced_column, "AccountId");
        } else {
            panic!("Expected ForeignKey constraint");
        }
    }

    #[test]
    fn test_map_schema_zero_scale_becomes_none() {
        let mappings = make_mappings(vec![("int", "int", true)]);
        let mut col = make_column("count", "int");
        col.numeric_precision = Some(10);
        col.numeric_scale = Some(0);

        let result = TableSchemaMapper::map_schema(&mappings, &[col], false).unwrap();
        assert_eq!(result[0].numeric_precision, Some(10));
        assert_eq!(result[0].numeric_scale, None);
    }
}
