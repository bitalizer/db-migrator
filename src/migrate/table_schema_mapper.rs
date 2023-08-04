use crate::common::constraints::Constraint;
use crate::common::helpers::format_snake_case;
use crate::common::schema::ColumnSchema;
use crate::mappings::Mappings;

pub struct TableSchemaMapper;

impl TableSchemaMapper {
    pub fn map_schema(
        mappings: &Mappings,
        table_schema: &[ColumnSchema],
        format: bool,
    ) -> Vec<ColumnSchema> {
        table_schema
            .iter()
            .map(|column| {
                let mapping = mappings.get(&column.data_type).unwrap_or_else(|| {
                    panic!("Mapping not found for data type: {}", column.data_type)
                });

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

                ColumnSchema {
                    column_name: new_column_name,
                    data_type: new_data_type,
                    character_maximum_length: new_characters_maximum_length,
                    numeric_precision: new_numeric_precision,
                    numeric_scale: new_numeric_scale,
                    is_nullable: column.is_nullable,
                    constraints: updated_constraints,
                }
            })
            .collect()
    }
}
