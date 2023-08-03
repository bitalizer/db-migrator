use std::fmt;

use crate::schema::{ColumnSchema, Constraint};

pub enum TableAction {
    Drop,
    Truncate,
}

impl fmt::Display for TableAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableAction::Drop => write!(f, "DROP"),
            TableAction::Truncate => write!(f, "TRUNCATE"),
        }
    }
}

pub fn build_insert_statement(table_name: &str, schema: &[ColumnSchema]) -> String {
    let column_names_string = schema
        .iter()
        .map(|column| column.column_name.as_str())
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "INSERT INTO `{}` ({}) VALUES",
        table_name, column_names_string
    )
}

pub fn build_reset_query(tables: &[String], action: &TableAction) -> String {
    tables
        .iter()
        .map(|table_name| {
            format!(
                "{} TABLE `{}`;",
                action.to_string().to_uppercase(),
                table_name
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn build_create_constraints(table_name: &str, schema: &[ColumnSchema]) -> Option<String> {
    let constraints: Vec<String> = schema
        .iter()
        .filter_map(|column| {
            column
                .constraints
                .as_ref()
                .map(|constraints| match constraints {
                    //Constraint::PrimaryKey => format!("ADD PRIMARY KEY(`{}`)", column.column_name),
                    Constraint::ForeignKey {
                        referenced_table,
                        referenced_column,
                    } => format!(
                        "ADD FOREIGN KEY(`{}`) REFERENCES `{}`(`{}`) ON DELETE CASCADE",
                        column.column_name, referenced_table, referenced_column
                    ),
                    Constraint::Unique => format!("ADD UNIQUE(`{}`)", column.column_name),
                    Constraint::Check(check_clause) => format!("ADD CHECK ({})", check_clause),
                    Constraint::Default(default_value) => format!("ADD DEFAULT {}", default_value),
                    _ => String::new(),
                })
        })
        .filter(|constraint| !constraint.is_empty())
        .collect();

    if constraints.is_empty() {
        return None;
    }

    let alter_table_query = format!(
        "SET FOREIGN_KEY_CHECKS=0; ALTER TABLE `{}` {}",
        table_name,
        constraints.join(", ")
    );

    Some(alter_table_query)
}

pub fn build_create_table_query(table_name: &str, schema: &[ColumnSchema]) -> String {
    let columns: Vec<String> = schema
        .iter()
        .map(|column| {
            let mut result_str = String::new();

            result_str.push_str(&column.column_name);
            result_str.push(' '); // Add a space after column_name

            result_str.push_str(&column.data_type);
            if let Some(max_length) = column.character_maximum_length {
                result_str.push_str(&format!("({})", max_length));
            } else if let Some(precision) = column.numeric_precision {
                if let Some(scale) = column.numeric_scale {
                    result_str.push_str(&format!("({}, {})", precision, scale));
                } else {
                    result_str.push_str(&format!("({})", precision));
                }
            }

            // Add constraints if it contains Constraint::PrimaryKey
            if let Some(constraint) = &column.constraints {
                if *constraint == Constraint::PrimaryKey {
                    result_str.push_str(" PRIMARY KEY");
                }
                // You can add more checks for other constraint types if needed
            }

            result_str.push(' '); // Add a space after data_type and type_properties
            let nullable_property = if column.is_nullable {
                "NULL"
            } else {
                "NOT NULL"
            };
            result_str.push_str(nullable_property);

            result_str
        })
        .collect();

    let columns = columns.join(", ");
    let create_table_query = format!("CREATE TABLE `{}` ({})", table_name, columns);

    create_table_query
}
