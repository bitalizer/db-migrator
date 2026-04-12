use crate::common::constraints::Constraint;
use crate::common::sql::escape_mysql_identifier;
use crate::common::target_schema::TargetColumn;
use crate::insert::table_action::TableAction;

pub fn build_insert_statement(table_name: &str, schema: &[TargetColumn]) -> String {
    let column_names_string = schema
        .iter()
        .map(|column| escape_mysql_identifier(&column.column_name))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "INSERT INTO {} ({}) VALUES",
        escape_mysql_identifier(table_name),
        column_names_string
    )
}

pub fn build_reset_query(tables: &[String], action: &TableAction) -> String {
    tables
        .iter()
        .map(|table_name| {
            format!(
                "{} TABLE {};",
                action.to_string().to_uppercase(),
                escape_mysql_identifier(table_name)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn build_create_constraints(
    table_name: &str,
    schema: &[TargetColumn],
    formatted_tables: &[String],
) -> Option<String> {
    let constraints: Vec<String> = schema
        .iter()
        .filter_map(|column| {
            column
                .constraints
                .as_ref()
                .filter(|constraint| {
                    match constraint {
                        Constraint::ForeignKey {
                            referenced_table,
                            referenced_column,
                            ..
                        } => {
                            if formatted_tables.contains(referenced_table) {
                                debug!("Creating constraint in table: {} on column `{}` with foreign key reference to `{}.{}`", table_name, column.column_name, referenced_table, referenced_column);
                                true
                            } else {
                                warn!(
                                    "Skipping constraint in table {} on column `{}`with foreign key reference to `{}.{}`",
                                    table_name, column.column_name, referenced_table, referenced_column
                                );
                                false
                            }
                        }
                        _ => true,
                    }
                })
                .map(|constraints| match constraints {
                    Constraint::ForeignKey {
                        referenced_table,
                        referenced_column,
                    } => format!(
                        "ADD FOREIGN KEY({}) REFERENCES {}({}) ON DELETE CASCADE",
                        escape_mysql_identifier(&column.column_name),
                        escape_mysql_identifier(referenced_table),
                        escape_mysql_identifier(referenced_column)
                    ),
                    Constraint::Unique => {
                        format!("ADD UNIQUE({})", escape_mysql_identifier(&column.column_name))
                    }
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
        "SET FOREIGN_KEY_CHECKS=0; ALTER TABLE {} {}",
        escape_mysql_identifier(table_name),
        constraints.join(", ")
    );

    Some(alter_table_query)
}

pub fn build_create_table_query(table_name: &str, schema: &[TargetColumn]) -> String {
    let columns: Vec<String> = schema
        .iter()
        .map(|column| {
            let mut result_str = String::new();

            result_str.push_str(&escape_mysql_identifier(&column.column_name));
            result_str.push(' ');

            result_str.push_str(&column.data_type.to_sql());

            if let Some(constraint) = &column.constraints
                && *constraint == Constraint::PrimaryKey
            {
                result_str.push_str(" PRIMARY KEY");
            }

            result_str.push(' ');
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
    format!(
        "CREATE TABLE {} ({})",
        escape_mysql_identifier(table_name),
        columns
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::constraints::Constraint;
    use crate::common::mysql_type::{MySqlBaseType, MySqlType};
    use crate::common::target_schema::TargetColumn;

    fn make_column(name: &str, base_type: MySqlBaseType, nullable: bool) -> TargetColumn {
        TargetColumn {
            column_name: name.to_string(),
            data_type: MySqlType {
                base_type,
                length: None,
                precision: None,
                scale: None,
                unsigned: false,
                zerofill: false,
            },
            is_nullable: nullable,
            constraints: None,
        }
    }

    #[test]
    fn test_build_insert_statement() {
        let schema = vec![
            make_column("id", MySqlBaseType::Int, false),
            make_column("name", MySqlBaseType::Varchar, true),
        ];
        let result = build_insert_statement("users", &schema);
        assert_eq!(result, "INSERT INTO `users` (`id`, `name`) VALUES");
    }

    #[test]
    fn test_build_insert_statement_single_column() {
        let schema = vec![make_column("id", MySqlBaseType::Int, false)];
        let result = build_insert_statement("test", &schema);
        assert_eq!(result, "INSERT INTO `test` (`id`) VALUES");
    }

    #[test]
    fn test_build_create_table_basic() {
        let schema = vec![
            make_column("id", MySqlBaseType::Int, false),
            make_column("name", MySqlBaseType::Varchar, true),
        ];
        let result = build_create_table_query("users", &schema);
        assert!(result.starts_with("CREATE TABLE `users`"));
        assert!(result.contains("`id` int NOT NULL"));
        assert!(result.contains("`name` varchar NULL"));
    }

    #[test]
    fn test_build_create_table_with_primary_key() {
        let mut col = make_column("id", MySqlBaseType::Int, false);
        col.constraints = Some(Constraint::PrimaryKey);
        let schema = vec![col];
        let result = build_create_table_query("test", &schema);
        assert!(result.contains("PRIMARY KEY"));
    }

    #[test]
    fn test_build_create_table_with_char_length() {
        let mut col = make_column("name", MySqlBaseType::Varchar, true);
        col.data_type.length = Some(255);
        let schema = vec![col];
        let result = build_create_table_query("test", &schema);
        assert!(result.contains("varchar(255)"));
    }

    #[test]
    fn test_build_create_table_with_precision_and_scale() {
        let mut col = make_column("price", MySqlBaseType::Decimal, false);
        col.data_type.precision = Some(10);
        col.data_type.scale = Some(2);
        let schema = vec![col];
        let result = build_create_table_query("products", &schema);
        assert!(result.contains("decimal(10, 2)"));
    }

    #[test]
    fn test_build_create_table_with_precision_only() {
        let mut col = make_column("count", MySqlBaseType::Float, false);
        col.data_type.precision = Some(10);
        let schema = vec![col];
        let result = build_create_table_query("test", &schema);
        assert!(result.contains("float(10)"));
    }

    #[test]
    fn test_build_reset_query_drop() {
        let tables = vec!["users".to_string(), "orders".to_string()];
        let result = build_reset_query(&tables, &TableAction::Drop);
        assert!(result.contains("DROP TABLE `users`;"));
        assert!(result.contains("DROP TABLE `orders`;"));
    }

    #[test]
    fn test_build_reset_query_truncate() {
        let tables = vec!["users".to_string()];
        let result = build_reset_query(&tables, &TableAction::Truncate);
        assert!(result.contains("TRUNCATE TABLE `users`;"));
    }

    #[test]
    fn test_build_create_constraints_with_foreign_key() {
        let mut col = make_column("user_id", MySqlBaseType::Int, false);
        col.constraints = Some(Constraint::ForeignKey {
            referenced_table: "users".to_string(),
            referenced_column: "id".to_string(),
        });
        let schema = vec![col];
        let formatted_tables = vec!["users".to_string(), "orders".to_string()];
        let result = build_create_constraints("orders", &schema, &formatted_tables);
        assert!(result.is_some());
        let query = result.unwrap();
        assert!(
            query.contains("ADD FOREIGN KEY(`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE")
        );
    }

    #[test]
    fn test_build_create_constraints_skips_missing_table() {
        let mut col = make_column("user_id", MySqlBaseType::Int, false);
        col.constraints = Some(Constraint::ForeignKey {
            referenced_table: "nonexistent".to_string(),
            referenced_column: "id".to_string(),
        });
        let schema = vec![col];
        let formatted_tables = vec!["orders".to_string()];
        let result = build_create_constraints("orders", &schema, &formatted_tables);
        assert!(result.is_none());
    }

    #[test]
    fn test_build_create_constraints_unique() {
        let mut col = make_column("email", MySqlBaseType::Varchar, false);
        col.constraints = Some(Constraint::Unique);
        let schema = vec![col];
        let formatted_tables = vec!["users".to_string()];
        let result = build_create_constraints("users", &schema, &formatted_tables);
        assert!(result.is_some());
        assert!(result.unwrap().contains("ADD UNIQUE(`email`)"));
    }

    #[test]
    fn test_build_create_constraints_no_constraints() {
        let schema = vec![make_column("id", MySqlBaseType::Int, false)];
        let formatted_tables = vec!["test".to_string()];
        let result = build_create_constraints("test", &schema, &formatted_tables);
        assert!(result.is_none());
    }

    #[test]
    fn test_build_create_constraints_check() {
        let mut col = make_column("age", MySqlBaseType::Int, false);
        col.constraints = Some(Constraint::Check("age > 0".to_string()));
        let schema = vec![col];
        let formatted_tables = vec!["users".to_string()];
        let result = build_create_constraints("users", &schema, &formatted_tables);
        assert!(result.is_some());
        assert!(result.unwrap().contains("ADD CHECK (age > 0)"));
    }

    #[test]
    fn test_build_insert_reserved_word_column() {
        let schema = vec![make_column("select", MySqlBaseType::Int, false)];
        let result = build_insert_statement("order", &schema);
        assert_eq!(result, "INSERT INTO `order` (`select`) VALUES");
    }

    #[test]
    fn test_build_create_table_backtick_in_name() {
        let schema = vec![make_column("col`name", MySqlBaseType::Int, false)];
        let result = build_create_table_query("my`table", &schema);
        assert!(result.contains("CREATE TABLE `my``table`"));
        assert!(result.contains("`col``name`"));
    }

    #[test]
    fn test_build_reset_query_reserved_word() {
        let tables = vec!["order".to_string(), "select".to_string()];
        let result = build_reset_query(&tables, &TableAction::Drop);
        assert!(result.contains("DROP TABLE `order`;"));
        assert!(result.contains("DROP TABLE `select`;"));
    }

    #[test]
    fn test_build_create_constraints_escaped_fk() {
        let mut col = make_column("group", MySqlBaseType::Int, false);
        col.constraints = Some(Constraint::ForeignKey {
            referenced_table: "order".to_string(),
            referenced_column: "select".to_string(),
        });
        let schema = vec![col];
        let formatted_tables = vec!["order".to_string()];
        let result = build_create_constraints("test", &schema, &formatted_tables);
        assert!(result.is_some());
        let query = result.unwrap();
        assert!(query.contains("ADD FOREIGN KEY(`group`) REFERENCES `order`(`select`)"));
    }
}
