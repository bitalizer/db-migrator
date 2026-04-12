use crate::common::errors::MigrationError;

#[derive(Debug, Clone, PartialEq)]
pub enum Constraint {
    PrimaryKey,
    ForeignKey {
        referenced_table: String,
        referenced_column: String,
    },
    Unique,
    Check(String),
    Default(String),
}

impl Constraint {
    pub(crate) fn from_str(s: &str) -> Result<Option<Self>, MigrationError> {
        let s = s.trim();

        if s.is_empty() {
            return Ok(None);
        }

        if s.starts_with("PRIMARY KEY") {
            Ok(Some(Constraint::PrimaryKey))
        } else if s.starts_with("FOREIGN KEY") {
            let parts: Vec<&str> = s.split(',').map(|p| p.trim()).collect();
            if parts.len() == 3 {
                let referenced_table = parts[1].to_string();
                let referenced_column = parts[2].to_string();

                Ok(Some(Constraint::ForeignKey {
                    referenced_table,
                    referenced_column,
                }))
            } else {
                Err(MigrationError::ConstraintParseFailed {
                    value: s.to_string(),
                    reason: format!(
                        "FOREIGN KEY constraint requires 3 comma-separated parts, got {}",
                        parts.len()
                    ),
                })
            }
        } else if s == "UNIQUE" {
            Ok(Some(Constraint::Unique))
        } else if s.starts_with("CHECK") {
            let check_clause = s.trim_matches(|c| c == '(' || c == ')').to_string();
            Ok(Some(Constraint::Check(check_clause)))
        } else if s.starts_with("DEFAULT") {
            let default_value = s.trim_start_matches("DEFAULT ").to_string();
            Ok(Some(Constraint::Default(default_value)))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primary_key() {
        let result = Constraint::from_str("PRIMARY KEY").unwrap();
        assert_eq!(result, Some(Constraint::PrimaryKey));
    }

    #[test]
    fn test_foreign_key_valid() {
        let result = Constraint::from_str("FOREIGN KEY, users, id").unwrap();
        assert_eq!(
            result,
            Some(Constraint::ForeignKey {
                referenced_table: "users".to_string(),
                referenced_column: "id".to_string(),
            })
        );
    }

    #[test]
    fn test_foreign_key_malformed_missing_parts() {
        let result = Constraint::from_str("FOREIGN KEY, users");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("3 comma-separated parts"),
            "Error message was: {}",
            err
        );
    }

    #[test]
    fn test_foreign_key_malformed_too_many_parts() {
        let result = Constraint::from_str("FOREIGN KEY, a, b, c");
        assert!(result.is_err());
    }

    #[test]
    fn test_unique() {
        let result = Constraint::from_str("UNIQUE").unwrap();
        assert_eq!(result, Some(Constraint::Unique));
    }

    #[test]
    fn test_check_constraint() {
        let result = Constraint::from_str("CHECK (age > 0)").unwrap();
        assert!(matches!(result, Some(Constraint::Check(_))));
        if let Some(Constraint::Check(clause)) = result {
            assert!(clause.contains("age > 0"));
        }
    }

    #[test]
    fn test_default_constraint() {
        let result = Constraint::from_str("DEFAULT 0").unwrap();
        assert_eq!(result, Some(Constraint::Default("0".to_string())));
    }

    #[test]
    fn test_default_constraint_string_value() {
        let result = Constraint::from_str("DEFAULT 'hello'").unwrap();
        assert_eq!(result, Some(Constraint::Default("'hello'".to_string())));
    }

    #[test]
    fn test_empty_string() {
        let result = Constraint::from_str("").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_whitespace_only() {
        let result = Constraint::from_str("   ").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_unknown_constraint() {
        let result = Constraint::from_str("SOMETHING_ELSE").unwrap();
        assert_eq!(result, None);
    }
}
