#[derive(Debug, Clone, PartialEq)]
pub enum Constraint {
    PrimaryKey,
    ForeignKey {
        referenced_table: String,
        referenced_column: String,
    },
    Unique,
    Check(String),
    // The argument will store the check clause string
    Default(String), // The argument will store the default value string
}

impl Constraint {
    pub(crate) fn from_str(s: String) -> Result<Option<Self>, ()> {
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
                Err(()) // Return an error if the FOREIGN KEY constraint format is incorrect
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
            Ok(None) // Return None for no constraint
        }
    }
}
