use std::fmt;

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
