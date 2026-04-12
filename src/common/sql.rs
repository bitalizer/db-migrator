/// Escape a MSSQL identifier by wrapping in `[]` and doubling any `]` inside.
/// Example: `my]table` -> `[my]]table]`
pub fn escape_mssql_identifier(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// Escape a MySQL identifier by wrapping in backticks and doubling any backtick inside.
/// Example: `my`table` -> `` `my``table` ``
pub fn escape_mysql_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Escape a string value for use in a SQL string literal (single-quoted).
/// Doubles single quotes and escapes backslashes.
/// Example: `O'Brien` -> `O''Brien`
pub fn escape_sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_mssql_identifier_simple() {
        assert_eq!(escape_mssql_identifier("users"), "[users]");
    }

    #[test]
    fn test_escape_mssql_identifier_with_bracket() {
        assert_eq!(escape_mssql_identifier("my]table"), "[my]]table]");
    }

    #[test]
    fn test_escape_mssql_identifier_with_spaces() {
        assert_eq!(escape_mssql_identifier("my table"), "[my table]");
    }

    #[test]
    fn test_escape_mssql_identifier_reserved_word() {
        assert_eq!(escape_mssql_identifier("select"), "[select]");
    }

    #[test]
    fn test_escape_mysql_identifier_simple() {
        assert_eq!(escape_mysql_identifier("users"), "`users`");
    }

    #[test]
    fn test_escape_mysql_identifier_with_backtick() {
        assert_eq!(escape_mysql_identifier("my`table"), "`my``table`");
    }

    #[test]
    fn test_escape_mysql_identifier_with_spaces() {
        assert_eq!(escape_mysql_identifier("my table"), "`my table`");
    }

    #[test]
    fn test_escape_mysql_identifier_reserved_word() {
        assert_eq!(escape_mysql_identifier("select"), "`select`");
    }

    #[test]
    fn test_escape_sql_string_simple() {
        assert_eq!(escape_sql_string("hello"), "hello");
    }

    #[test]
    fn test_escape_sql_string_with_quote() {
        assert_eq!(escape_sql_string("O'Brien"), "O''Brien");
    }

    #[test]
    fn test_escape_sql_string_with_backslash() {
        assert_eq!(escape_sql_string("path\\to"), "path\\\\to");
    }

    #[test]
    fn test_escape_sql_string_with_both() {
        assert_eq!(escape_sql_string("it's a\\path"), "it''s a\\\\path");
    }

    #[test]
    fn test_escape_sql_string_empty() {
        assert_eq!(escape_sql_string(""), "");
    }

    #[test]
    fn test_escape_mysql_identifier_empty() {
        assert_eq!(escape_mysql_identifier(""), "``");
    }

    #[test]
    fn test_escape_mssql_identifier_multiple_brackets() {
        assert_eq!(escape_mssql_identifier("a]b]c"), "[a]]b]]c]");
    }
}
