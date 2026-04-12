use anyhow::Error;

pub fn print_error_chain(err: &Error) {
    let error_message = err
        .chain()
        .enumerate()
        .map(|(index, cause)| {
            if index == 0 {
                cause.to_string()
            } else {
                format!("       └> {}", cause)
            }
        })
        .collect::<Vec<String>>()
        .join("\n");

    error!("{}", error_message);
}

pub fn format_snake_case(name: &str) -> String {
    let mut result = String::with_capacity(name.len() + 4);
    let chars: Vec<char> = name.chars().collect();

    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                let prev = chars[i - 1];
                // Insert underscore if previous char is lowercase/digit,
                // OR if previous is uppercase but next is lowercase (end of acronym)
                if prev.is_lowercase() || prev.is_ascii_digit() {
                    result.push('_');
                } else if prev.is_uppercase() {
                    // Check if next char is lowercase (acronym boundary)
                    if i + 1 < chars.len() && chars[i + 1].is_lowercase() {
                        result.push('_');
                    }
                }
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_camel_case() {
        assert_eq!(format_snake_case("TableName"), "table_name");
    }

    #[test]
    fn test_multiple_words() {
        assert_eq!(format_snake_case("MyTableName"), "my_table_name");
    }

    #[test]
    fn test_consecutive_uppercase_acronym() {
        assert_eq!(format_snake_case("MyID"), "my_id");
    }

    #[test]
    fn test_acronym_followed_by_word() {
        assert_eq!(format_snake_case("HTMLParser"), "html_parser");
    }

    #[test]
    fn test_already_snake_case() {
        assert_eq!(format_snake_case("already_snake"), "already_snake");
    }

    #[test]
    fn test_single_char() {
        assert_eq!(format_snake_case("A"), "a");
    }

    #[test]
    fn test_empty_string() {
        assert_eq!(format_snake_case(""), "");
    }

    #[test]
    fn test_all_uppercase() {
        assert_eq!(format_snake_case("ABC"), "abc");
    }

    #[test]
    fn test_all_lowercase() {
        assert_eq!(format_snake_case("lowercase"), "lowercase");
    }

    #[test]
    fn test_with_numbers() {
        assert_eq!(format_snake_case("Table1Name"), "table1_name");
    }

    #[test]
    fn test_leading_uppercase() {
        assert_eq!(format_snake_case("UserID"), "user_id");
    }

    #[test]
    fn test_mixed_acronyms() {
        assert_eq!(format_snake_case("XMLHTTPRequest"), "xmlhttp_request");
    }

    #[test]
    fn test_single_word_capitalized() {
        assert_eq!(format_snake_case("Users"), "users");
    }

    #[test]
    fn test_underscore_preserved() {
        assert_eq!(format_snake_case("my_Table"), "my_table");
    }

    #[test]
    fn test_number_at_end() {
        assert_eq!(format_snake_case("Column123"), "column123");
    }
}
