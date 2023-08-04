use anyhow::Error;

pub fn print_error_chain(err: &Error) {
    // Concatenate the main context message along with its chain of errors
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

    // Print the error message
    error!("{}", error_message);
}

pub fn format_snake_case(column_name: &str) -> String {
    let mut formatted_name = String::new();
    let mut prev_char: Option<char> = None;

    for c in column_name.chars() {
        if c.is_uppercase() {
            if let Some(prev) = prev_char {
                if !(prev == '_' || prev.is_uppercase()) {
                    formatted_name.push('_');
                }
            }
            formatted_name.push(c.to_ascii_lowercase());
        } else {
            formatted_name.push(c);
        }

        prev_char = Some(c);
    }

    formatted_name
}
