use crate::schema::ColumnSchema;
use prettytable::{format, row, Table};

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

pub fn print_schema_info(table_schema: &[ColumnSchema]) {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_BORDERS_ONLY);

    table.add_row(row![bFg => "Column Name", "Data Type", "Character Maximum Length", "Numeric Precision", "Numeric Scale"]);

    for column in table_schema {
        let character_maximum_length = column
            .character_maximum_length
            .map(|length| format!("{:?}", length));
        let precision = column.numeric_precision.map(|p| format!("{:?}", p));
        let scale = column.numeric_scale.map(|s| format!("{:?}", s));

        table.add_row(row![
            bFg => column.column_name,
            column.data_type,
            character_maximum_length.unwrap_or_else(|| "-".to_owned()),
            precision.unwrap_or_else(|| "-".to_owned()),
            scale.unwrap_or_else(|| "-".to_owned())
        ]);
    }

    table.printstd();
}
