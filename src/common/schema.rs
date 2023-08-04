use tiberius::Row;

use crate::common::constraints::Constraint;

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub column_name: String,
    pub data_type: String,
    pub character_maximum_length: Option<i32>,
    pub numeric_precision: Option<u8>,
    pub numeric_scale: Option<i32>,
    pub is_nullable: bool,
    pub constraints: Option<Constraint>,
}

impl ColumnSchema {
    pub fn from_row(row: &Row) -> Result<Self, Box<dyn std::error::Error>> {
        let column_name = Column::get(row, "COLUMN_NAME");
        let data_type = Column::get(row, "DATA_TYPE");
        let character_maximum_length = Column::get(row, "CHARACTER_MAXIMUM_LENGTH");
        let numeric_precision = Column::get(row, "NUMERIC_PRECISION");
        let numeric_scale = Column::get(row, "NUMERIC_SCALE");
        let is_nullable = parse_bool_from_string(Column::get(row, "IS_NULLABLE"));
        let constraints = Constraint::from_str(Column::get(row, "CONSTRAINTS")).unwrap();

        Ok(ColumnSchema {
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            constraints,
        })
    }
}

pub trait Column {
    fn get(row: &Row, col_name: &str) -> Self;
}

impl Column for i32 {
    fn get(row: &Row, col_name: &str) -> i32 {
        match row.try_get::<i32, _>(col_name) {
            Ok(Some(value)) => value,
            _ => panic!("Failed to get column value"),
        }
    }
}

impl Column for Option<i32> {
    fn get(row: &Row, col_name: &str) -> Option<i32> {
        row.get::<i32, _>(col_name)
    }
}

impl Column for Option<u8> {
    fn get(row: &Row, col_name: &str) -> Option<u8> {
        row.get::<u8, _>(col_name)
    }
}

impl Column for Option<i64> {
    fn get(row: &Row, col_name: &str) -> Option<i64> {
        row.get::<i64, _>(col_name)
    }
}

impl Column for String {
    fn get(row: &Row, col_name: &str) -> String {
        row.try_get::<&str, _>(col_name)
            .unwrap_or_default()
            .unwrap_or_default()
            .to_string()
    }
}

impl Column for Option<String> {
    fn get(row: &Row, col_name: &str) -> Option<String> {
        row.get::<&str, _>(col_name).map(|data| data.to_string())
    }
}

fn parse_bool_from_string(s: String) -> bool {
    match s.to_lowercase().as_str() {
        "yes" => true,
        "no" => false,
        _ => panic!("Invalid boolean value"),
    }
}
