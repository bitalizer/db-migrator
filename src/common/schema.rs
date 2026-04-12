use anyhow::{anyhow, Context, Result};
use tiberius::Row;

use crate::common::constraints::Constraint;
use crate::common::errors::MigrationError;

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
    pub fn from_row(row: &Row) -> Result<Self> {
        let column_name: String =
            Column::get(row, "COLUMN_NAME").context("Failed to read COLUMN_NAME")?;
        let data_type: String =
            Column::get(row, "DATA_TYPE").context("Failed to read DATA_TYPE")?;
        let character_maximum_length: Option<i32> = Column::get(row, "CHARACTER_MAXIMUM_LENGTH")
            .context("Failed to read CHARACTER_MAXIMUM_LENGTH")?;
        let numeric_precision: Option<u8> =
            Column::get(row, "NUMERIC_PRECISION").context("Failed to read NUMERIC_PRECISION")?;
        let numeric_scale: Option<i32> =
            Column::get(row, "NUMERIC_SCALE").context("Failed to read NUMERIC_SCALE")?;
        let is_nullable_str: String =
            Column::get(row, "IS_NULLABLE").context("Failed to read IS_NULLABLE")?;
        let is_nullable = parse_bool_from_string(&is_nullable_str)
            .with_context(|| format!("Failed to parse IS_NULLABLE value '{}'", is_nullable_str))?;
        let constraints_str: String =
            Column::get(row, "CONSTRAINTS").context("Failed to read CONSTRAINTS")?;
        let constraints = Constraint::from_str(&constraints_str)
            .map_err(|e| anyhow!(e))
            .with_context(|| format!("Failed to parse constraint for column '{}'", column_name))?;

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
    fn get(row: &Row, col_name: &str) -> Result<Self>
    where
        Self: Sized;
}

impl Column for i32 {
    fn get(row: &Row, col_name: &str) -> Result<i32> {
        match row.try_get::<i32, _>(col_name) {
            Ok(Some(value)) => Ok(value),
            Ok(None) => Err(MigrationError::ColumnParseFailed {
                column: col_name.to_string(),
                reason: "expected non-null i32 value, got NULL".to_string(),
            }
            .into()),
            Err(e) => Err(MigrationError::ColumnParseFailed {
                column: col_name.to_string(),
                reason: e.to_string(),
            }
            .into()),
        }
    }
}

impl Column for Option<i32> {
    fn get(row: &Row, col_name: &str) -> Result<Option<i32>> {
        row.try_get::<i32, _>(col_name).map_err(|e| {
            MigrationError::ColumnParseFailed {
                column: col_name.to_string(),
                reason: e.to_string(),
            }
            .into()
        })
    }
}

impl Column for Option<u8> {
    fn get(row: &Row, col_name: &str) -> Result<Option<u8>> {
        row.try_get::<u8, _>(col_name).map_err(|e| {
            MigrationError::ColumnParseFailed {
                column: col_name.to_string(),
                reason: e.to_string(),
            }
            .into()
        })
    }
}

impl Column for Option<i64> {
    fn get(row: &Row, col_name: &str) -> Result<Option<i64>> {
        row.try_get::<i64, _>(col_name).map_err(|e| {
            MigrationError::ColumnParseFailed {
                column: col_name.to_string(),
                reason: e.to_string(),
            }
            .into()
        })
    }
}

impl Column for String {
    fn get(row: &Row, col_name: &str) -> Result<String> {
        Ok(row
            .try_get::<&str, _>(col_name)
            .unwrap_or_default()
            .unwrap_or_default()
            .to_string())
    }
}

impl Column for Option<String> {
    fn get(row: &Row, col_name: &str) -> Result<Option<String>> {
        Ok(row.get::<&str, _>(col_name).map(|data| data.to_string()))
    }
}

fn parse_bool_from_string(s: &str) -> Result<bool> {
    match s.to_lowercase().as_str() {
        "yes" | "true" | "1" => Ok(true),
        "no" | "false" | "0" => Ok(false),
        _ => Err(anyhow!(
            "Invalid boolean value '{}': expected 'yes', 'no', 'true', 'false', '1', or '0'",
            s
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bool_yes() {
        assert!(parse_bool_from_string("YES").unwrap());
        assert!(parse_bool_from_string("yes").unwrap());
        assert!(parse_bool_from_string("Yes").unwrap());
    }

    #[test]
    fn test_parse_bool_no() {
        assert!(!parse_bool_from_string("NO").unwrap());
        assert!(!parse_bool_from_string("no").unwrap());
        assert!(!parse_bool_from_string("No").unwrap());
    }

    #[test]
    fn test_parse_bool_true_false() {
        assert!(parse_bool_from_string("true").unwrap());
        assert!(!parse_bool_from_string("false").unwrap());
    }

    #[test]
    fn test_parse_bool_numeric() {
        assert!(parse_bool_from_string("1").unwrap());
        assert!(!parse_bool_from_string("0").unwrap());
    }

    #[test]
    fn test_parse_bool_invalid() {
        let result = parse_bool_from_string("maybe");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("maybe"));
    }
}
