use anyhow::{Result, anyhow};
use std::collections::HashMap;

use crate::common::mssql_type::MssqlType;
use crate::common::mysql_type::MySqlBaseType;
use crate::common::type_mapping_entry::TypeMappingEntry;

#[derive(Clone, Debug)]
pub struct UserOverrides {
    overrides: HashMap<MssqlType, TypeMappingEntry>,
}

impl UserOverrides {
    pub fn empty() -> Self {
        UserOverrides {
            overrides: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.overrides.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&MssqlType, &TypeMappingEntry)> {
        self.overrides.iter()
    }

    pub(crate) fn from_toml(value: toml::Value) -> Result<UserOverrides> {
        let mappings_table = value
            .get("mappings")
            .ok_or(anyhow!("Missing [mappings] section"))?
            .as_table()
            .ok_or(anyhow!(
                "Invalid [mappings] format — expected key-value pairs"
            ))?;

        let mut overrides = HashMap::new();

        for (from_type_str, to_type_value) in mappings_table {
            let mssql_type = MssqlType::from_str(from_type_str).ok_or_else(|| {
                anyhow!(
                    "Unknown MSSQL type '{}'. Valid types: bit, tinyint, smallint, int, bigint, decimal, numeric, money, smallmoney, float, real, char, nchar, varchar, nvarchar, text, ntext, binary, varbinary, image, date, datetime, datetime2, smalldatetime, datetimeoffset, time, uniqueidentifier, timestamp, xml",
                    from_type_str
                )
            })?;

            let to_type_str = to_type_value
                .as_str()
                .ok_or(anyhow!(
                    "Invalid value for '{}' — expected a string like \"varchar(500)\"",
                    from_type_str
                ))?
                .trim();

            let entry = parse_to_type(to_type_str, from_type_str)?;
            overrides.insert(mssql_type, entry);
        }

        Ok(UserOverrides { overrides })
    }
}

fn parse_to_type(to_type_str: &str, from_type_str: &str) -> Result<TypeMappingEntry> {
    let (base_str, params_str) = if let Some(paren_start) = to_type_str.find('(') {
        let base = &to_type_str[..paren_start];
        let params = to_type_str[paren_start..]
            .trim_start_matches('(')
            .trim_end_matches(')');
        (base, Some(params))
    } else {
        (to_type_str, None)
    };

    let mysql_type = MySqlBaseType::from_str(base_str).ok_or_else(|| {
        anyhow!(
            "Unknown MySQL type '{}' in to_type for mapping from '{}'. Valid types: tinyint, smallint, int, bigint, decimal, float, real, char, varchar, text, longtext, binary, varbinary, longblob, datetime, timestamp, date, time",
            to_type_str, from_type_str
        )
    })?;

    let mut entry = TypeMappingEntry {
        mysql_type,
        carry_length: false,
        carry_precision: false,
        default_length: None,
        default_precision: None,
        default_scale: None,
        unsigned: false,
        zerofill: false,
    };

    if let Some(params) = params_str {
        if mysql_type.accepts_length() {
            let length: u32 = params
                .trim()
                .parse()
                .map_err(|_| anyhow!("Invalid length '{}' in to_type '{}'", params, to_type_str))?;
            let max = mysql_type.max_length().unwrap();
            if length > max {
                return Err(anyhow!(
                    "Length {} exceeds maximum {} for type '{}'. Use longtext/longblob for unlimited.",
                    length,
                    max,
                    mysql_type.as_str()
                ));
            }
            entry.carry_length = true;
            entry.default_length = Some(length);
        } else if mysql_type.accepts_precision() {
            let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();
            let precision: u8 = parts[0].parse().map_err(|_| {
                anyhow!(
                    "Invalid precision '{}' in to_type '{}'",
                    parts[0],
                    to_type_str
                )
            })?;
            entry.carry_precision = true;
            entry.default_precision = Some(precision);
            if parts.len() > 1 {
                let scale: u8 = parts[1].parse().map_err(|_| {
                    anyhow!("Invalid scale '{}' in to_type '{}'", parts[1], to_type_str)
                })?;
                entry.default_scale = Some(scale);
            }
        } else {
            return Err(anyhow!(
                "Type '{}' does not accept parameters, but got '{}'",
                mysql_type.as_str(),
                to_type_str
            ));
        }
    } else if mysql_type.accepts_length() {
        // No params given but type requires length — carry from source with a safe default
        entry.carry_length = true;
        entry.default_length = Some(255);
    } else if mysql_type.accepts_precision() {
        // No params given but type requires precision — carry from source with a safe default
        entry.carry_precision = true;
        entry.default_precision = Some(10);
        entry.default_scale = Some(2);
    }

    Ok(entry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::mssql_type::MssqlType;
    use crate::common::mysql_type::MySqlBaseType;

    #[test]
    fn test_parse_simple_override() {
        let toml: toml::Value = r#"
        [mappings]
        nvarchar = "varchar(500)"
        "#
        .parse()
        .unwrap();

        let overrides = UserOverrides::from_toml(toml).unwrap();
        assert_eq!(overrides.len(), 1);
        let (mssql_type, entry) = overrides.iter().next().unwrap();
        assert_eq!(*mssql_type, MssqlType::NVarchar);
        assert_eq!(entry.mysql_type, MySqlBaseType::Varchar);
        assert!(entry.carry_length);
        assert_eq!(entry.default_length, Some(500));
    }

    #[test]
    fn test_parse_decimal_override() {
        let toml: toml::Value = r#"
        [mappings]
        money = "decimal(19, 4)"
        "#
        .parse()
        .unwrap();

        let overrides = UserOverrides::from_toml(toml).unwrap();
        let (_, entry) = overrides.iter().next().unwrap();
        assert_eq!(entry.mysql_type, MySqlBaseType::Decimal);
        assert!(entry.carry_precision);
        assert_eq!(entry.default_precision, Some(19));
        assert_eq!(entry.default_scale, Some(4));
    }

    #[test]
    fn test_parse_no_params_override() {
        let toml: toml::Value = r#"
        [mappings]
        nvarchar = "longtext"
        "#
        .parse()
        .unwrap();

        let overrides = UserOverrides::from_toml(toml).unwrap();
        let (_, entry) = overrides.iter().next().unwrap();
        assert_eq!(entry.mysql_type, MySqlBaseType::LongText);
        assert!(!entry.carry_length);
        assert!(!entry.carry_precision);
        assert_eq!(entry.default_length, None);
    }

    #[test]
    fn test_parse_invalid_from_type() {
        let toml: toml::Value = r#"
        [mappings]
        varchat = "varchar(255)"
        "#
        .parse()
        .unwrap();

        let result = UserOverrides::from_toml(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("varchat"));
    }

    #[test]
    fn test_parse_invalid_to_type() {
        let toml: toml::Value = r#"
        [mappings]
        int = "spatial_nonsense"
        "#
        .parse()
        .unwrap();

        let result = UserOverrides::from_toml(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("spatial_nonsense"));
    }

    #[test]
    fn test_parse_length_exceeds_max() {
        let toml: toml::Value = r#"
        [mappings]
        varchar = "varchar(70000)"
        "#
        .parse()
        .unwrap();

        let result = UserOverrides::from_toml(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("65535"));
    }

    #[test]
    fn test_parse_length_on_non_length_type() {
        let toml: toml::Value = r#"
        [mappings]
        text = "longtext(500)"
        "#
        .parse()
        .unwrap();

        let result = UserOverrides::from_toml(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not accept"));
    }

    #[test]
    fn test_parse_empty_user_mappings() {
        let toml: toml::Value = r#"
        [mappings]
        "#
        .parse()
        .unwrap();

        let overrides = UserOverrides::from_toml(toml).unwrap();
        assert_eq!(overrides.len(), 0);
    }

    #[test]
    fn test_parse_varchar_no_params_carries_length() {
        let toml: toml::Value = r#"
        [mappings]
        nvarchar = "varchar"
        "#
        .parse()
        .unwrap();

        let overrides = UserOverrides::from_toml(toml).unwrap();
        let (_, entry) = overrides.iter().next().unwrap();
        assert_eq!(entry.mysql_type, MySqlBaseType::Varchar);
        assert!(entry.carry_length);
        assert_eq!(entry.default_length, Some(255));
    }

    #[test]
    fn test_parse_decimal_no_params_carries_precision() {
        let toml: toml::Value = r#"
        [mappings]
        money = "decimal"
        "#
        .parse()
        .unwrap();

        let overrides = UserOverrides::from_toml(toml).unwrap();
        let (_, entry) = overrides.iter().next().unwrap();
        assert_eq!(entry.mysql_type, MySqlBaseType::Decimal);
        assert!(entry.carry_precision);
        assert_eq!(entry.default_precision, Some(10));
        assert_eq!(entry.default_scale, Some(2));
    }

    #[test]
    fn test_parse_precision_only() {
        let toml: toml::Value = r#"
        [mappings]
        float = "float(53)"
        "#
        .parse()
        .unwrap();

        let overrides = UserOverrides::from_toml(toml).unwrap();
        let (_, entry) = overrides.iter().next().unwrap();
        assert_eq!(entry.default_precision, Some(53));
        assert_eq!(entry.default_scale, None);
    }
}
