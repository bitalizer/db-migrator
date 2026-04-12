use anyhow::{Result, anyhow};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Mappings {
    mappings: HashMap<String, Mapping>,
}

#[derive(Debug, Clone)]
pub struct Mapping {
    pub to_type: String,
    pub type_parameters: bool,
    pub numeric_precision: Option<u8>,
    pub numeric_scale: Option<u32>,
    pub max_characters_length: Option<u32>,
}

impl Mappings {
    #[cfg(test)]
    pub fn from_entries(entries: HashMap<String, Mapping>) -> Self {
        Mappings { mappings: entries }
    }

    pub fn get(&self, name: &str) -> Option<&Mapping> {
        self.mappings.get(name)
    }

    pub fn len(&self) -> usize {
        self.mappings.len()
    }

    pub(crate) fn from_toml(value: toml::Value) -> Result<Mappings> {
        let mappings_table = value
            .get("mappings")
            .ok_or(anyhow!("Missing mappings table"))?
            .as_array()
            .ok_or(anyhow!("Invalid mappings table format"))?;

        let mut mappings = HashMap::new();

        for mapping_table in mappings_table {
            let mapping_table = mapping_table
                .as_table()
                .ok_or(anyhow!("Invalid mapping format"))?;
            let from_type = mapping_table
                .get("from_type")
                .and_then(|v| v.as_str())
                .ok_or(anyhow!("Missing or invalid 'from_type' field"))?
                .to_string();
            let type_parameters = mapping_table
                .get("type_parameters")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            let to_type = mapping_table
                .get("to_type")
                .and_then(|v| v.as_str())
                .ok_or(anyhow!("Missing or invalid 'to_type' field"))?
                .to_string();
            let numeric_precision = mapping_table
                .get("numeric_precision")
                .and_then(|v| v.as_integer())
                .map(|v| v as u8);
            let numeric_scale = mapping_table
                .get("numeric_scale")
                .and_then(|v| v.as_integer())
                .map(|v| v as u32);
            let max_characters_length = mapping_table
                .get("max_characters_length")
                .and_then(|v| v.as_integer())
                .map(|v| v as u32);

            let mapping = Mapping {
                to_type,
                type_parameters,
                numeric_precision,
                numeric_scale,
                max_characters_length,
            };

            mappings.insert(from_type, mapping);
        }

        Ok(Mappings { mappings })
    }
}

use crate::common::mssql_type::MssqlType;
use crate::common::mysql_type::MySqlBaseType;
use crate::common::type_mapping_entry::TypeMappingEntry;

#[derive(Clone, Debug)]
pub struct UserOverrides {
    overrides: HashMap<MssqlType, TypeMappingEntry>,
}

impl UserOverrides {
    pub fn empty() -> Self {
        UserOverrides { overrides: HashMap::new() }
    }

    pub fn len(&self) -> usize {
        self.overrides.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&MssqlType, &TypeMappingEntry)> {
        self.overrides.iter()
    }

    pub(crate) fn from_toml(value: toml::Value) -> Result<UserOverrides> {
        let mappings_table = value
            .get("mappings")
            .ok_or(anyhow!("Missing mappings table"))?
            .as_array()
            .ok_or(anyhow!("Invalid mappings table format"))?;

        let mut overrides = HashMap::new();

        for mapping_table in mappings_table {
            let mapping_table = mapping_table
                .as_table()
                .ok_or(anyhow!("Invalid mapping format"))?;

            let from_type_str = mapping_table
                .get("from_type")
                .and_then(|v| v.as_str())
                .ok_or(anyhow!("Missing or invalid 'from_type' field"))?;

            let mssql_type = MssqlType::from_str(from_type_str).ok_or_else(|| {
                anyhow!(
                    "Unknown MSSQL type '{}' in from_type. Valid types: bit, tinyint, smallint, int, bigint, decimal, numeric, money, smallmoney, float, real, char, nchar, varchar, nvarchar, text, ntext, binary, varbinary, image, date, datetime, datetime2, smalldatetime, datetimeoffset, time, uniqueidentifier, timestamp, xml",
                    from_type_str
                )
            })?;

            let to_type_str = mapping_table
                .get("to_type")
                .and_then(|v| v.as_str())
                .ok_or(anyhow!("Missing or invalid 'to_type' field"))?
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
            let length: u32 = params.trim().parse().map_err(|_| {
                anyhow!("Invalid length '{}' in to_type '{}'", params, to_type_str)
            })?;
            let max = mysql_type.max_length().unwrap();
            if length > max {
                return Err(anyhow!(
                    "Length {} exceeds maximum {} for type '{}'. Use longtext/longblob for unlimited.",
                    length, max, mysql_type.as_str()
                ));
            }
            entry.carry_length = true;
            entry.default_length = Some(length);
        } else if mysql_type.accepts_precision() {
            let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();
            let precision: u8 = parts[0].parse().map_err(|_| {
                anyhow!("Invalid precision '{}' in to_type '{}'", parts[0], to_type_str)
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
                mysql_type.as_str(), to_type_str
            ));
        }
    }

    Ok(entry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_mappings() {
        let toml: toml::Value = r#"
        [[mappings]]
        from_type = "int"
        to_type = "int"
        type_parameters = true
        numeric_precision = 10

        [[mappings]]
        from_type = "varchar"
        to_type = "varchar"
        type_parameters = true
        max_characters_length = 255
        "#
        .parse()
        .unwrap();

        let mappings = Mappings::from_toml(toml).unwrap();
        assert_eq!(mappings.len(), 2);

        let int_mapping = mappings.get("int").unwrap();
        assert_eq!(int_mapping.to_type, "int");
        assert!(int_mapping.type_parameters);
        assert_eq!(int_mapping.numeric_precision, Some(10));

        let varchar_mapping = mappings.get("varchar").unwrap();
        assert_eq!(varchar_mapping.to_type, "varchar");
        assert_eq!(varchar_mapping.max_characters_length, Some(255));
    }

    #[test]
    fn test_missing_mappings_table() {
        let toml: toml::Value = r#"
        [other]
        key = "value"
        "#
        .parse()
        .unwrap();

        let result = Mappings::from_toml(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("mappings"));
    }

    #[test]
    fn test_missing_from_type() {
        let toml: toml::Value = r#"
        [[mappings]]
        to_type = "int"
        "#
        .parse()
        .unwrap();

        let result = Mappings::from_toml(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("from_type"));
    }

    #[test]
    fn test_missing_to_type() {
        let toml: toml::Value = r#"
        [[mappings]]
        from_type = "int"
        "#
        .parse()
        .unwrap();

        let result = Mappings::from_toml(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("to_type"));
    }

    #[test]
    fn test_type_parameters_defaults_to_false() {
        let toml: toml::Value = r#"
        [[mappings]]
        from_type = "text"
        to_type = "text"
        "#
        .parse()
        .unwrap();

        let mappings = Mappings::from_toml(toml).unwrap();
        let mapping = mappings.get("text").unwrap();
        assert!(!mapping.type_parameters);
    }

    #[test]
    fn test_optional_fields_are_none() {
        let toml: toml::Value = r#"
        [[mappings]]
        from_type = "text"
        to_type = "text"
        "#
        .parse()
        .unwrap();

        let mappings = Mappings::from_toml(toml).unwrap();
        let mapping = mappings.get("text").unwrap();
        assert_eq!(mapping.numeric_precision, None);
        assert_eq!(mapping.numeric_scale, None);
        assert_eq!(mapping.max_characters_length, None);
    }

    #[test]
    fn test_empty_mappings_array() {
        let toml: toml::Value = r#"
        mappings = []
        "#
        .parse()
        .unwrap();

        let mappings = Mappings::from_toml(toml).unwrap();
        assert_eq!(mappings.len(), 0);
    }

    #[test]
    fn test_get_nonexistent_mapping() {
        let toml: toml::Value = r#"
        [[mappings]]
        from_type = "int"
        to_type = "int"
        "#
        .parse()
        .unwrap();

        let mappings = Mappings::from_toml(toml).unwrap();
        assert!(mappings.get("nonexistent").is_none());
    }

    #[test]
    fn test_from_entries() {
        let mut entries = HashMap::new();
        entries.insert(
            "int".to_string(),
            Mapping {
                to_type: "int".to_string(),
                type_parameters: false,
                numeric_precision: None,
                numeric_scale: None,
                max_characters_length: None,
            },
        );
        let mappings = Mappings::from_entries(entries);
        assert_eq!(mappings.len(), 1);
        assert!(mappings.get("int").is_some());
    }

    // --- UserOverrides tests ---
    use crate::common::mssql_type::MssqlType;
    use crate::common::mysql_type::MySqlBaseType;

    #[test]
    fn test_parse_simple_override() {
        let toml: toml::Value = r#"
        [[mappings]]
        from_type = "nvarchar"
        to_type = "varchar(500)"
        "#.parse().unwrap();

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
        [[mappings]]
        from_type = "money"
        to_type = "decimal(19, 4)"
        "#.parse().unwrap();

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
        [[mappings]]
        from_type = "nvarchar"
        to_type = "longtext"
        "#.parse().unwrap();

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
        [[mappings]]
        from_type = "varchat"
        to_type = "varchar(255)"
        "#.parse().unwrap();

        let result = UserOverrides::from_toml(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("varchat"));
    }

    #[test]
    fn test_parse_invalid_to_type() {
        let toml: toml::Value = r#"
        [[mappings]]
        from_type = "int"
        to_type = "spatial_nonsense"
        "#.parse().unwrap();

        let result = UserOverrides::from_toml(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("spatial_nonsense"));
    }

    #[test]
    fn test_parse_length_exceeds_max() {
        let toml: toml::Value = r#"
        [[mappings]]
        from_type = "varchar"
        to_type = "varchar(70000)"
        "#.parse().unwrap();

        let result = UserOverrides::from_toml(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("65535"));
    }

    #[test]
    fn test_parse_length_on_non_length_type() {
        let toml: toml::Value = r#"
        [[mappings]]
        from_type = "text"
        to_type = "longtext(500)"
        "#.parse().unwrap();

        let result = UserOverrides::from_toml(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not accept"));
    }

    #[test]
    fn test_parse_empty_user_mappings() {
        let toml: toml::Value = r#"
        mappings = []
        "#.parse().unwrap();

        let overrides = UserOverrides::from_toml(toml).unwrap();
        assert_eq!(overrides.len(), 0);
    }

    #[test]
    fn test_parse_precision_only() {
        let toml: toml::Value = r#"
        [[mappings]]
        from_type = "float"
        to_type = "float(53)"
        "#.parse().unwrap();

        let overrides = UserOverrides::from_toml(toml).unwrap();
        let (_, entry) = overrides.iter().next().unwrap();
        assert_eq!(entry.default_precision, Some(53));
        assert_eq!(entry.default_scale, None);
    }
}
