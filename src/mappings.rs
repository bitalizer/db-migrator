use anyhow::{anyhow, Result};
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
}
