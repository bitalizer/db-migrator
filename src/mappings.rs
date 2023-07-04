use std::collections::HashMap;

#[derive(Debug)]
pub struct Mappings {
    mappings: HashMap<String, Mapping>,
}

#[derive(Debug)]
pub struct Mapping {
    pub to_type: String,
    pub numeric_precision: Option<u32>,
    pub numeric_scale: Option<u32>,
    pub max_characters_length: Option<u32>,
}

impl Mappings {
    pub fn get(&self, name: &str) -> Option<&Mapping> {
        self.mappings.get(name)
    }

    pub fn len(&self) -> usize {
        self.mappings.len()
    }

    pub(crate) fn from_toml(value: toml::Value) -> Result<Mappings, Box<dyn std::error::Error>> {
        let mappings_table = value
            .get("mappings")
            .ok_or("Missing mappings table")?
            .as_array()
            .ok_or("Invalid mappings table format")?;

        let mut mappings = HashMap::new();

        for mapping_table in mappings_table {
            let mapping_table = mapping_table.as_table().ok_or("Invalid mapping format")?;
            let name = mapping_table
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or("Missing or invalid 'name' field")?
                .to_string();
            let to_type = mapping_table
                .get("to_type")
                .and_then(|v| v.as_str())
                .ok_or("Missing or invalid 'to_type' field")?
                .to_string();
            let numeric_precision = mapping_table
                .get("numeric_precision")
                .and_then(|v| v.as_integer())
                .map(|v| v as u32);
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
                numeric_precision,
                numeric_scale,
                max_characters_length,
            };

            mappings.insert(name, mapping);
        }

        Ok(Mappings { mappings })
    }
}
