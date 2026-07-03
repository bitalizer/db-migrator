use anyhow::{Result, anyhow};
use toml::Value;

#[derive(Debug)]
pub(crate) struct Config {
    mssql_database: DatabaseConfig,
    mysql_database: DatabaseConfig,
    settings: SettingsConfig,
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
}

#[derive(Debug, Clone)]
pub struct SettingsConfig {
    pub max_packet_bytes: usize,
    #[allow(dead_code)]
    pub collation: String,
    pub whitelisted_tables: Vec<String>,
}

impl Config {
    pub(crate) fn from_toml(config: Value) -> Result<Self> {
        if let Some(table) = config.as_table() {
            for key in table.keys() {
                if !["mssql_database", "mysql_database", "settings"].contains(&key.as_str()) {
                    return Err(anyhow!(
                        "Unknown section '{}' in config.toml. Valid sections: mssql_database, mysql_database, settings",
                        key
                    ));
                }
            }
        }

        let mssql_database = parse_database_config(
            "mssql_database",
            config
                .get("mssql_database")
                .ok_or(anyhow!("Missing or invalid MSSQL database settings"))?
                .clone(),
            1433,
        )?;
        let mysql_database = parse_database_config(
            "mysql_database",
            config
                .get("mysql_database")
                .ok_or(anyhow!("Missing or invalid MySQL database settings"))?
                .clone(),
            3306,
        )?;
        let settings = parse_settings_config(
            config
                .get("settings")
                .ok_or(anyhow!("Missing or invalid settings"))?
                .clone(),
        )?;

        Ok(Config {
            mssql_database,
            mysql_database,
            settings,
        })
    }

    pub fn mssql_database(&self) -> &DatabaseConfig {
        &self.mssql_database
    }

    pub fn mysql_database(&self) -> &DatabaseConfig {
        &self.mysql_database
    }

    pub fn settings(&self) -> &SettingsConfig {
        &self.settings
    }
}

fn reject_unknown_keys(section: &str, config: &Value, valid: &[&str]) -> Result<()> {
    if let Some(table) = config.as_table() {
        for key in table.keys() {
            if !valid.contains(&key.as_str()) {
                return Err(anyhow!(
                    "Unknown key '{}' in [{}]. Valid keys: {}",
                    key,
                    section,
                    valid.join(", ")
                ));
            }
        }
    }
    Ok(())
}

fn parse_database_config(
    section: &str,
    config: Value,
    default_port: u16,
) -> Result<DatabaseConfig> {
    reject_unknown_keys(
        section,
        &config,
        &["host", "port", "username", "password", "database"],
    )?;

    // host and port are optional: missing values fall back to defaults, but
    // present values of the wrong type are still rejected.
    let host = match config.get("host") {
        None => "localhost".to_string(),
        Some(value) => value
            .as_str()
            .ok_or_else(|| anyhow!("Invalid host"))?
            .to_string(),
    };

    let port = match config.get("port") {
        None => default_port,
        Some(value) => value
            .as_integer()
            .ok_or_else(|| anyhow!("Invalid port"))?
            .try_into()?,
    };

    let username = config
        .get("username")
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("Missing or invalid username"))?
        .to_string();

    let password = config
        .get("password")
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("Missing or invalid password"))?
        .to_string();

    let database = config
        .get("database")
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("Missing or invalid database"))?
        .to_string();

    Ok(DatabaseConfig {
        host,
        port,
        username,
        password,
        database,
    })
}

fn parse_settings_config(config: Value) -> Result<SettingsConfig> {
    reject_unknown_keys(
        "settings",
        &config,
        &["max_packet_bytes", "collation", "whitelisted_tables"],
    )?;

    let max_packet_bytes = config
        .get("max_packet_bytes")
        .and_then(|v| v.as_integer())
        .ok_or_else(|| anyhow!("Missing or invalid max send packet value"))?;
    // A plain `as usize` cast would wrap negatives into huge values and
    // disable batch flushing entirely.
    let max_packet_bytes = usize::try_from(max_packet_bytes)
        .ok()
        .filter(|v| *v > 0)
        .ok_or_else(|| anyhow!("max_packet_bytes must be a positive integer"))?;

    let collation = config
        .get("collation")
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("Missing or invalid collation"))?
        .to_string();

    let whitelisted_tables = config
        .get("whitelisted_tables")
        .and_then(|value| value.as_array())
        .ok_or_else(|| anyhow!("Missing or invalid whitelisted tables"))?
        .iter()
        .map(|value| {
            value.as_str().map(|s| s.to_string()).ok_or_else(|| {
                anyhow!(
                    "Invalid whitelisted_tables entry '{}': table names must be strings, \
                     quote numeric names like \"42\"",
                    value
                )
            })
        })
        .collect::<Result<Vec<String>>>()?;

    Ok(SettingsConfig {
        max_packet_bytes,
        collation,
        whitelisted_tables,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config_toml() -> Value {
        r#"
        [mssql_database]
        host = "localhost"
        port = 1433
        username = "sa"
        password = "password123"
        database = "source_db"

        [mysql_database]
        host = "localhost"
        port = 3306
        username = "root"
        password = "password123"
        database = "target_db"

        [settings]
        max_packet_bytes = 1048576
        collation = "Latin1_General_CI_AS"
        whitelisted_tables = ["users", "orders"]
        "#
        .parse::<Value>()
        .unwrap()
    }

    #[test]
    fn test_valid_config() {
        let config = Config::from_toml(valid_config_toml()).unwrap();
        assert_eq!(config.mssql_database().host, "localhost");
        assert_eq!(config.mssql_database().port, 1433);
        assert_eq!(config.mssql_database().username, "sa");
        assert_eq!(config.mssql_database().database, "source_db");
        assert_eq!(config.mysql_database().host, "localhost");
        assert_eq!(config.mysql_database().port, 3306);
        assert_eq!(config.mysql_database().database, "target_db");
        assert_eq!(config.settings().max_packet_bytes, 1048576);
        assert_eq!(config.settings().collation, "Latin1_General_CI_AS");
        assert_eq!(
            config.settings().whitelisted_tables,
            vec!["users", "orders"]
        );
    }

    #[test]
    fn test_missing_mssql_section() {
        let toml: Value = r#"
        [mysql_database]
        host = "localhost"
        port = 3306
        username = "root"
        password = "pass"
        database = "db"

        [settings]
        max_packet_bytes = 1048576
        collation = "Latin1_General_CI_AS"
        whitelisted_tables = []
        "#
        .parse()
        .unwrap();

        let result = Config::from_toml(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("MSSQL"));
    }

    #[test]
    fn test_missing_mysql_section() {
        let toml: Value = r#"
        [mssql_database]
        host = "localhost"
        port = 1433
        username = "sa"
        password = "pass"
        database = "db"

        [settings]
        max_packet_bytes = 1048576
        collation = "Latin1_General_CI_AS"
        whitelisted_tables = []
        "#
        .parse()
        .unwrap();

        let result = Config::from_toml(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("MySQL"));
    }

    #[test]
    fn test_missing_settings_section() {
        let toml: Value = r#"
        [mssql_database]
        host = "localhost"
        port = 1433
        username = "sa"
        password = "pass"
        database = "db"

        [mysql_database]
        host = "localhost"
        port = 3306
        username = "root"
        password = "pass"
        database = "db"
        "#
        .parse()
        .unwrap();

        let result = Config::from_toml(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("settings"));
    }

    #[test]
    fn test_missing_host_defaults_to_localhost() {
        let toml: Value = r#"
        [mssql_database]
        port = 1433
        username = "sa"
        password = "pass"
        database = "db"
        "#
        .parse()
        .unwrap();

        let config = parse_database_config(
            "mssql_database",
            toml.get("mssql_database").unwrap().clone(),
            1433,
        )
        .unwrap();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 1433);
    }

    #[test]
    fn test_missing_port_uses_engine_default() {
        let toml: Value = r#"
        [db]
        host = "10.0.0.5"
        username = "sa"
        password = "pass"
        database = "db"
        "#
        .parse()
        .unwrap();

        let config = parse_database_config("db", toml.get("db").unwrap().clone(), 1433).unwrap();
        assert_eq!(config.host, "10.0.0.5");
        assert_eq!(config.port, 1433);
    }

    #[test]
    fn test_omitted_host_and_port_in_both_sections() {
        let config = Config::from_toml(
            r#"
            [mssql_database]
            username = "sa"
            password = "pass"
            database = "db"

            [mysql_database]
            username = "root"
            password = "pass"
            database = "db"

            [settings]
            max_packet_bytes = 1048576
            collation = "Latin1_General_CI_AS"
            whitelisted_tables = []
            "#
            .parse()
            .unwrap(),
        )
        .unwrap();

        assert_eq!(config.mssql_database().host, "localhost");
        assert_eq!(config.mssql_database().port, 1433);
        assert_eq!(config.mysql_database().host, "localhost");
        assert_eq!(config.mysql_database().port, 3306);
    }

    #[test]
    fn test_invalid_host_still_errors() {
        // Present but wrong type must error, not silently default
        let toml: Value = r#"
        [db]
        host = 123
        username = "sa"
        password = "pass"
        database = "db"
        "#
        .parse()
        .unwrap();

        let result = parse_database_config("db", toml.get("db").unwrap().clone(), 1433);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("host"));
    }

    #[test]
    fn test_invalid_port_still_errors() {
        let toml: Value = r#"
        [db]
        host = "localhost"
        port = "not-a-number"
        username = "sa"
        password = "pass"
        database = "db"
        "#
        .parse()
        .unwrap();

        let result = parse_database_config("db", toml.get("db").unwrap().clone(), 1433);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("port"));
    }

    #[test]
    fn test_out_of_range_port_errors() {
        let toml: Value = r#"
        [db]
        host = "localhost"
        port = 70000
        username = "sa"
        password = "pass"
        database = "db"
        "#
        .parse()
        .unwrap();

        let result = parse_database_config("db", toml.get("db").unwrap().clone(), 1433);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_max_packet_bytes() {
        let toml: Value = r#"
        [settings]
        collation = "Latin1_General_CI_AS"
        whitelisted_tables = []
        "#
        .parse()
        .unwrap();

        let result = parse_settings_config(toml.get("settings").unwrap().clone());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max send packet"));
    }

    #[test]
    fn test_negative_max_packet_bytes_errors() {
        let toml: Value = r#"
        [settings]
        max_packet_bytes = -1
        collation = "Latin1_General_CI_AS"
        whitelisted_tables = []
        "#
        .parse()
        .unwrap();

        let result = parse_settings_config(toml.get("settings").unwrap().clone());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("positive"));
    }

    #[test]
    fn test_zero_max_packet_bytes_errors() {
        let toml: Value = r#"
        [settings]
        max_packet_bytes = 0
        collation = "Latin1_General_CI_AS"
        whitelisted_tables = []
        "#
        .parse()
        .unwrap();

        let result = parse_settings_config(toml.get("settings").unwrap().clone());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("positive"));
    }

    #[test]
    fn test_unknown_key_in_database_section_errors() {
        let toml: Value = r#"
        [mssql_database]
        host = "localhost"
        prot = 1433
        username = "sa"
        password = "pass"
        database = "db"
        "#
        .parse()
        .unwrap();

        let result = parse_database_config(
            "mssql_database",
            toml.get("mssql_database").unwrap().clone(),
            1433,
        );
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("prot"));
        assert!(msg.contains("mssql_database"));
    }

    #[test]
    fn test_unknown_key_in_settings_errors() {
        let toml: Value = r#"
        [settings]
        max_packet_bytes = 1048576
        collation = "Latin1_General_CI_AS"
        whitelisted_tables = []
        max_paket_bytes = 99
        "#
        .parse()
        .unwrap();

        let result = parse_settings_config(toml.get("settings").unwrap().clone());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max_paket_bytes"));
    }

    #[test]
    fn test_unknown_top_level_section_errors() {
        let toml: Value = r#"
        [mssql_database]
        username = "sa"
        password = "pass"
        database = "db"

        [mysql_database]
        username = "root"
        password = "pass"
        database = "db"

        [settings]
        max_packet_bytes = 1048576
        collation = "Latin1_General_CI_AS"
        whitelisted_tables = []

        [setings]
        max_packet_bytes = 1
        "#
        .parse()
        .unwrap();

        let result = Config::from_toml(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("setings"));
    }

    #[test]
    fn test_non_string_whitelist_entry_errors() {
        let toml: Value = r#"
        [settings]
        max_packet_bytes = 1048576
        collation = "Latin1_General_CI_AS"
        whitelisted_tables = ["users", 42]
        "#
        .parse()
        .unwrap();

        let result = parse_settings_config(toml.get("settings").unwrap().clone());
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("42"));
        assert!(msg.contains("string"));
    }

    #[test]
    fn test_empty_whitelisted_tables() {
        let config = Config::from_toml(
            r#"
            [mssql_database]
            host = "localhost"
            port = 1433
            username = "sa"
            password = "pass"
            database = "db"

            [mysql_database]
            host = "localhost"
            port = 3306
            username = "root"
            password = "pass"
            database = "db"

            [settings]
            max_packet_bytes = 1048576
            collation = "Latin1_General_CI_AS"
            whitelisted_tables = []
            "#
            .parse()
            .unwrap(),
        )
        .unwrap();

        assert!(config.settings().whitelisted_tables.is_empty());
    }
}
