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
    pub send_packet_size: usize,
    pub format_snake_case: bool,
    pub collation: String,
    pub whitelisted_tables: Vec<String>,
}

impl Config {
    pub(crate) fn from_toml(config: Value) -> Result<Self, Box<dyn std::error::Error>> {
        let mssql_database = parse_database_config(
            config
                .get("mssql_database")
                .ok_or("Missing or invalid MSSQL database settings")?
                .clone(),
        )?;
        let mysql_database = parse_database_config(
            config
                .get("mysql_database")
                .ok_or("Missing or invalid MySQL database settings")?
                .clone(),
        )?;
        let settings = parse_settings_config(
            config
                .get("settings")
                .ok_or("Missing or invalid settings")?
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

fn parse_database_config(config: Value) -> Result<DatabaseConfig, Box<dyn std::error::Error>> {
    let host = config
        .get("host")
        .and_then(|value| value.as_str())
        .ok_or("Missing or invalid host")?
        .to_string();
    let port = config
        .get("port")
        .and_then(|value| value.as_integer())
        .ok_or("Missing or invalid port")?
        .try_into()?;
    let username = config
        .get("username")
        .and_then(|value| value.as_str())
        .ok_or("Missing or invalid username")?
        .to_string();
    let password = config
        .get("password")
        .and_then(|value| value.as_str())
        .ok_or("Missing or invalid password")?
        .to_string();
    let database = config
        .get("database")
        .and_then(|value| value.as_str())
        .ok_or("Missing or invalid database")?
        .to_string();

    Ok(DatabaseConfig {
        host,
        port,
        username,
        password,
        database,
    })
}

fn parse_settings_config(config: Value) -> Result<SettingsConfig, Box<dyn std::error::Error>> {
    let max_send_packet_bytes = config
        .get("send_packet_mb")
        .and_then(|v| v.as_integer().map(|v| v as usize))
        .ok_or("Missing or invalid max send packet value")?
        * 1024
        * 1024;

    let format_snake_case = config
        .get("format_snake_case")
        .and_then(|value| value.as_bool())
        .unwrap_or(true);

    let collation = config
        .get("collation")
        .and_then(|value| value.as_str())
        .ok_or("Missing or invalid collation")?
        .to_string();

    let whitelisted_tables = config
        .get("whitelisted_tables")
        .and_then(|value| value.as_array())
        .ok_or("Missing or invalid whitelisted tables")?
        .iter()
        .filter_map(|value| value.as_str().map(|s| s.to_string()))
        .collect::<Vec<String>>();

    Ok(SettingsConfig {
        send_packet_size: max_send_packet_bytes,
        format_snake_case,
        collation,
        whitelisted_tables,
    })
}
