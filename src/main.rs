use std::fs;
use toml::Value;

use crate::config::Config;
use crate::database_extractor::DatabaseExtractor;
use crate::database_inserter::DatabaseInserter;
use crate::database_migrator::DatabaseMigrator;

mod config;
mod database_extractor;
mod database_inserter;
mod database_migrator;
mod schema;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse config
    let config = load_config().expect("Failed to load config file");

    println!("Initializing connections...");

    // Initialize connections
    let mut extractor = DatabaseExtractor::new(config.mssql_database().clone());
    let mut inserter = DatabaseInserter::new(config.mysql_database().clone());

    //Connect to databases
    extractor.connect().await?;
    inserter.connect().await?;
    println!("Connections established");

    // Run database migration
    let mut migrator = DatabaseMigrator::new(extractor, inserter, config.settings().clone());
    migrator.run().await?;
    Ok(())
}

fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    let config_file = "config.toml";
    let content = fs::read_to_string(config_file)?;
    let value = content.parse::<Value>()?;
    let config = Config::from_toml(value)?;
    Ok(config)
}
