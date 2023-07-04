use std::fs;
use toml::Value;

use crate::config::Config;
use crate::connection::{DatabaseConnectionFactory, SqlxMySqlConnection, TiberiusConnection};
use crate::database_extractor::DatabaseExtractor;
use crate::database_inserter::DatabaseInserter;
use crate::database_migrator::DatabaseMigrator;
use crate::mappings::Mappings;

mod config;
mod connection;
mod database_extractor;
mod database_inserter;
mod database_migrator;
mod mappings;
mod schema;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse config
    let config = load_config().expect("Failed to load config file");

    let mappings = load_mappings().expect("Failed to load mappings file");

    println!("Total mappings loaded: {}", mappings.len());

    println!("Initializing connections...");

    // Create an instance of `DatabaseConnectionFactory` for `TiberiusConnection`
    let tiberius_factory =
        DatabaseConnectionFactory::<TiberiusConnection>::new(config.mssql_database().clone());

    // Create an instance of `DatabaseConnectionFactory` for `SqlxMySqlConnection`
    let sqlx_factory =
        DatabaseConnectionFactory::<SqlxMySqlConnection>::new(config.mysql_database().clone());

    // Create the Tiberius connection using the factory
    let tiberius_connection = tiberius_factory
        .create_connection()
        .await
        .expect("Failed to create MSSQL connection");

    // Create the SQLx MySQL connection using the factory
    let sqlx_connection = sqlx_factory
        .create_connection()
        .await
        .expect("Failed to create MySQL connection");

    // Initialize connections
    let extractor = DatabaseExtractor::new(tiberius_connection.client);
    let inserter = DatabaseInserter::new(sqlx_connection.pool, mappings);

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

fn load_mappings() -> Result<Mappings, Box<dyn std::error::Error>> {
    let mappings_file = "mappings.toml";
    let content = fs::read_to_string(mappings_file)?;
    let value = content.parse::<Value>()?;
    let mappings = Mappings::from_toml(value)?;
    Ok(mappings)
}
