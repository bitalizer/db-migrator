#[macro_use]
extern crate log;

use std::io::Write;
use std::{env, fs, thread};

use anyhow::{Context, Result};
use chrono::Local;
use env_logger::Env;
use structopt::StructOpt;
use toml::Value;

use crate::args::Args;
use crate::config::{Config, SettingsConfig};
use crate::connection::{DatabaseConnectionFactory, SqlxMySqlConnection, TiberiusConnection};
use crate::extract::extractor::DatabaseExtractor;
use crate::insert::inserter::DatabaseInserter;
use crate::mappings::Mappings;
use crate::migrate::migration_options::MigrationOptions;
use crate::migrate::migrator::DatabaseMigrator;

mod args;
mod common;
mod config;
mod connection;
mod extract;
mod insert;
mod mappings;
mod migrate;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    if let Err(errors) = init().await.with_context(|| "Initialization failed") {
        for (index, error) in errors.chain().enumerate() {
            error!("└> {} - {}", index, error);
        }
    }

    Ok(())
}

async fn init() -> Result<()> {
    let options = Args::from_args();

    initialize_logger(options.verbose, options.quiet);

    // Parse config
    let config = load_config().context("Failed to load config file")?;
    let mappings = load_mappings().context("Failed to load mappings file")?;

    debug!("Total mappings loaded: {}", mappings.len());
    info!("Initializing connections...");

    let max_connections = options.parallelism as u32;
    let tiberius_connection = create_tiberius_connection(&config, max_connections).await?;
    let sqlx_connection = create_sqlx_connection(&config, max_connections).await?;

    run_migration(
        tiberius_connection,
        sqlx_connection,
        mappings,
        config.settings().clone(),
        options,
    )
    .await?;

    Ok(())
}

async fn create_tiberius_connection(
    config: &Config,
    max_connections: u32,
) -> Result<TiberiusConnection> {
    let tiberius_factory =
        DatabaseConnectionFactory::<TiberiusConnection>::new(config.mssql_database().clone());
    let tiberius_connection = tiberius_factory.create_connection(max_connections).await?;
    Ok(tiberius_connection)
}

async fn create_sqlx_connection(
    config: &Config,
    max_connections: u32,
) -> Result<SqlxMySqlConnection> {
    let sqlx_factory =
        DatabaseConnectionFactory::<SqlxMySqlConnection>::new(config.mysql_database().clone());
    let sqlx_connection = sqlx_factory.create_connection(max_connections).await?;
    Ok(sqlx_connection)
}

async fn run_migration(
    tiberius_connection: TiberiusConnection,
    sqlx_connection: SqlxMySqlConnection,
    mappings: Mappings,
    settings: SettingsConfig,
    options: Args,
) -> Result<()> {
    let extractor = DatabaseExtractor::new(tiberius_connection.pool);
    let inserter = DatabaseInserter::new(sqlx_connection.pool);

    let migration_options = MigrationOptions {
        drop: options.drop,
        constraints: options.constraints,
        format_snake_case: options.format,
        max_concurrent_tasks: options.parallelism,
        max_packet_bytes: settings.max_packet_bytes,
        whitelisted_tables: settings.whitelisted_tables,
    };

    let mut migrator = DatabaseMigrator::new(extractor, inserter, mappings, migration_options);

    let migration_result = migrator.run().await.with_context(|| "Migration failed");

    if let Err(errors) = migration_result {
        for (index, error) in errors.chain().enumerate() {
            error!("└> {} - {}", index, error);
        }
    }

    Ok(())
}

fn initialize_logger(verbose: bool, quiet: bool) {
    // Set the `RUST_LOG` environment variable to control the logging level

    if quiet {
        env::set_var("RUST_LOG", "warn");
    } else {
        env::set_var("RUST_LOG", if verbose { "debug" } else { "info" });
    }

    // Initialize the logger with the desired format and additional configuration
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .filter_module("tiberius", log::LevelFilter::Error)
        .filter_module("sqlx", log::LevelFilter::Error)
        .format(|buf, record| {
            let timestamp = Local::now().format("%H:%M:%S");

            writeln!(
                buf,
                "{} {:<5} [{}] - {}",
                timestamp,
                record.level(),
                thread::current().name().unwrap_or("<unnamed>"),
                record.args()
            )
        })
        .init();
}

fn load_config() -> Result<Config> {
    let config_file = "config.toml";
    let content = fs::read_to_string(config_file)?;
    let value = content.parse::<Value>()?;
    let config = Config::from_toml(value)?;
    Ok(config)
}

fn load_mappings() -> Result<Mappings> {
    let mappings_file = "mappings.toml";
    let content = fs::read_to_string(mappings_file)?;
    let value = content.parse::<Value>()?;
    let mappings = Mappings::from_toml(value)?;
    Ok(mappings)
}
