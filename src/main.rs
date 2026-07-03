#[macro_use]
extern crate log;

use std::io::Write;
use std::{fs, process, thread};

use anyhow::{Context, Result, anyhow};
use chrono::Local;
use clap::Parser;
use toml::Value;

use crate::args::Args;
use crate::config::Config;
use crate::connection::{DatabaseConnectionFactory, SqlxMySqlConnection, TiberiusConnection};
use crate::extract::extractor::DatabaseExtractor;
use crate::insert::inserter::DatabaseInserter;
use crate::mappings::UserOverrides;
use crate::migrate::migration_options::MigrationOptions;
use crate::migrate::migrator::DatabaseMigrator;
use crate::migrate::type_registry::TypeRegistry;

mod args;
mod common;
mod config;
mod connection;
mod extract;
mod insert;
mod mappings;
mod migrate;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let options = Args::parse();

    initialize_logger(options.verbose, options.quiet);

    if let Err(errors) = run(options).await {
        for (index, error) in errors.chain().enumerate() {
            error!("└> {} - {}", index, error);
        }
        process::exit(1);
    }
}

async fn run(options: Args) -> Result<()> {
    let config = resolve_config(&options)?;
    info!("Initializing connections...");

    let max_connections = options.parallelism as u32;
    let tiberius_connection = create_tiberius_connection(&config, max_connections).await?;
    let sqlx_connection = create_sqlx_connection(&config, max_connections).await?;

    let extractor = DatabaseExtractor::new(tiberius_connection.pool);
    let inserter = DatabaseInserter::new(sqlx_connection.pool);

    let user_overrides = load_user_overrides().context("Failed to load mappings file")?;
    let registry = TypeRegistry::with_defaults().with_user_overrides(&user_overrides);

    let migration_options = MigrationOptions {
        drop: options.drop,
        constraints: options.constraints,
        format_snake_case: options.format,
        max_concurrent_tasks: options.parallelism,
        max_packet_bytes: config.settings().max_packet_bytes,
        whitelisted_tables: config.settings().whitelisted_tables.clone(),
    };

    let migrator = DatabaseMigrator::new(extractor, inserter, registry, migration_options);

    migrator.run().await.with_context(|| "Migration failed")?;

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

fn initialize_logger(verbose: bool, quiet: bool) {
    let log_level = if quiet {
        log::LevelFilter::Warn
    } else if verbose {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };

    env_logger::Builder::new()
        .filter_level(log_level)
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

fn load_user_overrides() -> Result<UserOverrides> {
    let mappings_file = "mappings.toml";
    match fs::read_to_string(mappings_file) {
        Ok(content) => {
            let value = content.parse::<Value>()?;
            UserOverrides::from_toml(value)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            info!("No mappings.toml found, using built-in defaults");
            Ok(UserOverrides::empty())
        }
        Err(e) => Err(e.into()),
    }
}

/// CLI mode (--source/--target/--tables) uses arguments exclusively;
/// config.toml is not read. Without them, config.toml is required, so the
/// two sources are never mixed.
fn resolve_config(options: &Args) -> Result<Config> {
    let cli_mode = options.source.is_some() || options.target.is_some() || options.tables.is_some();

    if cli_mode {
        let mut missing = Vec::new();
        if options.source.is_none() {
            missing.push("--source");
        }
        if options.target.is_none() {
            missing.push("--target");
        }
        if options.tables.is_none() {
            missing.push("--tables");
        }
        if !missing.is_empty() {
            return Err(anyhow!(
                "CLI mode requires --source, --target and --tables; missing: {}. config.toml is not read when CLI connection arguments are used.",
                missing.join(", ")
            ));
        }

        info!("Using CLI connection arguments, config.toml is not read");
        return Config::from_cli(
            options.source.as_deref().expect("checked above"),
            options.target.as_deref().expect("checked above"),
            options.tables.as_deref().expect("checked above"),
            options.max_packet_bytes,
        );
    }

    let mut config = load_config().context("Failed to load config file")?;
    if let Some(max_packet_bytes) = options.max_packet_bytes {
        config.override_max_packet_bytes(max_packet_bytes)?;
    }
    Ok(config)
}

fn load_config() -> Result<Config> {
    let config_file = "config.toml";
    let content = fs::read_to_string(config_file)?;
    let value = content.parse::<Value>()?;
    let config = Config::from_toml(value)?;
    Ok(config)
}
