use anyhow::{Context, Result};
use async_trait::async_trait;
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions};
use sqlx::ConnectOptions;
use tiberius::{AuthMethod, Config, EncryptionLevel};

use crate::config::DatabaseConfig;

pub struct TiberiusConnection {
    pub pool: Pool<ConnectionManager>,
}

pub struct SqlxMySqlConnection {
    pub pool: MySqlPool,
}

#[async_trait]
pub trait DatabaseConnection: Sized {
    async fn new(config: &DatabaseConfig, max_connections: u32) -> Result<Self>;
}

#[async_trait]
impl DatabaseConnection for TiberiusConnection {
    async fn new(config: &DatabaseConfig, max_connections: u32) -> Result<Self> {
        let mut tiberius_config = Config::new();
        tiberius_config.encryption(EncryptionLevel::NotSupported);
        tiberius_config.authentication(AuthMethod::sql_server(&config.username, &config.password));
        tiberius_config.database(&config.database);

        let mgr = ConnectionManager::new(tiberius_config);
        let pool = Pool::builder()
            .max_size(max_connections)
            .build(mgr)
            .await
            .context("Failed to connect to MSSQL server")?;

        Ok(TiberiusConnection { pool })
    }
}

#[async_trait]
impl DatabaseConnection for SqlxMySqlConnection {
    async fn new(config: &DatabaseConfig, max_connections: u32) -> Result<Self> {
        let options = MySqlConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .username(&config.username)
            .password(&config.password)
            .database(&config.database)
            .disable_statement_logging()
            .clone();

        let pool = MySqlPoolOptions::new()
            .max_connections(max_connections)
            .connect_with(options)
            .await?;

        Ok(SqlxMySqlConnection { pool })
    }
}

pub struct DatabaseConnectionFactory<C: DatabaseConnection> {
    config: DatabaseConfig,
    connection_type: std::marker::PhantomData<C>,
}

impl<C: DatabaseConnection> DatabaseConnectionFactory<C> {
    pub fn new(config: DatabaseConfig) -> Self {
        DatabaseConnectionFactory {
            config,
            connection_type: std::marker::PhantomData,
        }
    }

    pub async fn create_connection(&self, max_connections: u32) -> Result<C> {
        C::new(&self.config, max_connections).await
    }
}
