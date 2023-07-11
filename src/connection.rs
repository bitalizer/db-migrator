use crate::config::DatabaseConfig;
use anyhow::Result;
use async_trait::async_trait;
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions};
use tiberius::{AuthMethod, Config, EncryptionLevel};

pub struct TiberiusConnection {
    pub pool: Pool<ConnectionManager>,
}

pub struct SqlxMySqlConnection {
    pub pool: MySqlPool,
}

#[async_trait]
pub trait DatabaseConnection: Sized {
    async fn new(config: &DatabaseConfig) -> Result<Self>;
}

#[async_trait]
impl DatabaseConnection for TiberiusConnection {
    async fn new(config: &DatabaseConfig) -> Result<Self> {
        let mut tiberius_config = Config::new();
        tiberius_config.encryption(EncryptionLevel::NotSupported);
        tiberius_config.authentication(AuthMethod::sql_server(&config.username, &config.password));
        tiberius_config.database(&config.database);

        let mgr = ConnectionManager::new(tiberius_config);
        let pool = Pool::builder().max_size(4).build(mgr).await?;

        /*let tcp = TcpStream::connect(tiberius_config.get_addr())
            .await
            .context("Failed to connect to MSSQL server")?;
        let tcp_compat = tcp.compat_write();
        let client = Client::connect(tiberius_config, tcp_compat)
            .await
            .context("Failed to establish MSSQL connection")?;*/

        Ok(TiberiusConnection { pool })
    }
}

#[async_trait]
impl DatabaseConnection for SqlxMySqlConnection {
    async fn new(config: &DatabaseConfig) -> Result<Self> {
        let options = MySqlConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .username(&config.username)
            .password(&config.password)
            .database(&config.database);

        let pool = MySqlPoolOptions::new()
            .max_connections(4)
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

    pub async fn create_connection(&self) -> Result<C> {
        C::new(&self.config).await
    }
}
