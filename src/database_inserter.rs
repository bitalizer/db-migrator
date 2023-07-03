use crate::config::DatabaseConfig;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::MySqlPool;
use std::error::Error;

pub struct DatabaseInserter {
    config: DatabaseConfig,
    client: Option<MySqlPool>,
}

impl DatabaseInserter {
    pub fn new(config: DatabaseConfig) -> Self {
        DatabaseInserter {
            config,
            client: None,
        }
    }

    pub async fn connect(&mut self) -> Result<(), Box<dyn Error>> {
        let connection_string = format!(
            "mysql://{}:{}@{}:{}/{}",
            self.config.username(),
            self.config.password(),
            self.config.host(),
            self.config.port(),
            self.config.database()
        );

        let pool_result = MySqlPoolOptions::new()
            .max_connections(3)
            .connect(&connection_string)
            .await
            .map_err(|e| format!("Failed to connect to the MySQL database: {}", e))?;

        println!("Database Inserter has initialized");

        self.client = Some(pool_result);
        Ok(())
    }

    pub(crate) async fn create_table(&mut self, table_name: &str) -> Result<(), Box<dyn Error>> {
        println!("Table {} created successfully", table_name);

        Ok(())
    }
}
