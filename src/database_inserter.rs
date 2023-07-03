use crate::schema::ColumnSchema;

use sqlx::Error;
use sqlx::MySqlPool;
use std::error::Error as StdError;

pub struct DatabaseInserter {
    pool: MySqlPool,
}

impl DatabaseInserter {
    pub fn new(pool: MySqlPool) -> Self {
        DatabaseInserter { pool }
    }

    pub async fn create_table(
        &mut self,
        table_name: &str,
        schema: &[ColumnSchema],
    ) -> Result<(), Error> {
        println!("Creating table {}", table_name);
        let mut create_table_query = format!("CREATE TABLE `{}` (", table_name);

        for (i, column) in schema.iter().enumerate() {
            if i > 0 {
                create_table_query.push_str(", ");
            }

            let column_definition = match column.data_type.as_str().to_lowercase().as_str() {
                "varchar" | "nvarchar" => {
                    let column_length = column.character_maximum_length.unwrap_or(255);
                    format!("{} varchar({})", column.column_name, column_length)
                }
                "ntext" => {
                    format!("{} longtext", column.column_name)
                }
                "uniqueidentifier" => {
                    format!("{} CHAR(36)", column.column_name)
                }
                "decimal" | "money" => {
                    let decimal_precision = column.numeric_precision.unwrap_or(10);
                    let decimal_scale = column.numeric_scale.unwrap_or(2);
                    format!(
                        "{} decimal({}, {})",
                        column.column_name, decimal_precision, decimal_scale
                    )
                }
                _ => format!("{} {}", column.column_name, column.data_type),
            };

            create_table_query.push_str(&column_definition);
        }

        create_table_query.push_str(")");

        println!("\nQuery: {}\n", create_table_query);

        sqlx::query(&create_table_query).execute(&self.pool).await?;

        println!("[+] Table {} created successfully", table_name);

        Ok(())
    }

    pub async fn drop_table(&mut self, table_name: &str) -> Result<(), Box<dyn StdError>> {
        let table_exists = self.table_exists(table_name).await?;

        if !table_exists {
            return Ok(());
        }

        let drop_table_query = format!("DROP TABLE `{}`", table_name);

        sqlx::query(&drop_table_query).execute(&self.pool).await?;

        println!("[+] Table {} dropped successfully", table_name);

        Ok(())
    }

    async fn table_exists(&mut self, table_name: &str) -> Result<bool, Box<dyn StdError>> {
        let query = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '{}'",
            table_name
        );

        let count: i64 = sqlx::query_scalar(&query).fetch_one(&self.pool).await?;

        Ok(count > 0)
    }
}
