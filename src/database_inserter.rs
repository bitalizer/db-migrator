use crate::schema::ColumnSchema;

use crate::mappings::Mappings;
use sqlx::{Executor, MySqlPool};
use std::error::Error;

pub struct DatabaseInserter {
    pool: MySqlPool,
    mappings: Mappings,
}

impl DatabaseInserter {
    pub fn new(pool: MySqlPool, mappings: Mappings) -> Self {
        DatabaseInserter { pool, mappings }
    }

    pub async fn create_table(
        &mut self,
        table_name: &str,
        schema: &[ColumnSchema],
    ) -> Result<(), Box<dyn Error>> {
        println!("Creating table {}", table_name);

        let create_table_query = Self::build_create_table_query(table_name, schema)?;

        println!("\nQuery: {}\n", create_table_query);

        sqlx::query(&create_table_query).execute(&self.pool).await?;

        println!("[+] Table {} created successfully", table_name);

        Ok(())
    }

    pub async fn execute_transactional_queries(
        &mut self,
        queries: &[String],
    ) -> Result<(), Box<dyn Error>> {
        println!("Executing {} queries", queries.len());

        let mut transaction = self.pool.begin().await?;

        for query in queries {
            transaction.execute(query.as_str()).await?;
        }

        transaction.commit().await?;

        Ok(())
    }

    fn build_create_table_query(
        table_name: &str,
        schema: &[ColumnSchema],
    ) -> Result<String, Box<dyn Error>> {
        let columns: Vec<String> = schema
            .iter()
            .enumerate()
            .map(|(i, column)| {
                let column_definition = match column.data_type.as_str().to_lowercase().as_str() {
                    "bit" => {
                        format!("{} tinyint(1)", column.column_name)
                    }
                    "tinyint" => {
                        let numeric_precision = column.numeric_precision.unwrap_or(3);
                        format!("{} tinyint({})", column.column_name, numeric_precision)
                    }
                    "mediumint" => {
                        let numeric_precision = column.numeric_precision.unwrap_or(5);

                        format!("{} int({})", column.column_name, numeric_precision)
                    }
                    "int" => {
                        let numeric_precision = column.numeric_precision.unwrap_or(10);

                        format!("{} int({})", column.column_name, numeric_precision)
                    }
                    "bigint" => {
                        let numeric_precision = column.numeric_precision.unwrap_or(19);
                        format!("{} bigint({})", column.column_name, numeric_precision)
                    }
                    "nchar" => {
                        let column_length = column.character_maximum_length.unwrap_or(1);
                        format!("{} char({})", column.column_name, column_length)
                    }
                    "varchar" => {
                        let column_length = column.character_maximum_length.unwrap_or(255);

                        if column_length > 65535 || column_length == -1 {
                            format!("{} longtext", column.column_name)
                        } else {
                            format!("{} varchar({})", column.column_name, column_length)
                        }
                    }
                    "nvarchar" => {
                        let column_length = column.character_maximum_length.unwrap_or(255);

                        if column_length > 65535 || column_length == -1 {
                            format!("{} longtext", column.column_name)
                        } else {
                            format!("{} varchar({})", column.column_name, column_length)
                        }
                    }
                    "text" => {
                        format!("{} text", column.column_name)
                    }
                    "ntext" => {
                        format!("{} longtext", column.column_name)
                    }
                    "uniqueidentifier" => {
                        format!("{} CHAR(36)", column.column_name)
                    }
                    "decimal" => {
                        let decimal_precision = column.numeric_precision.unwrap_or(10);
                        let decimal_scale = column.numeric_scale.unwrap_or(2);
                        format!(
                            "{} decimal({}, {})",
                            column.column_name, decimal_precision, decimal_scale
                        )
                    }
                    "numeric" => {
                        let decimal_precision = column.numeric_precision.unwrap_or(18);
                        let decimal_scale = column.numeric_scale.unwrap_or(0);
                        format!(
                            "{} decimal({}, {})",
                            column.column_name, decimal_precision, decimal_scale
                        )
                    }
                    "smallmoney" => {
                        let decimal_precision = column.numeric_precision.unwrap_or(10);
                        let decimal_scale = column.numeric_scale.unwrap_or(2);
                        format!(
                            "{} decimal({}, {})",
                            column.column_name, decimal_precision, decimal_scale
                        )
                    }
                    "money" => {
                        let decimal_precision = column.numeric_precision.unwrap_or(19);
                        let decimal_scale = column.numeric_scale.unwrap_or(4);
                        format!(
                            "{} decimal({}, {})",
                            column.column_name, decimal_precision, decimal_scale
                        )
                    }
                    "datetime" | "datetime2" | "timestamp" | "date" | "datetimeoffset" => {
                        format!("{} datetime", column.column_name)
                    }
                    "binary" => {
                        format!("{} binary", column.column_name)
                    }
                    _ => {
                        eprintln!("Unsupported data type: {}", column.data_type);
                        format!("{} {}", column.column_name, column.data_type)
                    } //_ => format!("{} {}", column.column_name, column.data_type),
                };

                if i > 0 {
                    format!(", {}", column_definition)
                } else {
                    column_definition
                }
            })
            .collect();

        let create_table_query = format!("CREATE TABLE `{}` ({})", table_name, columns.join(""));

        Ok(create_table_query)
    }

    pub async fn drop_table(&mut self, table_name: &str) -> Result<(), Box<dyn Error>> {
        let table_exists = self.table_exists(table_name).await?;

        if !table_exists {
            return Ok(());
        }

        let drop_table_query = format!("DROP TABLE `{}`", table_name);

        sqlx::query(&drop_table_query).execute(&self.pool).await?;

        println!("[+] Table {} dropped successfully", table_name);

        Ok(())
    }

    async fn table_exists(&mut self, table_name: &str) -> Result<bool, Box<dyn Error>> {
        let query = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '{}'",
            table_name
        );

        let count: i64 = sqlx::query_scalar(&query).fetch_one(&self.pool).await?;

        Ok(count > 0)
    }
}
