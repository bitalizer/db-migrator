use anyhow::{anyhow, Result};
use bb8::{Pool, PooledConnection};
use bb8_tiberius::ConnectionManager;
use futures::stream::{BoxStream, StreamExt};
use futures::TryStreamExt;

use crate::common::schema::ColumnSchema;
use crate::extract::format::format_row_values;

#[derive(Clone)]
pub struct DatabaseExtractor {
    pub pool: Pool<ConnectionManager>,
}

impl DatabaseExtractor {
    pub fn new(pool: Pool<ConnectionManager>) -> Self {
        DatabaseExtractor { pool }
    }

    pub async fn fetch_tables(&mut self) -> Result<Vec<String>> {
        let mut conn = self.pool.get().await?;

        let rows = conn
            .simple_query(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'",
            )
            .await?
            .into_results()
            .await?;

        let tables = rows
            .iter()
            .flatten()
            .map(|row| {
                let table_name: Option<&str> = row.get(0);
                match table_name {
                    Some(name) => Ok(name.to_owned()),
                    None => Err(anyhow!("Failed to retrieve table name")),
                }
            })
            .collect::<Result<Vec<String>, _>>()?;

        Ok(tables)
    }

    pub async fn get_table_schema(&mut self, table: &str) -> Result<Vec<ColumnSchema>> {
        let mut conn = self.pool.get().await?;

        let query = format !(
            "SELECT 
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.IS_NULLABLE,
                (
                    SELECT CASE 
                        WHEN tc.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 'PRIMARY KEY'
                        WHEN tc.CONSTRAINT_TYPE = 'FOREIGN KEY' THEN 'FOREIGN KEY,' + rcf.TABLE_NAME + ',' + rcf.COLUMN_NAME   
                        WHEN tc.CONSTRAINT_TYPE = 'UNIQUE' THEN 'UNIQUE'
                        WHEN cc.CHECK_CLAUSE IS NOT NULL THEN 'CHECK (' + cc.CHECK_CLAUSE + ')'
                        WHEN c.COLUMN_DEFAULT IS NOT NULL THEN 'DEFAULT ' + c.COLUMN_DEFAULT
                        ELSE ''
                    END
                    FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu 
                    LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON ccu.CONSTRAINT_CATALOG = tc.CONSTRAINT_CATALOG AND ccu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                    LEFT JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc ON tc.CONSTRAINT_CATALOG = cc.CONSTRAINT_CATALOG AND tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
                    LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc ON tc.CONSTRAINT_CATALOG = rc.CONSTRAINT_CATALOG AND tc.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                    LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu_ref ON rc.UNIQUE_CONSTRAINT_CATALOG = ccu_ref.CONSTRAINT_CATALOG AND rc.UNIQUE_CONSTRAINT_SCHEMA = ccu_ref.CONSTRAINT_SCHEMA AND rc.UNIQUE_CONSTRAINT_NAME = ccu_ref.CONSTRAINT_NAME
                    LEFT JOIN INFORMATION_SCHEMA.COLUMNS rcf ON ccu_ref.TABLE_CATALOG = rcf.TABLE_CATALOG AND ccu_ref.TABLE_SCHEMA = rcf.TABLE_SCHEMA AND ccu_ref.TABLE_NAME = rcf.TABLE_NAME AND ccu_ref.COLUMN_NAME = rcf.COLUMN_NAME
                    WHERE ccu.TABLE_NAME = c.TABLE_NAME AND ccu.COLUMN_NAME = c.COLUMN_NAME
                ) AS CONSTRAINTS
            FROM 
                INFORMATION_SCHEMA.COLUMNS c       
            WHERE c.TABLE_NAME = '{}';",
            table
        );

        let rows = conn.simple_query(query).await?.into_first_result().await?;

        let schema = rows
            .into_iter()
            .map(|r| ColumnSchema::from_row(&r))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        Ok(schema)
    }
}

pub async fn open_row_stream<'a>(
    conn: &'a mut PooledConnection<'_, ConnectionManager>,
    table: &'a str,
) -> Result<BoxStream<'a, Result<Vec<String>, tiberius::error::Error>>> {
    let query = format!("SELECT * FROM [{}]", table);
    let stream = conn
        .simple_query(query)
        .await?
        .into_row_stream()
        .map_ok(format_row_values)
        .boxed();

    Ok(stream)
}
