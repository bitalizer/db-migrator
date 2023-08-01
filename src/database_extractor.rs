use anyhow::{anyhow, Result};
use bb8::{Pool, PooledConnection};
use bb8_tiberius::ConnectionManager;
use futures::stream::{BoxStream, StreamExt};
use futures::TryStreamExt;
use hex::encode;
use tiberius::{ColumnData, Row};

use crate::helpers::{
    format_date, format_datetime, format_datetime2, format_datetime_offset, format_number_value,
    format_numeric_value, format_small_datetime, format_string_value, format_time,
};
use crate::schema::ColumnSchema;

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
    offset_index: i64,
) -> Result<BoxStream<'a, Result<Vec<String>, tiberius::error::Error>>> {
    let query = format!(
        "SELECT * FROM [{}] ORDER BY (SELECT NULL) OFFSET {} ROWS",
        table, offset_index
    );
    let stream = conn
        .simple_query(query)
        .await?
        .into_row_stream()
        .map_ok(format_row_values)
        .boxed();

    Ok(stream)
}

fn format_row_values(row: Row) -> Vec<String> {
    row.into_iter().map(format_column_value).collect()
}

fn format_column_value(item: ColumnData) -> String {
    match item {
        ColumnData::Binary(Some(val)) => format!("'0x{}'", encode(val)),
        ColumnData::Binary(None) => "NULL".to_string(),
        ColumnData::Bit(val) => val.unwrap_or_default().to_string(),
        ColumnData::I16(val) => format_number_value(val),
        ColumnData::I32(val) => format_number_value(val),
        ColumnData::I64(val) => format_number_value(val),
        ColumnData::F32(val) => format_string_value(val),
        ColumnData::F64(val) => format_string_value(val),
        ColumnData::Guid(val) => format_string_value(val),
        ColumnData::Numeric(val) => format_numeric_value(val),
        ColumnData::String(val) => format_string_value(val),
        ColumnData::Time(ref val) => format_time(val),
        ColumnData::Date(ref val) => format_date(val),
        ColumnData::SmallDateTime(ref val) => format_small_datetime(val),
        ColumnData::DateTime(ref val) => format_datetime(val),
        ColumnData::DateTime2(ref val) => format_datetime2(val),
        ColumnData::DateTimeOffset(ref val) => format_datetime_offset(val),
        ColumnData::U8(val) => val.unwrap_or_default().to_string(),
        ColumnData::Xml(val) => val.unwrap().as_ref().to_string(),
    }
}
