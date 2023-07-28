use crate::schema::ColumnSchema;
use anyhow::{anyhow, Result};
use bb8::{Pool, PooledConnection};
use bb8_tiberius::ConnectionManager;
use chrono::DateTime as ChronosDateTime;
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures::stream::{BoxStream, StreamExt};
use futures::TryStreamExt;
use hex::encode;

use tiberius::numeric::Numeric;
use tiberius::time::{Date, DateTime, DateTime2, DateTimeOffset, SmallDateTime, Time};
use tiberius::{ColumnData, Row};

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

pub async fn fetch_table_data<'a>(
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

fn format_numeric_value(value: Option<Numeric>) -> String {
    match value {
        Some(numeric) => {
            let int_part = numeric.int_part();
            let dec_part = numeric.dec_part().abs();
            let scale = numeric.scale() as usize;

            let formatted_value = format!("{}.{:0<scale$}", int_part, dec_part, scale = scale);

            format!("'{}'", formatted_value)
        }
        None => "NULL".to_string(),
    }
}

fn format_string_value<T: ToString>(value: Option<T>) -> String {
    value
        .map(|v| format!("'{}'", v.to_string().replace('\'', "''")))
        .unwrap_or_else(|| "NULL".to_string())
}

fn format_number_value<T>(value: Option<T>) -> String
where
    T: std::fmt::Display,
{
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

fn format_time(val: &Option<Time>) -> String {
    val.map(|time| {
        let ns = time.increments() as i64 * 10i64.pow(9 - time.scale() as u32);
        let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::nanoseconds(ns);
        format!("{}", time.format("'%H:%M:%S'"))
    })
    .unwrap_or_else(|| "NULL".to_string())
}

fn format_date(val: &Option<Date>) -> String {
    val.map(|dt| {
        let datetime = from_days(dt.days() as i64, 1);
        datetime.format("'%Y-%m-%d'").to_string()
    })
    .unwrap_or_else(|| "NULL".to_string())
}

fn format_datetime(val: &Option<DateTime>) -> String {
    val.map(|dt| {
        let datetime = NaiveDateTime::new(
            from_days(dt.days() as i64, 1900),
            from_sec_fragments(dt.seconds_fragments() as i64),
        );
        datetime.format("'%Y-%m-%d %H:%M:%S'").to_string()
    })
    .unwrap_or_else(|| "NULL".to_string())
}

fn format_datetime2(val: &Option<DateTime2>) -> String {
    val.map(|dt| {
        let datetime = NaiveDateTime::new(
            from_days(dt.date().days() as i64, 1),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap()
                + Duration::nanoseconds(
                    dt.time().increments() as i64 * 10i64.pow(9 - dt.time().scale() as u32),
                ),
        );
        datetime.format("'%Y-%m-%d %H:%M:%S'").to_string()
    })
    .unwrap_or_else(|| "NULL".to_string())
}

fn format_small_datetime(val: &Option<SmallDateTime>) -> String {
    val.map(|dt| {
        let datetime = NaiveDateTime::new(
            from_days(dt.days() as i64, 1900),
            from_mins(dt.seconds_fragments() as u32 * 60),
        );
        datetime.format("'%Y-%m-%d %H:%M:%S'").to_string()
    })
    .unwrap_or_else(|| "NULL".to_string())
}

fn format_datetime_offset(val: &Option<DateTimeOffset>) -> String {
    val.map(|dto| {
        let date = from_days(dto.datetime2().date().days() as i64, 1);
        let ns = dto.datetime2().time().increments() as i64
            * 10i64.pow(9 - dto.datetime2().time().scale() as u32);

        let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::nanoseconds(ns)
            - Duration::minutes(dto.offset() as i64);
        let naive = NaiveDateTime::new(date, time);

        let dto: ChronosDateTime<Utc> = ChronosDateTime::from_utc(naive, Utc);
        dto.format("'%Y-%m-%d %H:%M:%S %z'").to_string()
    })
    .unwrap_or_else(|| "NULL".to_string())
}

fn from_days(days: i64, base_year: i32) -> NaiveDate {
    NaiveDate::from_ymd_opt(base_year, 1, 1).expect("Invalid date components")
        + Duration::days(days)
}

fn from_mins(minutes: u32) -> NaiveTime {
    let hours = minutes / 60;
    let minutes_remainder = minutes % 60;

    NaiveTime::from_hms_opt(0, hours, minutes_remainder).expect("Invalid time components")
}

fn from_sec_fragments(seconds_fragments: i64) -> NaiveTime {
    let milliseconds = seconds_fragments * 1000 / 300;
    let seconds = milliseconds / 1000;
    let milliseconds_remainder = milliseconds % 1000;
    let minutes = seconds / 60;
    let seconds_remainder = seconds % 60;
    let hours = minutes / 60;
    let minutes_remainder = minutes % 60;

    NaiveTime::from_hms_milli_opt(
        hours as u32,
        minutes_remainder as u32,
        seconds_remainder as u32,
        milliseconds_remainder as u32,
    )
    .expect("Invalid time components")
}
