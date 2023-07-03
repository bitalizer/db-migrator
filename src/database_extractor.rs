use crate::schema::ColumnSchema;
use chrono::DateTime as ChronosDateTime;
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use hex::encode;
use tiberius::time::{Date, DateTime, DateTime2, DateTimeOffset, SmallDateTime, Time};
use tiberius::{Client, ColumnData, Row};
use tokio::net::TcpStream;
use tokio_util::compat::Compat;

pub struct DatabaseExtractor {
    client: Client<Compat<TcpStream>>,
}

impl DatabaseExtractor {
    pub fn new(client: Client<Compat<TcpStream>>) -> Self {
        DatabaseExtractor { client }
    }

    pub async fn fetch_tables(&mut self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let rows = self
            .client
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
                    None => Err(Box::<dyn std::error::Error>::from(
                        "Failed to retrieve table name",
                    )),
                }
            })
            .collect::<Result<Vec<String>, _>>()?;

        Ok(tables)
    }

    pub async fn get_table_schema(
        &mut self,
        table: &str,
    ) -> Result<Vec<ColumnSchema>, Box<dyn std::error::Error>> {
        let query = format!(
            "SELECT
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{}'",
            table
        );

        let rows = self
            .client
            .simple_query(query)
            .await?
            .into_first_result()
            .await?;

        let schema = rows
            .into_iter()
            .map(|r| ColumnSchema {
                column_name: Column::get(&r, "COLUMN_NAME"),
                data_type: Column::get(&r, "DATA_TYPE"),
                character_maximum_length: Column::get(&r, "CHARACTER_MAXIMUM_LENGTH"),
                numeric_precision: Column::get(&r, "NUMERIC_PRECISION"),
                numeric_scale: Column::get(&r, "NUMERIC_SCALE"),
            })
            .collect();

        Ok(schema)
    }

    pub async fn fetch_rows_from_table(
        &mut self,
        table: &str,
    ) -> Result<Vec<Row>, Box<dyn std::error::Error>> {
        let rows = self
            .client
            .simple_query(format!("SELECT * FROM [{}]", table))
            .await?
            .into_first_result()
            .await?;

        Ok(rows)
    }

    pub fn generate_insert_queries(
        &mut self,
        table_name: &str,
        rows: Vec<Row>,
        schema: &[ColumnSchema],
    ) -> Vec<String> {
        let mut insert_queries = Vec::new();

        for row in rows {
            let insert_statement = Self::generate_insert_statement(table_name, schema);
            let values_clause = Self::generate_values_clause(row);

            let full_query = format!("{} {}", insert_statement, values_clause);
            insert_queries.push(full_query);
        }

        insert_queries
    }

    fn generate_insert_statement(table_name: &str, schema: &[ColumnSchema]) -> String {
        let mut insert_query = format!("INSERT INTO `{}` (", table_name);

        for (i, column) in schema.iter().enumerate() {
            if i > 0 {
                insert_query.push_str(", ");
            }

            insert_query.push_str(&column.column_name);
        }

        insert_query.push_str(")");

        insert_query
    }

    fn generate_values_clause(row: Row) -> String {
        let mut values_query = "VALUES (".to_string();
        let mut first_value = true;

        for item in row.into_iter() {
            let output = match item {
                ColumnData::Binary(Some(val)) => format!("'0x{}'", encode(&val)),
                ColumnData::Binary(None) => "NULL".to_string(),
                ColumnData::Bit(val) => val.unwrap_or_default().to_string(),
                ColumnData::I16(val) => format_numeric_value(val),
                ColumnData::I32(val) => format_numeric_value(val),
                ColumnData::I64(val) => format_numeric_value(val),
                ColumnData::F32(val) => format_string_value(val),
                ColumnData::F64(val) => format_string_value(val),
                ColumnData::Guid(val) => format_string_value(val),
                ColumnData::Numeric(val) => format_string_value(val),
                ColumnData::String(val) => format_string_value(val),
                ColumnData::Time(ref val) => format_time(val),
                ColumnData::Date(ref val) => format_date(val),
                ColumnData::SmallDateTime(ref val) => format_small_datetime(val),
                ColumnData::DateTime(ref val) => format_datetime(val),
                ColumnData::DateTime2(ref val) => format_datetime2(val),
                ColumnData::DateTimeOffset(ref val) => format_datetime_offset(val),
                ColumnData::U8(val) => val.unwrap_or_default().to_string(),
                ColumnData::Xml(val) => val.unwrap().as_ref().to_string(),
                _ => "NULL".to_string(), // "NULL" for unsupported column types or None values
            };

            if !first_value {
                values_query.push_str(", ");
            }
            values_query.push_str(&output);
            first_value = false;
        }

        values_query.push_str(")");
        values_query
    }
}

fn format_string_value<T: ToString>(value: Option<T>) -> String {
    value
        .map(|v| format!("'{}'", v.to_string().replace("'", "''")))
        .unwrap_or_else(|| "NULL".to_string())
}

fn format_numeric_value<T>(value: Option<T>) -> String
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
        datetime.format("'%Y-%m-%d").to_string()
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

pub trait Column {
    fn get(row: &Row, col_name: &str) -> Self;
}

impl Column for i32 {
    fn get(row: &Row, col_name: &str) -> i32 {
        match row.try_get::<i32, _>(col_name) {
            Ok(Some(value)) => value,
            _ => panic!("Failed to get column value"),
        }
    }
}

impl Column for Option<i32> {
    fn get(row: &Row, col_name: &str) -> Option<i32> {
        row.get::<i32, _>(col_name)
    }
}

impl Column for Option<u8> {
    fn get(row: &Row, col_name: &str) -> Option<u8> {
        row.get::<u8, _>(col_name)
    }
}

impl Column for Option<i64> {
    fn get(row: &Row, col_name: &str) -> Option<i64> {
        row.get::<i64, _>(col_name)
    }
}

impl Column for String {
    fn get(row: &Row, col_name: &str) -> String {
        row.try_get::<&str, _>(col_name)
            .unwrap_or_default()
            .unwrap_or_default()
            .to_string()
    }
}

impl Column for Option<String> {
    fn get(row: &Row, col_name: &str) -> Option<String> {
        match row.get::<&str, _>(col_name) {
            Some(data) => Some(data.to_string()),
            None => None,
        }
    }
}
