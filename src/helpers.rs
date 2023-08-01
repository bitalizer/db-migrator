use anyhow::Error;
use chrono::DateTime as ChronosDateTime;
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use tiberius::numeric::Numeric;
use tiberius::time::{Date, DateTime, DateTime2, DateTimeOffset, SmallDateTime, Time};

pub fn print_error_chain(err: &Error) {
    // Concatenate the main context message along with its chain of errors
    let error_message = err
        .chain()
        .enumerate()
        .map(|(index, cause)| {
            if index == 0 {
                cause.to_string()
            } else {
                format!("       └> {}", cause)
            }
        })
        .collect::<Vec<String>>()
        .join("\n");

    // Print the error message
    error!("{}", error_message);
}

pub fn format_snake_case(column_name: &str) -> String {
    let mut formatted_name = String::new();
    let mut prev_char: Option<char> = None;

    for c in column_name.chars() {
        if c.is_uppercase() {
            if let Some(prev) = prev_char {
                if !(prev == '_' || prev.is_uppercase()) {
                    formatted_name.push('_');
                }
            }
            formatted_name.push(c.to_ascii_lowercase());
        } else {
            formatted_name.push(c);
        }

        prev_char = Some(c);
    }

    formatted_name
}

pub fn format_numeric_value(value: Option<Numeric>) -> String {
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

pub fn format_string_value<T: ToString>(value: Option<T>) -> String {
    value
        .map(|v| format!("'{}'", v.to_string().replace('\'', "''")))
        .unwrap_or_else(|| "NULL".to_string())
}

pub fn format_number_value<T>(value: Option<T>) -> String
where
    T: std::fmt::Display,
{
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

pub fn format_time(val: &Option<Time>) -> String {
    val.map(|time| {
        let ns = time.increments() as i64 * 10i64.pow(9 - time.scale() as u32);
        let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::nanoseconds(ns);
        format!("{}", time.format("'%H:%M:%S'"))
    })
    .unwrap_or_else(|| "NULL".to_string())
}

pub fn format_date(val: &Option<Date>) -> String {
    val.map(|dt| {
        let datetime = from_days(dt.days() as i64, 1);
        datetime.format("'%Y-%m-%d'").to_string()
    })
    .unwrap_or_else(|| "NULL".to_string())
}

pub fn format_datetime(val: &Option<DateTime>) -> String {
    val.map(|dt| {
        let datetime = NaiveDateTime::new(
            from_days(dt.days() as i64, 1900),
            from_sec_fragments(dt.seconds_fragments() as i64),
        );
        datetime.format("'%Y-%m-%d %H:%M:%S'").to_string()
    })
    .unwrap_or_else(|| "NULL".to_string())
}

pub fn format_datetime2(val: &Option<DateTime2>) -> String {
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

pub fn format_small_datetime(val: &Option<SmallDateTime>) -> String {
    val.map(|dt| {
        let datetime = NaiveDateTime::new(
            from_days(dt.days() as i64, 1900),
            from_mins(dt.seconds_fragments() as u32 * 60),
        );
        datetime.format("'%Y-%m-%d %H:%M:%S'").to_string()
    })
    .unwrap_or_else(|| "NULL".to_string())
}

pub fn format_datetime_offset(val: &Option<DateTimeOffset>) -> String {
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

pub fn from_days(days: i64, base_year: i32) -> NaiveDate {
    NaiveDate::from_ymd_opt(base_year, 1, 1).expect("Invalid date components")
        + Duration::days(days)
}

pub fn from_mins(minutes: u32) -> NaiveTime {
    let hours = minutes / 60;
    let minutes_remainder = minutes % 60;

    NaiveTime::from_hms_opt(0, hours, minutes_remainder).expect("Invalid time components")
}

pub fn from_sec_fragments(seconds_fragments: i64) -> NaiveTime {
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
