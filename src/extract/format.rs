use anyhow::{Result, anyhow};
use chrono::DateTime as ChronosDateTime;
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use hex::encode;
use tiberius::numeric::Numeric;
use tiberius::time::{Date, DateTime, DateTime2, DateTimeOffset, SmallDateTime, Time};
use tiberius::{ColumnData, Row};

use crate::common::errors::MigrationError;

pub fn format_row_values(row: Row) -> Result<Vec<String>> {
    row.into_iter().map(format_column_value).collect()
}

pub fn format_column_value(item: ColumnData) -> Result<String> {
    match item {
        ColumnData::Binary(Some(val)) => Ok(format!("0x{}", encode(val))),
        ColumnData::Binary(None) => Ok("NULL".to_string()),
        ColumnData::Bit(val) => Ok(format_number_value(val.map(|b| b as u8))),
        ColumnData::I16(val) => Ok(format_number_value(val)),
        ColumnData::I32(val) => Ok(format_number_value(val)),
        ColumnData::I64(val) => Ok(format_number_value(val)),
        ColumnData::F32(val) => Ok(format_number_value(val)),
        ColumnData::F64(val) => Ok(format_number_value(val)),
        ColumnData::Guid(val) => Ok(format_string_value(val)),
        ColumnData::Numeric(val) => Ok(format_numeric_value(val)),
        ColumnData::String(val) => Ok(format_string_value(val)),
        ColumnData::Time(ref val) => format_time(val),
        ColumnData::Date(ref val) => format_date(val),
        ColumnData::SmallDateTime(ref val) => format_small_datetime(val),
        ColumnData::DateTime(ref val) => format_datetime(val),
        ColumnData::DateTime2(ref val) => format_datetime2(val),
        ColumnData::DateTimeOffset(ref val) => format_datetime_offset(val),
        ColumnData::U8(val) => Ok(format_number_value(val)),
        ColumnData::Xml(val) => match val {
            Some(xml) => Ok(format_string_value(Some(xml.as_ref().to_string()))),
            None => Ok("NULL".to_string()),
        },
    }
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

pub fn format_time(val: &Option<Time>) -> Result<String> {
    match val {
        Some(time) => {
            let ns = time.increments() as i64 * 10i64.pow(9 - time.scale() as u32);
            let base_time = NaiveTime::from_hms_opt(0, 0, 0).ok_or_else(|| {
                MigrationError::InvalidDateTimeValue {
                    reason: "failed to create base time 00:00:00".to_string(),
                }
            })?;
            let time = base_time + Duration::nanoseconds(ns);
            Ok(format!("{}", time.format("'%H:%M:%S%.f'")))
        }
        None => Ok("NULL".to_string()),
    }
}

pub fn format_date(val: &Option<Date>) -> Result<String> {
    match val {
        Some(dt) => {
            let datetime = from_days(dt.days() as i64, 1)?;
            Ok(datetime.format("'%Y-%m-%d'").to_string())
        }
        None => Ok("NULL".to_string()),
    }
}

pub fn format_datetime(val: &Option<DateTime>) -> Result<String> {
    match val {
        Some(dt) => {
            let date = from_days(dt.days() as i64, 1900)?;
            let time = from_sec_fragments(dt.seconds_fragments() as i64)?;
            let datetime = NaiveDateTime::new(date, time);
            Ok(datetime.format("'%Y-%m-%d %H:%M:%S'").to_string())
        }
        None => Ok("NULL".to_string()),
    }
}

pub fn format_datetime2(val: &Option<DateTime2>) -> Result<String> {
    match val {
        Some(dt) => {
            let date = from_days(dt.date().days() as i64, 1)?;
            let base_time = NaiveTime::from_hms_opt(0, 0, 0).ok_or_else(|| {
                MigrationError::InvalidDateTimeValue {
                    reason: "failed to create base time 00:00:00".to_string(),
                }
            })?;
            let ns = dt.time().increments() as i64 * 10i64.pow(9 - dt.time().scale() as u32);
            let time = base_time + Duration::nanoseconds(ns);
            let datetime = NaiveDateTime::new(date, time);
            Ok(datetime.format("'%Y-%m-%d %H:%M:%S%.f'").to_string())
        }
        None => Ok("NULL".to_string()),
    }
}

pub fn format_small_datetime(val: &Option<SmallDateTime>) -> Result<String> {
    match val {
        Some(dt) => {
            let date = from_days(dt.days() as i64, 1900)?;
            let time = from_minutes(dt.seconds_fragments() as u32)?;
            let datetime = NaiveDateTime::new(date, time);
            Ok(datetime.format("'%Y-%m-%d %H:%M:%S'").to_string())
        }
        None => Ok("NULL".to_string()),
    }
}

pub fn format_datetime_offset(val: &Option<DateTimeOffset>) -> Result<String> {
    match val {
        Some(dto) => {
            let date = from_days(dto.datetime2().date().days() as i64, 1)?;
            let ns = dto.datetime2().time().increments() as i64
                * 10i64.pow(9 - dto.datetime2().time().scale() as u32);

            let base_time = NaiveTime::from_hms_opt(0, 0, 0).ok_or_else(|| {
                MigrationError::InvalidDateTimeValue {
                    reason: "failed to create base time 00:00:00".to_string(),
                }
            })?;
            let time =
                base_time + Duration::nanoseconds(ns) - Duration::minutes(dto.offset() as i64);
            let naive = NaiveDateTime::new(date, time);

            let dto: ChronosDateTime<Utc> = naive.and_utc();
            Ok(dto.format("'%Y-%m-%d %H:%M:%S%.f'").to_string())
        }
        None => Ok("NULL".to_string()),
    }
}

pub fn from_days(days: i64, base_year: i32) -> Result<NaiveDate> {
    let base = NaiveDate::from_ymd_opt(base_year, 1, 1).ok_or_else(|| {
        MigrationError::InvalidDateTimeValue {
            reason: format!("invalid base year {}", base_year),
        }
    })?;
    base.checked_add_signed(Duration::days(days))
        .ok_or_else(|| {
            anyhow!(MigrationError::InvalidDateTimeValue {
                reason: format!("date overflow: {} days from base year {}", days, base_year),
            })
        })
}

pub fn from_minutes(minutes: u32) -> Result<NaiveTime> {
    let hours = minutes / 60;
    let minutes_remainder = minutes % 60;

    NaiveTime::from_hms_opt(hours, minutes_remainder, 0).ok_or_else(|| {
        anyhow!(MigrationError::InvalidDateTimeValue {
            reason: format!(
                "invalid time from {} minutes ({}h {}m)",
                minutes, hours, minutes_remainder
            ),
        })
    })
}

pub fn from_sec_fragments(seconds_fragments: i64) -> Result<NaiveTime> {
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
    .ok_or_else(|| {
        anyhow!(MigrationError::InvalidDateTimeValue {
            reason: format!(
                "invalid time from seconds_fragments {}: {}h {}m {}s {}ms",
                seconds_fragments,
                hours,
                minutes_remainder,
                seconds_remainder,
                milliseconds_remainder
            ),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_string_value_some() {
        assert_eq!(format_string_value(Some("hello")), "'hello'");
    }

    #[test]
    fn test_format_string_value_with_quotes() {
        assert_eq!(format_string_value(Some("it's")), "'it''s'");
    }

    #[test]
    fn test_format_string_value_none() {
        assert_eq!(format_string_value::<String>(None), "NULL");
    }

    #[test]
    fn test_format_number_value_some() {
        assert_eq!(format_number_value(Some(42)), "42");
    }

    #[test]
    fn test_format_number_value_none() {
        assert_eq!(format_number_value::<i32>(None), "NULL");
    }

    #[test]
    fn test_format_number_value_negative() {
        assert_eq!(format_number_value(Some(-100)), "-100");
    }

    #[test]
    fn test_format_number_value_float() {
        assert_eq!(format_number_value(Some(3.14)), "3.14");
    }

    #[test]
    fn test_from_days_valid() {
        let date = from_days(0, 2023).unwrap();
        assert_eq!(date, NaiveDate::from_ymd_opt(2023, 1, 1).unwrap());
    }

    #[test]
    fn test_from_days_with_offset() {
        let date = from_days(31, 2023).unwrap();
        assert_eq!(date, NaiveDate::from_ymd_opt(2023, 2, 1).unwrap());
    }

    #[test]
    fn test_from_days_base_year_1() {
        let date = from_days(0, 1).unwrap();
        assert_eq!(date, NaiveDate::from_ymd_opt(1, 1, 1).unwrap());
    }

    #[test]
    fn test_from_days_base_year_1900() {
        let date = from_days(0, 1900).unwrap();
        assert_eq!(date, NaiveDate::from_ymd_opt(1900, 1, 1).unwrap());
    }

    #[test]
    fn test_from_minutes_zero() {
        let time = from_minutes(0).unwrap();
        assert_eq!(time, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    }

    #[test]
    fn test_from_minutes_standard() {
        let time = from_minutes(90).unwrap();
        assert_eq!(time, NaiveTime::from_hms_opt(1, 30, 0).unwrap());
    }

    #[test]
    fn test_from_sec_fragments_zero() {
        let time = from_sec_fragments(0).unwrap();
        assert_eq!(time, NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_from_sec_fragments_one_second() {
        // 300 fragments = 1 second in MSSQL datetime encoding
        let time = from_sec_fragments(300).unwrap();
        assert_eq!(time, NaiveTime::from_hms_milli_opt(0, 0, 1, 0).unwrap());
    }

    #[test]
    fn test_from_sec_fragments_one_hour() {
        // 300 * 3600 = 1,080,000 fragments = 1 hour
        let time = from_sec_fragments(300 * 3600).unwrap();
        assert_eq!(time, NaiveTime::from_hms_milli_opt(1, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_format_time_none() {
        let result = format_time(&None).unwrap();
        assert_eq!(result, "NULL");
    }

    #[test]
    fn test_format_date_none() {
        let result = format_date(&None).unwrap();
        assert_eq!(result, "NULL");
    }

    #[test]
    fn test_format_datetime_none() {
        let result = format_datetime(&None).unwrap();
        assert_eq!(result, "NULL");
    }

    #[test]
    fn test_format_datetime2_none() {
        let result = format_datetime2(&None).unwrap();
        assert_eq!(result, "NULL");
    }

    #[test]
    fn test_format_small_datetime_none() {
        let result = format_small_datetime(&None).unwrap();
        assert_eq!(result, "NULL");
    }

    #[test]
    fn test_format_datetime_offset_none() {
        let result = format_datetime_offset(&None).unwrap();
        assert_eq!(result, "NULL");
    }

    #[test]
    fn test_format_binary_value_unquoted() {
        let result =
            format_column_value(ColumnData::Binary(Some(vec![0xFF, 0xAB].into()))).unwrap();
        assert_eq!(result, "0xffab");
    }

    #[test]
    fn test_format_binary_empty() {
        let result = format_column_value(ColumnData::Binary(Some(vec![].into()))).unwrap();
        assert_eq!(result, "0x");
    }

    #[test]
    fn test_format_bit_null() {
        assert_eq!(format_column_value(ColumnData::Bit(None)).unwrap(), "NULL");
    }

    #[test]
    fn test_format_bit_true() {
        assert_eq!(
            format_column_value(ColumnData::Bit(Some(true))).unwrap(),
            "1"
        );
    }

    #[test]
    fn test_format_bit_false() {
        assert_eq!(
            format_column_value(ColumnData::Bit(Some(false))).unwrap(),
            "0"
        );
    }

    #[test]
    fn test_format_u8_null() {
        assert_eq!(format_column_value(ColumnData::U8(None)).unwrap(), "NULL");
    }

    #[test]
    fn test_format_u8_value() {
        assert_eq!(format_column_value(ColumnData::U8(Some(42))).unwrap(), "42");
    }

    #[test]
    fn test_format_f32_unquoted() {
        let result = format_column_value(ColumnData::F32(Some(3.14))).unwrap();
        assert!(
            !result.starts_with('\''),
            "F32 should not be quoted: {}",
            result
        );
    }

    #[test]
    fn test_format_f64_unquoted() {
        let result = format_column_value(ColumnData::F64(Some(2.718))).unwrap();
        assert!(
            !result.starts_with('\''),
            "F64 should not be quoted: {}",
            result
        );
    }

    #[test]
    fn test_format_f32_null() {
        assert_eq!(format_column_value(ColumnData::F32(None)).unwrap(), "NULL");
    }

    #[test]
    fn test_format_f64_null() {
        assert_eq!(format_column_value(ColumnData::F64(None)).unwrap(), "NULL");
    }

    #[test]
    fn test_format_datetime2_fractional_seconds() {
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2023, 6, 15).unwrap(),
            NaiveTime::from_hms_micro_opt(14, 30, 45, 123456).unwrap(),
        );
        let formatted = dt.format("'%Y-%m-%d %H:%M:%S%.f'").to_string();
        assert!(formatted.contains("14:30:45.123456"), "Got: {}", formatted);
    }

    #[test]
    fn test_from_minutes_90_is_1h30m() {
        let time = from_minutes(90).unwrap();
        assert_eq!(time, NaiveTime::from_hms_opt(1, 30, 0).unwrap());
    }

    #[test]
    fn test_from_minutes_1440_is_invalid() {
        let result = from_minutes(1440);
        assert!(result.is_err());
    }
}
