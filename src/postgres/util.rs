use bytes::Bytes;
use chrono::{
    DateTime, FixedOffset, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, Offset, Utc,
};
use geo::Point;
use ijson::{IArray, INumber, IObject, IValue};
use ordered_float::OrderedFloat;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use std::num::ParseIntError;
use thiserror::Error;
use tokio_postgres::types::Type;

use super::{
    field::Field,
    types::{PostgresSchemaError, TableColumn},
};

pub const DATE_FORMAT: &str = "%Y-%m-%d";

pub fn is_network_failure(err: &tokio_postgres::Error) -> bool {
    //https://www.postgresql.org/docs/current/errcodes-appendix.html#ERRCODES-TABLE
    err.code().is_some_and(|c| c.code().starts_with("08"))
}

/// efficiently check if a decimal number can be parsed to an f64 such that when this f64 is converted back to a string,
/// it matches the original input without precision loss.
fn can_roundtrip_through_f64_losslessly(s: &str) -> bool {
    const LARGEST_ACCURATE_FRACTION: &str = "4503599627370495"; // 2^52 - 1

    let c: &[_] = &['-', '0', '.'];
    let s = s.trim_matches(c);

    if s.chars().any(|c| !c.is_ascii_digit() && c != '.') {
        // invalid number
        return false;
    }

    let fraction_digits = s.chars().filter(char::is_ascii_digit).count();

    #[allow(clippy::comparison_chain)]
    if fraction_digits > LARGEST_ACCURATE_FRACTION.len() {
        return false;
    } else if fraction_digits < LARGEST_ACCURATE_FRACTION.len() {
        return true;
    }

    let no_dot = fraction_digits == s.len();
    if no_dot {
        s <= LARGEST_ACCURATE_FRACTION
    } else {
        s.replace('.', "").as_str() <= LARGEST_ACCURATE_FRACTION
    }
}

/// tries to parse a decimal number to an f64 without losing precision when the f64 is converted back to a string.
pub fn lossless_string_f64_parse_opt(s: &str) -> Option<f64> {
    if can_roundtrip_through_f64_losslessly(s) {
        s.parse().ok()
    } else {
        None
    }
}
pub fn serde_json_to_json_value(
    value: serde_json::Value,
) -> Result<ijson::IValue, serde_json::Error> {
    // this match block's sole purpose is to properly convert `serde_json::Number` to IValue
    // when `serde_json/arbitrary_precision` feature is enabled.
    // `ijson::to_value()` by itself does not properly convert it.
    match value {
        serde_json::Value::Number(number) => {
            fn ivalue_from_number_opt(number: &serde_json::Number) -> Option<IValue> {
                if let Some(n) = number.as_i64() {
                    return Some(INumber::from(n).into());
                } else if let Some(n) = number.as_u64() {
                    return Some(INumber::from(n).into());
                } else if let Some(n) = lossless_string_f64_parse_opt(&number.to_string()) {
                    if let Ok(value) = INumber::try_from(n) {
                        return Some(value.into());
                    }
                }
                None
            }
            if let Some(value) = ivalue_from_number_opt(&number) {
                Ok(value)
            } else {
                ijson::to_value(serde_json::Value::Number(number)).map_err(Into::into)
            }
        }
        serde_json::Value::Array(vec) => {
            let mut array = IArray::with_capacity(vec.len());
            for value in vec {
                array.push(serde_json_to_json_value(value)?);
            }
            Ok(array.into())
        }
        serde_json::Value::Object(map) => {
            let mut object = IObject::with_capacity(map.len());
            for (key, value) in map {
                object.insert(key, serde_json_to_json_value(value)?);
            }
            Ok(object.into())
        }
        value => ijson::to_value(value).map_err(Into::into),
    }
}

#[derive(Error, Debug)]
pub enum DateConversionError {
    #[error("Failed to read error part. Error: {0}")]
    FailedParseDate(#[from] ParseIntError),

    #[error("Failed to convert date")]
    InvalidDate,

    #[error("Failed to convert time")]
    InvalidTime,

    #[error("Ambiguous date result")]
    AmbiguousTimeResult,
}

pub fn convert_date(date: &str) -> Result<NaiveDateTime, DateConversionError> {
    // Fill right side with zeros when date time is not full
    let date_string = format!("{date:0<26}");

    let year: i32 = date_string[0..4].parse()?;
    let month: u32 = date_string[5..7].parse()?;
    let day: u32 = date_string[8..10].parse()?;
    let hour: u32 = date_string[11..13].parse()?;
    let minute: u32 = date_string[14..16].parse()?;
    let second: u32 = date_string[17..19].parse()?;
    let microseconds = if date_string.len() == 19 {
        0
    } else {
        date_string[20..26].parse()?
    };

    let naive_date =
        NaiveDate::from_ymd_opt(year, month, day).ok_or(DateConversionError::InvalidDate)?;
    let naive_time = NaiveTime::from_hms_micro_opt(hour, minute, second, microseconds)
        .ok_or(DateConversionError::InvalidTime)?;

    Ok(NaiveDateTime::new(naive_date, naive_time))
}

pub fn parse_json_slice(bytes: &[u8]) -> Result<ijson::IValue, serde_json::Error> {
    let serde_value: serde_json::Value = serde_json::from_slice(bytes)?;
    ijson::to_value(serde_value).map_err(Into::into)
}

pub fn postgres_type_to_field(
    value: Option<&Bytes>,
    column: &TableColumn,
) -> Result<Field, PostgresSchemaError> {
    let column_type = column.r#type.clone();
    value.map_or(Ok(Field::Null), |v| match column_type {
        Type::INT2 | Type::INT4 | Type::INT8 => Ok(Field::Int(
            String::from_utf8(v.to_vec()).unwrap().parse().unwrap(),
        )),
        Type::FLOAT4 | Type::FLOAT8 => Ok(Field::Float(OrderedFloat(
            String::from_utf8(v.to_vec())
                .unwrap()
                .parse::<f64>()
                .unwrap(),
        ))),
        Type::TEXT
        | Type::VARCHAR
        | Type::CHAR
        | Type::BPCHAR
        | Type::ANYENUM
        | Type::CHAR_ARRAY
        | Type::UNKNOWN
        | Type::VARCHAR_ARRAY
        | Type::UUID => Ok(Field::String(String::from_utf8(v.to_vec()).unwrap())),
        Type::BYTEA => Ok(Field::Binary(v.to_vec())),
        Type::NUMERIC => Ok(Field::Decimal(
            Decimal::from_f64(
                String::from_utf8(v.to_vec())
                    .unwrap()
                    .parse::<f64>()
                    .unwrap(),
            )
            .unwrap(),
        )),
        Type::TIMESTAMP => {
            let date_string = String::from_utf8(v.to_vec())?;

            Ok(Field::Timestamp(DateTime::from_naive_utc_and_offset(
                convert_date(&date_string)?,
                Utc.fix(),
            )))
        }
        Type::TIMESTAMPTZ => {
            let date_string = String::from_utf8(v.to_vec())?;

            Ok(convert_date_with_timezone(&date_string).map(Field::Timestamp)?)
        }
        Type::DATE => {
            let date: NaiveDate = NaiveDate::parse_from_str(
                String::from_utf8(v.to_vec()).unwrap().as_str(),
                DATE_FORMAT,
            )
            .unwrap();
            Ok(Field::Date(date))
        }
        Type::JSONB | Type::JSON => {
            let val: serde_json::Value = serde_json::from_slice(v).map_err(|_| {
                PostgresSchemaError::JSONBParseError(format!(
                    "Error converting to a single row for: {}",
                    column_type.name()
                ))
            })?;
            let json = serde_json_to_json_value(val).map_err(|e| PostgresSchemaError::TypeError)?;
            Ok(Field::Json(json))
        }
        Type::JSONB_ARRAY | Type::JSON_ARRAY => {
            let json_val = parse_json_slice(v).map_err(|_| {
                PostgresSchemaError::JSONBParseError(format!(
                    "Error converting to a single row for: {}",
                    column_type.name()
                ))
            })?;
            Ok(Field::Json(json_val))
        }
        Type::BOOL => Ok(Field::Boolean(v.slice(0..1) == "t")),
        Type::POINT => Ok(Field::Point(
            String::from_utf8(v.to_vec())
                .map(|i| parse_point(i.as_str()))
                .map_err(PostgresSchemaError::StringParseError)?
                .map_err(|_| PostgresSchemaError::PointParseError)?,
        )),
        _ => Err(PostgresSchemaError::ColumnTypeNotSupported(
            column_type.name().to_string(),
        )),
    })
}

fn parse_point(point: &str) -> Result<geo::Point<OrderedFloat<f64>>, PostgresSchemaError> {
    let s = point.replace('(', "");
    let s = s.replace(')', "");
    let mut cs = s.split(',');
    let x = cs
        .next()
        .ok_or(PostgresSchemaError::TypeError)?
        .parse::<f64>()
        .map_err(|_| PostgresSchemaError::TypeError)?;
    let y = cs
        .next()
        .ok_or(PostgresSchemaError::TypeError)?
        .parse::<f64>()
        .map_err(|_| PostgresSchemaError::TypeError)?;
    Ok(Point::from((OrderedFloat(x), OrderedFloat(y))))
}

/// This function converts any offset string (+03, +03:00 and etc) to `FixedOffset`
///
fn parse_timezone_offset(offset_string: &str) -> Result<Option<FixedOffset>, ParseIntError> {
    // Fill right side with zeros when offset is not full length
    let offset_string = format!("{offset_string:0<9}");

    let sign = &offset_string[0..1];
    let hour = offset_string[1..3].parse::<i32>()?;
    let min = offset_string[4..6].parse::<i32>()?;
    let sec = offset_string[7..9].parse::<i32>()?;

    let secs = (hour * 3600) + (min * 60) + sec;

    if sign == "-" {
        Ok(FixedOffset::west_opt(secs))
    } else {
        Ok(FixedOffset::east_opt(secs))
    }
}

fn convert_date_with_timezone(date: &str) -> Result<DateTime<FixedOffset>, DateConversionError> {
    // Find position of last + or -, which is the start of timezone offset
    let pos_plus = date.rfind('+');
    let pos_min = date.rfind('-');

    let pos = match (pos_plus, pos_min) {
        (Some(plus), Some(min)) => {
            if plus > min {
                plus
            } else {
                min
            }
        }
        (None, Some(pos)) | (Some(pos), None) => pos,
        (None, None) => 0,
    };

    let (date, offset_string) = date.split_at(pos);

    let offset = parse_timezone_offset(offset_string)?.map_or(Utc.fix(), |x| x);

    match convert_date(date)?.and_local_timezone(offset) {
        LocalResult::None => Err(DateConversionError::InvalidTime),
        LocalResult::Single(date) => Ok(date),
        LocalResult::Ambiguous(_, _) => Err(DateConversionError::AmbiguousTimeResult),
    }
}
