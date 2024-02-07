use std::time::Duration;

use ordered_float::OrderedFloat;
use postgres_types::Type;
use serde::{Deserialize, Serialize};

use super::types::PostgresSchemaError;


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum FieldType {
    /// Unsigned 64-bit integer.
    UInt,
    /// Unsigned 128-bit integer.
    U128,
    /// Signed 64-bit integer.
    Int,
    /// Signed 128-bit integer.
    I128,
    /// 64-bit floating point number.
    Float,
    /// `true` or `false`.
    Boolean,
    /// A string with limited length.
    String,
    /// A long string.
    Text,
    /// Raw bytes.
    Binary,
    /// `Decimal` represents a 128 bit representation of a fixed-precision decimal number.
    /// The finite set of values of type `Decimal` are of the form m / 10<sup>e</sup>,
    /// where m is an integer such that -2<sup>96</sup> < m < 2<sup>96</sup>, and e is an integer
    /// between 0 and 28 inclusive.
    Decimal,
    /// Timestamp up to nanoseconds.
    Timestamp,
    /// Allows for every date from Jan 1, 262145 BCE to Dec 31, 262143 CE.
    Date,
    /// JsonValue.
    Json,
    /// A geographic point.
    Point,
    /// Duration up to nanoseconds.
    Duration,
}


pub fn postgres_type_to_field_type(column_type: &Type) -> Result<FieldType, PostgresSchemaError> {
    match column_type {
        &Type::BOOL => Ok(FieldType::Boolean),
        &Type::INT2 | &Type::INT4 | &Type::INT8 => Ok(FieldType::Int),
        &Type::CHAR | &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR | &Type::UUID | &Type::ANYENUM => {
            Ok(FieldType::String)
        }
        &Type::FLOAT4 | &Type::FLOAT8 => Ok(FieldType::Float),
        &Type::BYTEA => Ok(FieldType::Binary),
        &Type::TIMESTAMP | &Type::TIMESTAMPTZ => Ok(FieldType::Timestamp),
        &Type::NUMERIC => Ok(FieldType::Decimal),
        &Type::JSONB
        | &Type::JSON
        | &Type::JSONB_ARRAY
        | &Type::JSON_ARRAY
        | &Type::TEXT_ARRAY
        | &Type::CHAR_ARRAY
        | &Type::VARCHAR_ARRAY
        | &Type::BPCHAR_ARRAY => Ok(FieldType::Json),
        &Type::DATE => Ok(FieldType::Date),
        &Type::POINT => Ok(FieldType::Point),
        _ => Err(PostgresSchemaError::ColumnTypeNotSupported(
            column_type.name().to_string(),
        )),
    }
}



#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Field {
    UInt(u64),
    U128(u128),
    Int(i64),
    I128(i128),
    Float(ordered_float::OrderedFloat<f64>),
    Boolean(bool),
    String(String),
    Text(String),
    Binary(Vec<u8>),
    Decimal(rust_decimal::Decimal),
    Timestamp(chrono::DateTime<chrono::FixedOffset>),
    Date(chrono::NaiveDate),
    Json(ijson::IValue),
    Point(geo::Point<OrderedFloat<f64>>),
    // TODO convert to ISO duration
    Duration(Duration),
    Null,
}