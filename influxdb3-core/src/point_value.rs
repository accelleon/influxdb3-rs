use chrono::{DateTime, Utc};

use crate::error::InfluxDBError;

#[derive(Debug, Clone)]
pub enum PointValue {
    Null,
    Float(f64),
    Integer(i64),
    UInteger(u64),
    Boolean(bool),
    String(String),
    Timestamp(DateTime<Utc>)
}

impl PointValue {
    pub(crate) fn serialize(&self) -> String {
        match self {
            PointValue::Null => String::new(),
            PointValue::Float(v) => v.to_string(),
            PointValue::Integer(v) => format!("{v}i"),
            PointValue::UInteger(v) => format!("{}u", v),
            PointValue::Boolean(v) => if *v { "t".to_string() } else { "f".to_string() },
            PointValue::String(v) => format!("\"{}\"", v.replace("\\", "\\\\".into()).replace("\"", "\\\"".into())),
            PointValue::Timestamp(v) => v.timestamp_nanos_opt().expect("Invalid timestamp".into()).to_string(),
        }
    }

    pub fn get_value<'a, T>(&'a self) -> Result<Option<T>, InfluxDBError>
    where
        T: Decode<'a>,
    {
        match self {
            PointValue::Null => Ok(None),
            _ => Ok(Some(T::decode(self)?)),
        }
    }
}

pub trait Encode: std::fmt::Debug {
    fn encode(self) -> PointValue;
}

pub trait Decode<'a> {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError>
    where
        Self: Sized;
}

// Blanket implementations
impl<T: Encode + Clone> Encode for &T {
    fn encode(self) -> PointValue {
        (self).clone().encode()
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encode(self) -> PointValue {
        match self {
            Some(v) => v.encode(),
            None => PointValue::Null,
        }
    }
}

impl<'a, T: Decode<'a>> Decode<'a> for Option<T> {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError> {
        match value {
            PointValue::Null => Ok(None),
            _ => Ok(Some(T::decode(value)?)),
        }
    }
}

// Encode implementations
impl Encode for () {
    fn encode(self) -> PointValue {
        PointValue::Null
    }
}

impl Encode for f64 {
    fn encode(self) -> PointValue {
        PointValue::Float(self)
    }
}

impl Encode for f32 {
    fn encode(self) -> PointValue {
        PointValue::Float(self as f64)
    }
}

impl Encode for i8 {
    fn encode(self) -> PointValue {
        PointValue::Integer(self as i64)
    }
}

impl Encode for i16 {
    fn encode(self) -> PointValue {
        PointValue::Integer(self as i64)
    }
}

impl Encode for i32 {
    fn encode(self) -> PointValue {
        PointValue::Integer(self as i64)
    }
}

impl Encode for i64 {
    fn encode(self) -> PointValue {
        PointValue::Integer(self)
    }
}

impl Encode for u8 {
    fn encode(self) -> PointValue {
        PointValue::UInteger(self as u64)
    }
}

impl Encode for u16 {
    fn encode(self) -> PointValue {
        PointValue::UInteger(self as u64)
    }
}

impl Encode for u32 {
    fn encode(self) -> PointValue {
        PointValue::UInteger(self as u64)
    }
}

impl Encode for u64 {
    fn encode(self) -> PointValue {
        PointValue::UInteger(self)
    }
}

impl Encode for bool {
    fn encode(self) -> PointValue {
        PointValue::Boolean(self)
    }
}

impl Encode for &str {
    fn encode(self) -> PointValue {
        PointValue::String(self.to_string())
    }
}

impl Encode for String {
    fn encode(self) -> PointValue {
        PointValue::String(self.clone())
    }
}

impl Encode for DateTime<Utc> {
    fn encode(self) -> PointValue {
        PointValue::Timestamp(self)
    }
}

impl Encode for PointValue {
    fn encode(self) -> PointValue {
        self.clone()
    }
}

// Decode implementations
impl<'a> Decode<'a> for f64 {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError> {
        match value {
            PointValue::Float(v) => Ok(*v),
            _ => Err(InfluxDBError::InvalidPointValueConversion("PointValue is not a Float".into())),
        }
    }
}

impl<'a> Decode<'a> for f32 {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError> {
        match value {
            PointValue::Float(v) => {
                let f = *v as f32;
                if v.is_finite() != f.is_finite() {
                    return Err(InfluxDBError::InvalidPointValueConversion("Value out of range for f32".into()));
                }
                Ok(f)
            },
            _ => Err(InfluxDBError::InvalidPointValueConversion("PointValue is not a Float".into())),
        }
    }
}

impl<'a> Decode<'a> for i8 {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError> {
        match value {
            PointValue::Integer(v) => (*v).try_into().map_err(|_| InfluxDBError::InvalidPointValueConversion("Value out of range for i8".into())),
            _ => Err(InfluxDBError::InvalidPointValueConversion("PointValue is not an Integer".into())),
        }
    }
}

impl<'a> Decode<'a> for i16 {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError> {
        match value {
            PointValue::Integer(v) => (*v).try_into().map_err(|_| InfluxDBError::InvalidPointValueConversion("Value out of range for i16".into())),
            _ => Err(InfluxDBError::InvalidPointValueConversion("PointValue is not an Integer".into())),
        }
    }
}

impl<'a> Decode<'a> for i32 {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError> {
        match value {
            PointValue::Integer(v) => (*v).try_into().map_err(|_| InfluxDBError::InvalidPointValueConversion("Value out of range for i32".into())),
            _ => Err(InfluxDBError::InvalidPointValueConversion("PointValue is not an Integer".into())),
        }
    }
}

impl<'a> Decode<'a> for i64 {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError> {
        match value {
            PointValue::Integer(v) => Ok(*v),
            v => Err(InfluxDBError::InvalidPointValueConversion(format!("PointValue is not an Integer {:?}", v))),
        }
    }
}

impl<'a> Decode<'a> for u8 {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError> {
        match value {
            PointValue::UInteger(v) => (*v).try_into().map_err(|_| InfluxDBError::InvalidPointValueConversion("Value out of range for u8".into())),
            _ => Err(InfluxDBError::InvalidPointValueConversion("PointValue is not a UInteger".into())),
        }
    }
}

impl<'a> Decode<'a> for u16 {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError> {
        match value {
            PointValue::UInteger(v) => (*v).try_into().map_err(|_| InfluxDBError::InvalidPointValueConversion("Value out of range for u16".into())),
            _ => Err(InfluxDBError::InvalidPointValueConversion("PointValue is not a UInteger".into())),
        }
    }
}

impl<'a> Decode<'a> for u32 {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError> {
        match value {
            PointValue::UInteger(v) => (*v).try_into().map_err(|_| InfluxDBError::InvalidPointValueConversion("Value out of range for u32".into())),
            _ => Err(InfluxDBError::InvalidPointValueConversion("PointValue is not a UInteger".into())),
        }
    }
}

impl<'a> Decode<'a> for u64 {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError> {
        match value {
            PointValue::UInteger(v) => Ok(*v),
            _ => Err(InfluxDBError::InvalidPointValueConversion("PointValue is not a UInteger".into())),
        }
    }
}

impl<'a> Decode<'a> for bool {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError> {
        match value {
            PointValue::Boolean(v) => Ok(*v),
            _ => Err(InfluxDBError::InvalidPointValueConversion("PointValue is not a Boolean".into())),
        }
    }
}

impl<'a> Decode<'a> for String {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError> {
        match value {
            PointValue::String(v) => Ok(v.clone()),
            _ => Err(InfluxDBError::InvalidPointValueConversion("PointValue is not a String".into())),
        }
    }
}

impl<'a> Decode<'a> for &'a str {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError> {
        match value {
            PointValue::String(v) => Ok(v.as_str()),
            _ => Err(InfluxDBError::InvalidPointValueConversion("PointValue is not a String".into())),
        }
    }
}

impl<'a> Decode<'a> for DateTime<Utc> {
    fn decode(value: &'a PointValue) -> Result<Self, InfluxDBError> {
        match value {
            PointValue::Timestamp(v) => Ok(*v),
            _ => Err(InfluxDBError::InvalidPointValueConversion("PointValue is not a Timestamp".into())),
        }
    }
}