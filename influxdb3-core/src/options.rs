use chrono::DateTime;

use crate::error::InfluxDBError;

#[derive(Debug, Default)]
#[repr(u8)]
pub enum QueryType {
    #[default]
    SQL = 0,
    InfluxQL
}

#[derive(Debug, Default, Clone, Copy)]
pub enum TimestampPrecision {
    #[default]
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

impl TimestampPrecision {
    pub fn v2_str(&self) -> &'static str {
        match self {
            TimestampPrecision::Nanoseconds => "ns",
            TimestampPrecision::Microseconds => "µs",
            TimestampPrecision::Milliseconds => "ms",
            TimestampPrecision::Seconds => "s",
        }
    }

    pub fn v3_str(&self) -> &'static str {
        match self {
            TimestampPrecision::Nanoseconds => "nanosecond",
            TimestampPrecision::Microseconds => "microsecond",
            TimestampPrecision::Milliseconds => "millisecond",
            TimestampPrecision::Seconds => "second",
        }
    }
}

impl TryFrom<&str> for TimestampPrecision {
    type Error = InfluxDBError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "ns" | "nanosecond" => Ok(TimestampPrecision::Nanoseconds),
            "us" | "microsecond" => Ok(TimestampPrecision::Microseconds),
            "ms" | "millisecond" => Ok(TimestampPrecision::Milliseconds),
            "s" | "second" => Ok(TimestampPrecision::Seconds),
            _ => Err(InfluxDBError::InvalidTimestampPrecision(value.to_string())),
        }
    }
}

impl TimestampPrecision {
    pub(crate) fn process_timestamp<Tz>(&self, dt: DateTime<Tz>) -> i64
    where
        Tz: chrono::TimeZone,
    {
        match self {
            TimestampPrecision::Nanoseconds => dt.timestamp_nanos_opt().expect("Timestamp out of range"),
            TimestampPrecision::Microseconds => dt.timestamp_micros(),
            TimestampPrecision::Milliseconds => dt.timestamp_millis(),
            TimestampPrecision::Seconds => dt.timestamp(),
        }
    }
}