use arrow_array::cast::{as_boolean_array, as_primitive_array, as_string_array};
use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_schema::{DataType, Field, TimeUnit};
use chrono::DateTime;
use futures::{Stream, StreamExt as _};

use crate::Point;
use crate::InfluxDBError;
use crate::PointValue;

enum ColumnType {
    Integer,
    UInteger,
    Float,
    String,
    Boolean,
    Tag,
    Timestamp,
    Unknown,
}

impl From<&str> for ColumnType {
    fn from(s: &str) -> Self {
        match s {
            "iox::column_type::field::integer" => ColumnType::Integer,
            "iox::column_type::field::uinteger" => ColumnType::UInteger,
            "iox::column_type::field::float" => ColumnType::Float,
            "iox::column_type::field::string" => ColumnType::String,
            "iox::column_type::field::boolean" => ColumnType::Boolean,
            "iox::column_type::tag" => ColumnType::Tag,
            "iox::column_type::timestamp" => ColumnType::Timestamp,
            _ => ColumnType::Unknown,
        }
    }
}

fn get_arrow_value(array: &dyn Array, field: &Field, row: usize) -> Result<PointValue, InfluxDBError> {
    match field.data_type() {
        DataType::Null => Ok(PointValue::Null),
        DataType::Boolean => {
            let arr: &BooleanArray = as_boolean_array(array);
            if arr.is_null(row) {
                Ok(PointValue::Null)
            } else {
                Ok(PointValue::Boolean(arr.value(row)))
            }
        },
        DataType::Float64 => {
            let arr: &Float64Array = as_primitive_array(array);
            if arr.is_null(row) {
                Ok(PointValue::Null)
            } else {
                Ok(PointValue::Float(arr.value(row)))
            }
        },
        DataType::Int64 => {
            let arr: &Int64Array = as_primitive_array(array);
            if arr.is_null(row) {
                Ok(PointValue::Null)
            } else {
                Ok(PointValue::Integer(arr.value(row)))
            }
        },
        DataType::UInt64 => {
            let arr: &Int64Array = as_primitive_array(array);
            if arr.is_null(row) {
                Ok(PointValue::Null)
            } else {
                Ok(PointValue::UInteger(arr.value(row) as u64))
            }
        },
        DataType::Utf8 => {
            let arr: &StringArray = as_string_array(array);
            if arr.is_null(row) {
                Ok(PointValue::Null)
            } else {
                Ok(PointValue::String(arr.value(row).to_string()))
            }
        },
        DataType::Timestamp(unit, _tz ) => {
            match unit {
                TimeUnit::Second => {
                    let arr: &TimestampSecondArray = as_primitive_array(array);
                    if arr.is_null(row) {
                        Ok(PointValue::Null)
                    } else {
                        Ok(PointValue::Timestamp(DateTime::from_timestamp_secs(arr.value(row)).expect("Invalid timestamp")))
                    }
                },
                TimeUnit::Millisecond => {
                    let arr: &TimestampMillisecondArray = as_primitive_array(array);
                    if arr.is_null(row) {
                        Ok(PointValue::Null)
                    } else {
                        Ok(PointValue::Timestamp(DateTime::from_timestamp_millis(arr.value(row)).expect("Invalid timestamp")))
                    }
                },
                TimeUnit::Microsecond => {
                    let arr: &TimestampMicrosecondArray = as_primitive_array(array);
                    if arr.is_null(row) {
                        Ok(PointValue::Null)
                    } else {
                        Ok(PointValue::Timestamp(DateTime::from_timestamp_micros(arr.value(row)).expect("Invalid timestamp")))
                    }
                },
                TimeUnit::Nanosecond => {
                    let arr: &TimestampNanosecondArray = as_primitive_array(array);
                    if arr.is_null(row) {
                        Ok(PointValue::Null)
                    } else {
                        Ok(PointValue::Timestamp(DateTime::from_timestamp_nanos(arr.value(row))))
                    }
                },
            }
        },
        _ => Err(InfluxDBError::InvalidPointValue(field.name().to_string(), field.data_type().to_string())),
    }
}

fn get_point(batch: &RecordBatch, row: usize) -> Result<Point, InfluxDBError> {
    let mut point = Point::default();

    for (i, field) in batch.schema().fields().iter().enumerate() {
        let array = batch.column(i);
        let column_type = ColumnType::from(
            field.metadata()
                .get("iox::column::type")
                .map(String::as_str)
                .unwrap_or(""),
        );
        let value = get_arrow_value(array.as_ref(), field, row)?;
        let name = field.name();
        
        if matches!(field.data_type(), DataType::Utf8) && (name == "measurement" || name == "iox::measurement") {
            if let Some(v) = value.get_value()? {
                point.set_measurement(v);
                continue;
            }
        }

        match column_type {
            ColumnType::Unknown => {
                if matches!(field.data_type(), DataType::Timestamp(_, _)) && name == "time" {
                    if let Some(v) = value.get_value()? {
                        point.set_timestamp(v);
                    }
                } else {
                    point.set_field(name, value);
                }
            },
            ColumnType::Tag => {
                if let Some(v) = value.get_value()? {
                    point.set_tag(name, v);
                }
            },
            ColumnType::Timestamp => {
                if let Some(v) = value.get_value()? {
                    point.set_timestamp(v);
                }
            },
            _ => {
                point.set_field(name, value);
            }
        }
    }

    Ok(point)
}

#[derive(Debug)]
pub struct PointStream {
    inner: FlightRecordBatchStream,
    batch_buffer: Option<RecordBatch>,
    i: usize,
    len: usize,
}

impl PointStream {
    pub fn new(inner: FlightRecordBatchStream) -> Self {
        Self { inner, batch_buffer: None, i: 0, len: 0 }
    }
}

impl Stream for PointStream {
    type Item = Result<Point, InfluxDBError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            if let Some(ref buffer) = self.batch_buffer {
                if self.i < self.len {
                    let result = get_point(buffer, self.i);
                    self.i += 1;
                    return std::task::Poll::Ready(Some(result));
                } else {
                    self.batch_buffer = None;
                    self.i = 0;
                    self.len = 0;
                }
            }

            match futures::ready!(self.inner.poll_next_unpin(cx)) {
                Some(batch) => {
                    let batch = batch?;
                    self.len = batch.num_rows();
                    self.batch_buffer = Some(batch);
                }
                None => return std::task::Poll::Ready(None),
            }
        }
    }
}