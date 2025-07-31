use super::{Error, FailedToBuildRecordBatchSnafu, Result};
use crate::sql::arrow_sql_gen::trino::schema::trino_data_type_to_arrow_type;
use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
        Decimal256Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
        Int64Builder, Int8Builder, LargeStringBuilder, ListBuilder, NullBuilder, RecordBatch,
        StringBuilder, Time64NanosecondBuilder, TimestampMicrosecondBuilder,
    },
    datatypes::{i256, DataType, Date32Type, Field, Schema, TimeUnit},
};
use bigdecimal::BigDecimal;
use bigdecimal::ToPrimitive;
use chrono::{NaiveDate, NaiveTime, Timelike};
use serde_json::Value;
use snafu::ResultExt;
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone)]
pub struct TrinoColumn {
    pub name: String,
    pub type_name: String,
}

pub fn rows_to_arrow(rows: &[Vec<Value>], columns: &Vec<TrinoColumn>) -> Result<RecordBatch> {
    if rows.is_empty() {
        if !columns.is_empty() {
            let schema = build_schema_from_columns(&columns)?;
            let empty_arrays: Vec<ArrayRef> = schema
                .fields()
                .iter()
                .map(|field| create_empty_array(field.data_type()))
                .collect();

            return RecordBatch::try_new(Arc::new(schema), empty_arrays)
                .context(FailedToBuildRecordBatchSnafu);
        }
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }

    let schema = build_schema_from_columns(&columns)?;
    let mut builders = create_builders(&schema, rows.len())?;

    for row in rows {
        append_row_to_builders(row, &schema, &mut builders)?;
    }

    let arrays = finish_builders(builders, &schema)?;

    RecordBatch::try_new(Arc::new(schema), arrays).context(FailedToBuildRecordBatchSnafu)
}

fn build_schema_from_columns(columns: &[TrinoColumn]) -> Result<Schema> {
    let mut fields = Vec::new();

    for column in columns {
        let arrow_type = trino_data_type_to_arrow_type(&column.type_name)?;
        fields.push(Field::new(&column.name, arrow_type, true));
    }

    Ok(Schema::new(fields))
}

fn create_empty_array(data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Boolean => Arc::new(BooleanBuilder::new().finish()),
        DataType::Int8 => Arc::new(Int8Builder::new().finish()),
        DataType::Int16 => Arc::new(Int16Builder::new().finish()),
        DataType::Int32 => Arc::new(Int32Builder::new().finish()),
        DataType::Int64 => Arc::new(Int64Builder::new().finish()),
        DataType::Float32 => Arc::new(Float32Builder::new().finish()),
        DataType::Float64 => Arc::new(Float64Builder::new().finish()),
        DataType::Utf8 => Arc::new(StringBuilder::new().finish()),
        DataType::LargeUtf8 => Arc::new(LargeStringBuilder::new().finish()),
        DataType::Binary => Arc::new(BinaryBuilder::new().finish()),
        DataType::Date32 => Arc::new(Date32Builder::new().finish()),
        DataType::Time64(TimeUnit::Nanosecond) => Arc::new(Time64NanosecondBuilder::new().finish()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Arc::new(TimestampMicrosecondBuilder::new().finish())
        }
        DataType::Decimal128(_, _) => Arc::new(Decimal128Builder::new().finish()),
        DataType::Decimal256(_, _) => Arc::new(Decimal256Builder::new().finish()),
        DataType::List(_) => {
            let values_builder = StringBuilder::new();
            Arc::new(ListBuilder::new(values_builder).finish())
        }
        DataType::Null => Arc::new(NullBuilder::new().finish()),
        _ => {
            // Fallback to string for unsupported types
            Arc::new(StringBuilder::new().finish())
        }
    }
}

type BuilderMap = HashMap<String, Box<dyn TrinoArrayBuilderTrait>>;

trait TrinoArrayBuilderTrait {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()>;
    fn finish_builder(self: Box<Self>) -> Result<ArrayRef>;
}

fn create_builders(schema: &Schema, capacity: usize) -> Result<BuilderMap> {
    let mut builders: BuilderMap = HashMap::new();

    for field in schema.fields() {
        let builder: Box<dyn TrinoArrayBuilderTrait> = match field.data_type() {
            DataType::Boolean => Box::new(TrinoBooleanArrayBuilder::new(capacity)),
            DataType::Int8 => Box::new(TrinoInt8ArrayBuilder::new(capacity)),
            DataType::Int16 => Box::new(TrinoInt16ArrayBuilder::new(capacity)),
            DataType::Int32 => Box::new(TrinoInt32ArrayBuilder::new(capacity)),
            DataType::Int64 => Box::new(TrinoInt64ArrayBuilder::new(capacity)),
            DataType::Float32 => Box::new(TrinoFloat32ArrayBuilder::new(capacity)),
            DataType::Float64 => Box::new(TrinoFloat64ArrayBuilder::new(capacity)),
            DataType::Utf8 => Box::new(TrinoStringArrayBuilder::new(capacity)),
            DataType::LargeUtf8 => Box::new(TrinoLargeStringArrayBuilder::new(capacity)),
            DataType::Binary => Box::new(TrinoBinaryArrayBuilder::new(capacity)),
            DataType::Date32 => Box::new(TrinoDate32ArrayBuilder::new(capacity)),
            DataType::Time64(TimeUnit::Nanosecond) => {
                Box::new(TrinoTime64ArrayBuilder::new(capacity))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Box::new(TrinoTimestampArrayBuilder::new(capacity))
            }
            DataType::Decimal128(precision, scale) => Box::new(TrinoDecimal128ArrayBuilder::new(
                capacity, *precision, *scale,
            )?),
            DataType::Decimal256(precision, scale) => Box::new(TrinoDecimal256ArrayBuilder::new(
                capacity, *precision, *scale,
            )?),
            DataType::List(_) => Box::new(TrinoListArrayBuilder::new(capacity)),
            DataType::Null => Box::new(TrinoNullArrayBuilder::new()),
            _ => {
                // Fallback to string for unsupported types
                Box::new(TrinoStringArrayBuilder::new(capacity))
            }
        };

        builders.insert(field.name().clone(), builder);
    }

    Ok(builders)
}

fn append_row_to_builders(
    row: &Vec<Value>,
    schema: &Schema,
    builders: &mut BuilderMap,
) -> Result<()> {
    for (field_idx, field) in schema.fields().iter().enumerate() {
        let field_name = field.name();
        let value = row.get(field_idx);

        if let Some(builder) = builders.get_mut(field_name) {
            builder.append_value(value)?;
        }
    }
    Ok(())
}

fn finish_builders(mut builders: BuilderMap, schema: &Schema) -> Result<Vec<ArrayRef>> {
    let mut arrays = Vec::new();

    for field in schema.fields() {
        let field_name = field.name();
        if let Some(builder) = builders.remove(field_name) {
            arrays.push(builder.finish_builder()?);
        } else {
            return Err(Error::FailedToFindFieldInSchema {
                column_name: field_name.to_string(),
            });
        }
    }

    Ok(arrays)
}

// Builder implementations
struct TrinoBooleanArrayBuilder(BooleanBuilder);
struct TrinoInt8ArrayBuilder(Int8Builder);
struct TrinoInt16ArrayBuilder(Int16Builder);
struct TrinoInt32ArrayBuilder(Int32Builder);
struct TrinoInt64ArrayBuilder(Int64Builder);
struct TrinoFloat32ArrayBuilder(Float32Builder);
struct TrinoFloat64ArrayBuilder(Float64Builder);
struct TrinoStringArrayBuilder(StringBuilder);
struct TrinoLargeStringArrayBuilder(LargeStringBuilder);
struct TrinoBinaryArrayBuilder(BinaryBuilder);
struct TrinoDate32ArrayBuilder(Date32Builder);
struct TrinoTime64ArrayBuilder(Time64NanosecondBuilder);
struct TrinoTimestampArrayBuilder(TimestampMicrosecondBuilder);
struct TrinoDecimal128ArrayBuilder {
    builder: Decimal128Builder,
}
struct TrinoDecimal256ArrayBuilder {
    builder: Decimal256Builder,
}
struct TrinoListArrayBuilder(ListBuilder<StringBuilder>);
struct TrinoNullArrayBuilder(NullBuilder);

impl TrinoBooleanArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(BooleanBuilder::with_capacity(capacity))
    }
}

impl TrinoArrayBuilderTrait for TrinoBooleanArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.0.append_null(),
            Some(Value::Bool(b)) => self.0.append_value(*b),
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl TrinoInt8ArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(Int8Builder::with_capacity(capacity))
    }
}

impl TrinoArrayBuilderTrait for TrinoInt8ArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.0.append_null(),
            Some(Value::Number(n)) if n.is_i64() => {
                if let Some(i) = n.as_i64() {
                    if i >= i8::MIN as i64 && i <= i8::MAX as i64 {
                        self.0.append_value(i as i8);
                    } else {
                        self.0.append_null();
                    }
                } else {
                    self.0.append_null();
                }
            }
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl TrinoInt16ArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(Int16Builder::with_capacity(capacity))
    }
}

impl TrinoArrayBuilderTrait for TrinoInt16ArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.0.append_null(),
            Some(Value::Number(n)) if n.is_i64() => {
                if let Some(i) = n.as_i64() {
                    if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
                        self.0.append_value(i as i16);
                    } else {
                        self.0.append_null();
                    }
                } else {
                    self.0.append_null();
                }
            }
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl TrinoInt32ArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(Int32Builder::with_capacity(capacity))
    }
}

impl TrinoArrayBuilderTrait for TrinoInt32ArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.0.append_null(),
            Some(Value::Number(n)) if n.is_i64() => {
                if let Some(i) = n.as_i64() {
                    if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                        self.0.append_value(i as i32);
                    } else {
                        self.0.append_null();
                    }
                } else {
                    self.0.append_null();
                }
            }
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl TrinoInt64ArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(Int64Builder::with_capacity(capacity))
    }
}

impl TrinoArrayBuilderTrait for TrinoInt64ArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.0.append_null(),
            Some(Value::Number(n)) if n.is_i64() => {
                if let Some(i) = n.as_i64() {
                    self.0.append_value(i);
                } else {
                    self.0.append_null();
                }
            }
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl TrinoFloat32ArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(Float32Builder::with_capacity(capacity))
    }
}

impl TrinoArrayBuilderTrait for TrinoFloat32ArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.0.append_null(),
            Some(Value::Number(n)) if n.is_f64() => {
                if let Some(f) = n.as_f64() {
                    self.0.append_value(f as f32);
                } else {
                    self.0.append_null();
                }
            }
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl TrinoFloat64ArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(Float64Builder::with_capacity(capacity))
    }
}

impl TrinoArrayBuilderTrait for TrinoFloat64ArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.0.append_null(),
            Some(Value::Number(n)) if n.is_f64() => {
                if let Some(f) = n.as_f64() {
                    self.0.append_value(f);
                } else {
                    self.0.append_null();
                }
            }
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl TrinoStringArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(StringBuilder::with_capacity(capacity, 1024))
    }
}

impl TrinoArrayBuilderTrait for TrinoStringArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.0.append_null(),
            Some(Value::String(s)) => self.0.append_value(s),
            Some(other) => {
                // Convert other JSON values to string representation
                let str_val = serde_json::to_string(other).unwrap_or_default();
                self.0.append_value(&str_val);
            }
            None => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl TrinoLargeStringArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(LargeStringBuilder::with_capacity(capacity, 1024))
    }
}

impl TrinoArrayBuilderTrait for TrinoLargeStringArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.0.append_null(),
            Some(Value::String(s)) => self.0.append_value(s),
            Some(other) => {
                let str_val = serde_json::to_string(other).unwrap_or_default();
                self.0.append_value(&str_val);
            }
            None => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl TrinoBinaryArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(BinaryBuilder::with_capacity(capacity, 1024))
    }
}

impl TrinoArrayBuilderTrait for TrinoBinaryArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.0.append_null(),
            Some(Value::String(s)) => {
                // Try to decode as base64, fallback to raw bytes
                if let Ok(bytes) = base64::decode(s) {
                    self.0.append_value(bytes);
                } else {
                    self.0.append_value(s.as_bytes());
                }
            }
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl TrinoDate32ArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(Date32Builder::with_capacity(capacity))
    }
}

impl TrinoArrayBuilderTrait for TrinoDate32ArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.0.append_null(),
            Some(Value::String(date_str)) => {
                if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                    self.0.append_value(Date32Type::from_naive_date(date));
                } else {
                    return Err(Error::InvalidDateValue {
                        value: date_str.to_string(),
                    });
                }
            }
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl TrinoTime64ArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(Time64NanosecondBuilder::with_capacity(capacity))
    }
}

impl TrinoArrayBuilderTrait for TrinoTime64ArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.0.append_null(),
            Some(Value::String(time_str)) => {
                if let Ok(time) = NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f") {
                    let nanos = i64::from(time.num_seconds_from_midnight()) * 1_000_000_000
                        + i64::from(time.nanosecond());
                    self.0.append_value(nanos);
                } else {
                    return Err(Error::InvalidTimeValue {
                        value: time_str.to_string(),
                    });
                }
            }
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl TrinoTimestampArrayBuilder {
    fn new(capacity: usize) -> Self {
        Self(TimestampMicrosecondBuilder::with_capacity(capacity))
    }
}

impl TrinoArrayBuilderTrait for TrinoTimestampArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.0.append_null(),
            Some(Value::String(timestamp_str)) => {
                // Try to parse ISO 8601 format first
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(timestamp_str) {
                    self.0.append_value(dt.timestamp_micros());
                } else if let Ok(dt) =
                    chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.f")
                {
                    self.0.append_value(dt.and_utc().timestamp_micros());
                } else {
                    return Err(Error::InvalidTimestampValue {
                        value: timestamp_str.to_string(),
                    });
                }
            }
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl TrinoDecimal128ArrayBuilder {
    fn new(capacity: usize, precision: u8, scale: i8) -> Result<Self> {
        let builder = Decimal128Builder::with_capacity(capacity)
            .with_precision_and_scale(precision, scale)
            .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
        Ok(Self { builder })
    }
}

impl TrinoArrayBuilderTrait for TrinoDecimal128ArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.builder.append_null(),
            Some(Value::String(decimal_str)) => {
                if let Ok(big_decimal) = decimal_str.parse::<BigDecimal>() {
                    if let Some(decimal_value) = big_decimal.to_i128() {
                        self.builder.append_value(decimal_value);
                    } else {
                        self.builder.append_null();
                    }
                } else {
                    return Err(Error::FailedToParseDecimal {
                        value: decimal_str.to_string(),
                    });
                }
            }
            Some(_) => self.builder.append_null(),
            None => self.builder.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }
}

impl TrinoDecimal256ArrayBuilder {
    fn new(capacity: usize, precision: u8, scale: i8) -> Result<Self> {
        let builder = Decimal256Builder::with_capacity(capacity)
            .with_precision_and_scale(precision, scale)
            .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
        Ok(Self { builder })
    }
}

impl TrinoArrayBuilderTrait for TrinoDecimal256ArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.builder.append_null(),
            Some(Value::String(decimal_str)) => {
                if let Ok(big_decimal) = decimal_str.parse::<BigDecimal>() {
                    let decimal_value = to_decimal_256(&big_decimal);
                    self.builder.append_value(decimal_value);
                } else {
                    return Err(Error::FailedToParseDecimal {
                        value: decimal_str.to_string(),
                    });
                }
            }
            Some(_) => self.builder.append_null(),
            None => self.builder.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }
}

impl TrinoListArrayBuilder {
    fn new(capacity: usize) -> Self {
        let values_builder = StringBuilder::with_capacity(capacity * 4, 256);
        Self(ListBuilder::new(values_builder))
    }
}

impl TrinoArrayBuilderTrait for TrinoListArrayBuilder {
    fn append_value(&mut self, value: Option<&Value>) -> Result<()> {
        match value {
            Some(v) if v.is_null() => self.0.append_null(),
            Some(Value::Array(arr)) => {
                for item in arr {
                    match item {
                        Value::String(s) => self.0.values().append_value(s),
                        other => self
                            .0
                            .values()
                            .append_value(&serde_json::to_string(other).unwrap_or_default()),
                    }
                }
                self.0.append(true);
            }
            Some(_) => self.0.append_null(),
            None => self.0.append_null(),
        }
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

impl TrinoNullArrayBuilder {
    fn new() -> Self {
        Self(NullBuilder::new())
    }
}

impl TrinoArrayBuilderTrait for TrinoNullArrayBuilder {
    fn append_value(&mut self, _value: Option<&Value>) -> Result<()> {
        self.0.append_null();
        Ok(())
    }

    fn finish_builder(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

fn to_decimal_256(decimal: &BigDecimal) -> i256 {
    let (bigint_value, _) = decimal.as_bigint_and_exponent();
    let mut bigint_bytes = bigint_value.to_signed_bytes_le();

    let is_negative = bigint_value.sign() == num_bigint::Sign::Minus;
    let fill_byte = if is_negative { 0xFF } else { 0x00 };

    if bigint_bytes.len() > 32 {
        bigint_bytes.truncate(32);
    } else {
        bigint_bytes.resize(32, fill_byte);
    };

    let mut array = [0u8; 32];
    array.copy_from_slice(&bigint_bytes);

    i256::from_le_bytes(array)
}
