use super::{Error, FailedToBuildRecordBatchSnafu, Result};
use crate::sql::arrow_sql_gen::trino::schema::trino_data_type_to_arrow_type;
use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
        Decimal256Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
        Int64Builder, Int8Builder, LargeStringBuilder, ListBuilder, MapBuilder, NullBuilder,
        RecordBatch, StringBuilder, StructBuilder, Time64NanosecondBuilder,
        TimestampMicrosecondBuilder,
    },
    datatypes::{i256, DataType, Date32Type, Field, Fields, Schema, TimeUnit},
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
        DataType::List(field) => {
            let values_array = create_empty_array(field.data_type());
            let values_builder: Box<dyn ArrayBuilder> = Box::new(StringBuilder::new());
            Arc::new(ListBuilder::new(values_builder).finish())
        }
        DataType::Struct(fields) => {
            let arrays: Vec<ArrayRef> = fields
                .iter()
                .map(|field| create_empty_array(field.data_type()))
                .collect();
            Arc::new(arrow::array::StructArray::try_new(fields.clone(), arrays, None).unwrap())
        }
        DataType::Map(field, _) => {
            if let DataType::Struct(struct_fields) = field.data_type() {
                if struct_fields.len() == 2 {
                    let key_builder: Box<dyn ArrayBuilder> = Box::new(StringBuilder::new());
                    let value_builder: Box<dyn ArrayBuilder> = Box::new(StringBuilder::new());
                    Arc::new(MapBuilder::new(None, key_builder, value_builder).finish())
                } else {
                    Arc::new(StringBuilder::new().finish())
                }
            } else {
                Arc::new(StringBuilder::new().finish())
            }
        }
        DataType::Null => Arc::new(NullBuilder::new().finish()),
        _ => {
            // Fallback to string for unsupported types
            Arc::new(StringBuilder::new().finish())
        }
    }
}

type BuilderMap = HashMap<String, Box<dyn ArrayBuilder>>;

fn create_builders(schema: &Schema, capacity: usize) -> Result<BuilderMap> {
    let mut builders: BuilderMap = HashMap::new();

    for field in schema.fields() {
        let builder: Box<dyn ArrayBuilder> = create_arrow_builder_for_field(field, capacity)?;
        builders.insert(field.name().clone(), builder);
    }

    Ok(builders)
}

fn create_arrow_builder_for_field(field: &Field, capacity: usize) -> Result<Box<dyn ArrayBuilder>> {
    match field.data_type() {
        DataType::Boolean => Ok(Box::new(BooleanBuilder::with_capacity(capacity))),
        DataType::Int8 => Ok(Box::new(Int8Builder::with_capacity(capacity))),
        DataType::Int16 => Ok(Box::new(Int16Builder::with_capacity(capacity))),
        DataType::Int32 => Ok(Box::new(Int32Builder::with_capacity(capacity))),
        DataType::Int64 => Ok(Box::new(Int64Builder::with_capacity(capacity))),
        DataType::Float32 => Ok(Box::new(Float32Builder::with_capacity(capacity))),
        DataType::Float64 => Ok(Box::new(Float64Builder::with_capacity(capacity))),
        DataType::Utf8 => Ok(Box::new(StringBuilder::with_capacity(capacity, 1024))),
        DataType::LargeUtf8 => Ok(Box::new(LargeStringBuilder::with_capacity(capacity, 1024))),
        DataType::Binary => Ok(Box::new(BinaryBuilder::with_capacity(capacity, 1024))),
        DataType::Date32 => Ok(Box::new(Date32Builder::with_capacity(capacity))),
        DataType::Time64(TimeUnit::Nanosecond) => {
            Ok(Box::new(Time64NanosecondBuilder::with_capacity(capacity)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(Box::new(
            TimestampMicrosecondBuilder::with_capacity(capacity),
        )),
        DataType::Decimal128(precision, scale) => {
            let builder = Decimal128Builder::with_capacity(capacity)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
            Ok(Box::new(builder))
        }
        DataType::Decimal256(precision, scale) => {
            let builder = Decimal256Builder::with_capacity(capacity)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
            Ok(Box::new(builder))
        }
        DataType::List(field) => {
            let values_builder = create_arrow_builder_for_field(field, capacity * 4)?;
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Struct(fields) => {
            let mut field_builders = Vec::new();
            for field in fields {
                field_builders.push(create_arrow_builder_for_field(field, capacity)?);
            }
            Ok(Box::new(StructBuilder::new(fields.clone(), field_builders)))
        }
        DataType::Map(field, _) => {
            if let DataType::Struct(struct_fields) = field.data_type() {
                if struct_fields.len() == 2 {
                    let key_builder = create_arrow_builder_for_field(&struct_fields[0], capacity)?;
                    let value_builder =
                        create_arrow_builder_for_field(&struct_fields[1], capacity)?;
                    Ok(Box::new(MapBuilder::new(None, key_builder, value_builder)))
                } else {
                    // Fallback to string for invalid map structure
                    Ok(Box::new(StringBuilder::with_capacity(capacity, 1024)))
                }
            } else {
                // Fallback to string for invalid map structure
                Ok(Box::new(StringBuilder::with_capacity(capacity, 1024)))
            }
        }
        DataType::Null => Ok(Box::new(NullBuilder::new())),
        _ => {
            // Fallback to string for unsupported types
            Ok(Box::new(StringBuilder::with_capacity(capacity, 1024)))
        }
    }
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
            append_value_to_builder(builder.as_mut(), value, field.data_type())?;
        }
    }
    Ok(())
}

fn append_value_to_builder(
    builder: &mut dyn ArrayBuilder,
    value: Option<&Value>,
    data_type: &DataType,
) -> Result<()> {
    match data_type {
        DataType::Boolean => {
            let bool_builder = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "BooleanBuilder".to_string(),
                })?;
            match value {
                Some(v) if v.is_null() => bool_builder.append_null(),
                Some(Value::Bool(b)) => bool_builder.append_value(*b),
                Some(_) => bool_builder.append_null(),
                None => bool_builder.append_null(),
            }
        }
        DataType::Int8 => {
            let int_builder = builder
                .as_any_mut()
                .downcast_mut::<Int8Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int8Builder".to_string(),
                })?;
            append_int8_value(int_builder, value);
        }
        DataType::Int16 => {
            let int_builder = builder
                .as_any_mut()
                .downcast_mut::<Int16Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int16Builder".to_string(),
                })?;
            append_int16_value(int_builder, value);
        }
        DataType::Int32 => {
            let int_builder = builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int32Builder".to_string(),
                })?;
            append_int32_value(int_builder, value);
        }
        DataType::Int64 => {
            let int_builder = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int64Builder".to_string(),
                })?;
            append_int64_value(int_builder, value);
        }
        DataType::Float32 => {
            let float_builder = builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Float32Builder".to_string(),
                })?;
            append_float32_value(float_builder, value);
        }
        DataType::Float64 => {
            let float_builder = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Float64Builder".to_string(),
                })?;
            append_float64_value(float_builder, value);
        }
        DataType::Utf8 => {
            let string_builder = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "StringBuilder".to_string(),
                })?;
            append_string_value(string_builder, value);
        }
        DataType::LargeUtf8 => {
            let large_string_builder = builder
                .as_any_mut()
                .downcast_mut::<LargeStringBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "LargeStringBuilder".to_string(),
                })?;
            append_large_string_value(large_string_builder, value);
        }
        DataType::Binary => {
            let binary_builder = builder
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "BinaryBuilder".to_string(),
                })?;
            append_binary_value(binary_builder, value)?;
        }
        DataType::Date32 => {
            let date_builder = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Date32Builder".to_string(),
                })?;
            append_date32_value(date_builder, value)?;
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let time_builder = builder
                .as_any_mut()
                .downcast_mut::<Time64NanosecondBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Time64NanosecondBuilder".to_string(),
                })?;
            append_time64_value(time_builder, value)?;
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let timestamp_builder = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "TimestampMicrosecondBuilder".to_string(),
                })?;
            append_timestamp_value(timestamp_builder, value)?;
        }
        DataType::Decimal128(_, _) => {
            let decimal_builder = builder
                .as_any_mut()
                .downcast_mut::<Decimal128Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Decimal128Builder".to_string(),
                })?;
            append_decimal128_value(decimal_builder, value)?;
        }
        DataType::Decimal256(_, _) => {
            let decimal_builder = builder
                .as_any_mut()
                .downcast_mut::<Decimal256Builder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Decimal256Builder".to_string(),
                })?;
            append_decimal256_value(decimal_builder, value)?;
        }
        DataType::List(_) => {
            append_list_value(builder, value)?;
        }
        DataType::Struct(fields) => {
            let struct_builder = builder
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "StructBuilder".to_string(),
                })?;
            append_struct_value(struct_builder, value, fields)?;
        }
        DataType::Map(_, _) => {
            append_map_value(builder, value)?;
        }
        DataType::Null => {
            let null_builder = builder
                .as_any_mut()
                .downcast_mut::<NullBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "NullBuilder".to_string(),
                })?;
            null_builder.append_null();
        }
        _ => {
            // Fallback to string for unsupported types
            let string_builder = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "StringBuilder (fallback)".to_string(),
                })?;
            append_string_value(string_builder, value);
        }
    }
    Ok(())
}

fn append_int8_value(builder: &mut Int8Builder, value: Option<&Value>) {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::Number(n)) if n.is_i64() => {
            if let Some(i) = n.as_i64() {
                if i >= i8::MIN as i64 && i <= i8::MAX as i64 {
                    builder.append_value(i as i8);
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_null();
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
}

fn append_int16_value(builder: &mut Int16Builder, value: Option<&Value>) {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::Number(n)) if n.is_i64() => {
            if let Some(i) = n.as_i64() {
                if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
                    builder.append_value(i as i16);
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_null();
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
}

fn append_int32_value(builder: &mut Int32Builder, value: Option<&Value>) {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::Number(n)) if n.is_i64() => {
            if let Some(i) = n.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    builder.append_value(i as i32);
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_null();
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
}

fn append_int64_value(builder: &mut Int64Builder, value: Option<&Value>) {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::Number(n)) if n.is_i64() => {
            if let Some(i) = n.as_i64() {
                builder.append_value(i);
            } else {
                builder.append_null();
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
}

fn append_float32_value(builder: &mut Float32Builder, value: Option<&Value>) {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::Number(n)) if n.is_f64() => {
            if let Some(f) = n.as_f64() {
                builder.append_value(f as f32);
            } else {
                builder.append_null();
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
}

fn append_float64_value(builder: &mut Float64Builder, value: Option<&Value>) {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::Number(n)) if n.is_f64() => {
            if let Some(f) = n.as_f64() {
                builder.append_value(f);
            } else {
                builder.append_null();
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
}

fn append_string_value(builder: &mut StringBuilder, value: Option<&Value>) {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(s)) => builder.append_value(s),
        Some(other) => {
            let str_val = serde_json::to_string(other).unwrap_or_default();
            builder.append_value(&str_val);
        }
        None => builder.append_null(),
    }
}

fn append_large_string_value(builder: &mut LargeStringBuilder, value: Option<&Value>) {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(s)) => builder.append_value(s),
        Some(other) => {
            let str_val = serde_json::to_string(other).unwrap_or_default();
            builder.append_value(&str_val);
        }
        None => builder.append_null(),
    }
}

fn append_binary_value(builder: &mut BinaryBuilder, value: Option<&Value>) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(s)) => {
            // Try to decode as base64, fallback to raw bytes
            if let Ok(bytes) = base64::decode(s) {
                builder.append_value(bytes);
            } else {
                builder.append_value(s.as_bytes());
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
    Ok(())
}

fn append_date32_value(builder: &mut Date32Builder, value: Option<&Value>) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(date_str)) => {
            if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                builder.append_value(Date32Type::from_naive_date(date));
            } else {
                return Err(Error::InvalidDateValue {
                    value: date_str.to_string(),
                });
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
    Ok(())
}

fn append_time64_value(builder: &mut Time64NanosecondBuilder, value: Option<&Value>) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(time_str)) => {
            if let Ok(time) = NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f") {
                let nanos = i64::from(time.num_seconds_from_midnight()) * 1_000_000_000
                    + i64::from(time.nanosecond());
                builder.append_value(nanos);
            } else {
                return Err(Error::InvalidTimeValue {
                    value: time_str.to_string(),
                });
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
    Ok(())
}

fn append_timestamp_value(
    builder: &mut TimestampMicrosecondBuilder,
    value: Option<&Value>,
) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(timestamp_str)) => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(timestamp_str) {
                builder.append_value(dt.timestamp_micros());
            } else if let Ok(dt) =
                chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.f")
            {
                builder.append_value(dt.and_utc().timestamp_micros());
            } else {
                return Err(Error::InvalidTimestampValue {
                    value: timestamp_str.to_string(),
                });
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
    Ok(())
}

fn append_decimal128_value(builder: &mut Decimal128Builder, value: Option<&Value>) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(decimal_str)) => {
            if let Ok(big_decimal) = decimal_str.parse::<BigDecimal>() {
                if let Some(decimal_value) = big_decimal.to_i128() {
                    builder.append_value(decimal_value);
                } else {
                    builder.append_null();
                }
            } else {
                return Err(Error::FailedToParseDecimal {
                    value: decimal_str.to_string(),
                });
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
    Ok(())
}

fn append_decimal256_value(builder: &mut Decimal256Builder, value: Option<&Value>) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(decimal_str)) => {
            if let Ok(big_decimal) = decimal_str.parse::<BigDecimal>() {
                let decimal_value = to_decimal_256(&big_decimal);
                builder.append_value(decimal_value);
            } else {
                return Err(Error::FailedToParseDecimal {
                    value: decimal_str.to_string(),
                });
            }
        }
        Some(_) => builder.append_null(),
        None => builder.append_null(),
    }
    Ok(())
}

fn append_list_value(builder: &mut dyn ArrayBuilder, value: Option<&Value>) -> Result<()> {
    match value {
        Some(v) if v.is_null() => {
            // We need to figure out what type of list builder this is
            // For now, let's assume it's a string list (most common case)
            if let Some(list_builder) = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<StringBuilder>>()
            {
                list_builder.append_null();
            }
        }
        Some(Value::Array(arr)) => {
            if let Some(list_builder) = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<StringBuilder>>()
            {
                for item in arr {
                    match item {
                        Value::String(s) => list_builder.values().append_value(s),
                        other => list_builder
                            .values()
                            .append_value(&serde_json::to_string(other).unwrap_or_default()),
                    }
                }
                list_builder.append(true);
            }
        }
        Some(_) => {
            if let Some(list_builder) = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<StringBuilder>>()
            {
                list_builder.append_null();
            }
        }
        None => {
            if let Some(list_builder) = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<StringBuilder>>()
            {
                list_builder.append_null();
            }
        }
    }
    Ok(())
}

fn append_map_value(builder: &mut dyn ArrayBuilder, value: Option<&Value>) -> Result<()> {
    // Similar to list, this needs type-specific handling
    // For now, we'll handle the most common case
    match value {
        Some(v) if v.is_null() => {
            // Try to downcast to common map types
            if let Some(map_builder) = builder
                .as_any_mut()
                .downcast_mut::<MapBuilder<StringBuilder, StringBuilder>>()
            {
                map_builder
                    .append(false)
                    .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
            }
        }
        Some(Value::Object(map)) => {
            if let Some(map_builder) = builder
                .as_any_mut()
                .downcast_mut::<MapBuilder<StringBuilder, StringBuilder>>()
            {
                for (key, val) in map {
                    map_builder.keys().append_value(key);
                    match val {
                        Value::String(s) => map_builder.values().append_value(s),
                        other => map_builder
                            .values()
                            .append_value(&serde_json::to_string(other).unwrap_or_default()),
                    }
                }
                map_builder
                    .append(true)
                    .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
            }
        }
        Some(Value::Array(arr)) => {
            if let Some(map_builder) = builder
                .as_any_mut()
                .downcast_mut::<MapBuilder<StringBuilder, StringBuilder>>()
            {
                for item in arr {
                    if let Value::Object(kv_pair) = item {
                        if kv_pair.len() == 2 {
                            let mut iter = kv_pair.iter();
                            if let (Some((_, key_val)), Some((_, value_val))) =
                                (iter.next(), iter.next())
                            {
                                match key_val {
                                    Value::String(k) => map_builder.keys().append_value(k),
                                    other => map_builder.keys().append_value(
                                        &serde_json::to_string(other).unwrap_or_default(),
                                    ),
                                }
                                match value_val {
                                    Value::String(v) => map_builder.values().append_value(v),
                                    other => map_builder.values().append_value(
                                        &serde_json::to_string(other).unwrap_or_default(),
                                    ),
                                }
                            }
                        }
                    }
                }
                map_builder
                    .append(true)
                    .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
            }
        }
        Some(_) => {
            if let Some(map_builder) = builder
                .as_any_mut()
                .downcast_mut::<MapBuilder<StringBuilder, StringBuilder>>()
            {
                map_builder
                    .append(false)
                    .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
            }
        }
        None => {
            if let Some(map_builder) = builder
                .as_any_mut()
                .downcast_mut::<MapBuilder<StringBuilder, StringBuilder>>()
            {
                map_builder
                    .append(false)
                    .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
            }
        }
    }
    Ok(())
}

fn append_null_to_any_builder(builder: &mut dyn ArrayBuilder) {
    // Try to append null to various builder types
    if let Some(bool_builder) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
        bool_builder.append_null();
    } else if let Some(i8_builder) = builder.as_any_mut().downcast_mut::<Int8Builder>() {
        i8_builder.append_null();
    } else if let Some(i16_builder) = builder.as_any_mut().downcast_mut::<Int16Builder>() {
        i16_builder.append_null();
    } else if let Some(i32_builder) = builder.as_any_mut().downcast_mut::<Int32Builder>() {
        i32_builder.append_null();
    } else if let Some(i64_builder) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
        i64_builder.append_null();
    } else if let Some(f32_builder) = builder.as_any_mut().downcast_mut::<Float32Builder>() {
        f32_builder.append_null();
    } else if let Some(f64_builder) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
        f64_builder.append_null();
    } else if let Some(string_builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
        string_builder.append_null();
    } else if let Some(large_string_builder) =
        builder.as_any_mut().downcast_mut::<LargeStringBuilder>()
    {
        large_string_builder.append_null();
    } else if let Some(binary_builder) = builder.as_any_mut().downcast_mut::<BinaryBuilder>() {
        binary_builder.append_null();
    } else if let Some(date_builder) = builder.as_any_mut().downcast_mut::<Date32Builder>() {
        date_builder.append_null();
    } else if let Some(time_builder) = builder
        .as_any_mut()
        .downcast_mut::<Time64NanosecondBuilder>()
    {
        time_builder.append_null();
    } else if let Some(timestamp_builder) = builder
        .as_any_mut()
        .downcast_mut::<TimestampMicrosecondBuilder>()
    {
        timestamp_builder.append_null();
    } else if let Some(decimal128_builder) =
        builder.as_any_mut().downcast_mut::<Decimal128Builder>()
    {
        decimal128_builder.append_null();
    } else if let Some(decimal256_builder) =
        builder.as_any_mut().downcast_mut::<Decimal256Builder>()
    {
        decimal256_builder.append_null();
    } else if let Some(null_builder) = builder.as_any_mut().downcast_mut::<NullBuilder>() {
        null_builder.append_null();
    }
    // Add more types as needed
}

fn finish_builders(mut builders: BuilderMap, schema: &Schema) -> Result<Vec<ArrayRef>> {
    let mut arrays = Vec::new();

    for field in schema.fields() {
        let field_name = field.name();
        if let Some(mut builder) = builders.remove(field_name) {
            arrays.push(builder.finish());
        } else {
            return Err(Error::FailedToFindFieldInSchema {
                column_name: field_name.to_string(),
            });
        }
    }

    Ok(arrays)
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

fn append_struct_value_with_fields(
    builder: &mut StructBuilder,
    value: Option<&Value>,
    fields: &Fields,
) -> Result<()> {
    match value {
        Some(v) if v.is_null() => {
            // Append null to each field
            for (i, field) in fields.iter().enumerate() {
                append_to_struct_field_builder(builder, i, None, field.data_type())?;
            }
            builder.append_null();
        }
        Some(Value::Object(obj)) => {
            // Append values by field name
            for (i, field) in fields.iter().enumerate() {
                let field_value = obj.get(field.name());
                append_to_struct_field_builder(builder, i, field_value, field.data_type())?;
            }
            builder.append(true);
        }
        Some(Value::Array(arr)) => {
            // Append values by position
            for (i, field) in fields.iter().enumerate() {
                let field_value = arr.get(i);
                append_to_struct_field_builder(builder, i, field_value, field.data_type())?;
            }
            builder.append(true);
        }
        Some(_) => {
            // Invalid struct format, append nulls to all fields
            for (i, field) in fields.iter().enumerate() {
                append_to_struct_field_builder(builder, i, None, field.data_type())?;
            }
            builder.append_null();
        }
        None => {
            // Append null to each field
            for (i, field) in fields.iter().enumerate() {
                append_to_struct_field_builder(builder, i, None, field.data_type())?;
            }
            builder.append_null();
        }
    }
    Ok(())
}

fn append_to_struct_field_builder(
    builder: &mut StructBuilder,
    field_index: usize,
    value: Option<&Value>,
    field_data_type: &DataType,
) -> Result<()> {
    match field_data_type {
        DataType::Boolean => {
            let field_builder = builder
                .field_builder::<BooleanBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "BooleanBuilder".to_string(),
                })?;
            match value {
                Some(v) if v.is_null() => field_builder.append_null(),
                Some(Value::Bool(b)) => field_builder.append_value(*b),
                _ => field_builder.append_null(),
            }
        }
        DataType::Int8 => {
            let field_builder = builder
                .field_builder::<Int8Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int8Builder".to_string(),
                })?;
            append_int8_value(field_builder, value);
        }
        DataType::Int16 => {
            let field_builder = builder
                .field_builder::<Int16Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int16Builder".to_string(),
                })?;
            append_int16_value(field_builder, value);
        }
        DataType::Int32 => {
            let field_builder = builder
                .field_builder::<Int32Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int32Builder".to_string(),
                })?;
            append_int32_value(field_builder, value);
        }
        DataType::Int64 => {
            let field_builder = builder
                .field_builder::<Int64Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Int64Builder".to_string(),
                })?;
            append_int64_value(field_builder, value);
        }
        DataType::Float32 => {
            let field_builder = builder
                .field_builder::<Float32Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Float32Builder".to_string(),
                })?;
            append_float32_value(field_builder, value);
        }
        DataType::Float64 => {
            let field_builder = builder
                .field_builder::<Float64Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Float64Builder".to_string(),
                })?;
            append_float64_value(field_builder, value);
        }
        DataType::Utf8 => {
            let field_builder = builder
                .field_builder::<StringBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "StringBuilder".to_string(),
                })?;
            append_string_value(field_builder, value);
        }
        DataType::LargeUtf8 => {
            let field_builder = builder
                .field_builder::<LargeStringBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "LargeStringBuilder".to_string(),
                })?;
            append_large_string_value(field_builder, value);
        }
        DataType::Binary => {
            let field_builder = builder
                .field_builder::<BinaryBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "BinaryBuilder".to_string(),
                })?;
            append_binary_value(field_builder, value)?;
        }
        DataType::Date32 => {
            let field_builder = builder
                .field_builder::<Date32Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Date32Builder".to_string(),
                })?;
            append_date32_value(field_builder, value)?;
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let field_builder = builder
                .field_builder::<Time64NanosecondBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Time64NanosecondBuilder".to_string(),
                })?;
            append_time64_value(field_builder, value)?;
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let field_builder = builder
                .field_builder::<TimestampMicrosecondBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "TimestampMicrosecondBuilder".to_string(),
                })?;
            append_timestamp_value(field_builder, value)?;
        }
        DataType::Decimal128(_, _) => {
            let field_builder = builder
                .field_builder::<Decimal128Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Decimal128Builder".to_string(),
                })?;
            append_decimal128_value(field_builder, value)?;
        }
        DataType::Decimal256(_, _) => {
            let field_builder = builder
                .field_builder::<Decimal256Builder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Decimal256Builder".to_string(),
                })?;
            append_decimal256_value(field_builder, value)?;
        }
        DataType::Null => {
            let field_builder = builder
                .field_builder::<NullBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "NullBuilder".to_string(),
                })?;
            field_builder.append_null();
        }
        _ => {
            // For unsupported types, fall back to string
            let field_builder = builder
                .field_builder::<StringBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "StringBuilder (fallback)".to_string(),
                })?;
            append_string_value(field_builder, value);
        }
    }
    Ok(())
}

fn append_struct_value(
    builder: &mut StructBuilder,
    value: Option<&Value>,
    fields: &Fields,
) -> Result<()> {
    match value {
        Some(v) if v.is_null() => {
            // Append null to each field
            for (i, field) in fields.iter().enumerate() {
                append_to_struct_field_builder(builder, i, None, field.data_type())?;
            }
            builder.append_null();
        }
        Some(Value::Object(obj)) => {
            // Append values by field name
            for (i, field) in fields.iter().enumerate() {
                let field_value = obj.get(field.name());
                append_to_struct_field_builder(builder, i, field_value, field.data_type())?;
            }
            builder.append(true);
        }
        Some(Value::Array(arr)) => {
            // Append values by position
            for (i, field) in fields.iter().enumerate() {
                let field_value = arr.get(i);
                append_to_struct_field_builder(builder, i, field_value, field.data_type())?;
            }
            builder.append(true);
        }
        Some(_) => {
            // Invalid struct format, append nulls to all fields
            for (i, field) in fields.iter().enumerate() {
                append_to_struct_field_builder(builder, i, None, field.data_type())?;
            }
            builder.append_null();
        }
        None => {
            // Append null to each field
            for (i, field) in fields.iter().enumerate() {
                append_to_struct_field_builder(builder, i, None, field.data_type())?;
            }
            builder.append_null();
        }
    }
    Ok(())
}
