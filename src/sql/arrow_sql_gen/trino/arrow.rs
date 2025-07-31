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
use std::any::Any;
use arrow_schema::ArrowError;

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
        DataType::Decimal128(precision, scale) => Arc::new(Decimal128Builder::new().finish()),
        DataType::Decimal256(_, _) => Arc::new(Decimal256Builder::new().finish()),
        DataType::List(_) => {
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
            let builder = Decimal128BuilderWrapper::new(capacity, *precision, *scale)
                .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
            Ok(Box::new(builder))
        }
        DataType::Decimal256(precision, scale) => {
            let builder = Decimal256BuilderWrapper::new(capacity, *precision, *scale)
                .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
            Ok(Box::new(builder))
        }
        DataType::List(field) => create_list_builder_for_field(field, capacity),
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

struct Decimal128BuilderWrapper {
    inner: Box<Decimal128Builder>,
    precision: u8,
    scale: i8,
}

impl Decimal128BuilderWrapper {
    fn new(capacity: usize, precision: u8, scale: i8) -> std::result::Result<Self, ArrowError> {
        let inner = Decimal128Builder::with_capacity(capacity)
            .with_precision_and_scale(precision, scale)?;

        Ok(Self {
            inner: Box::new(inner),
            precision,
            scale,
        })
    }

    fn append_value(&mut self, value: i128) {
        self.inner.append_value(value);
    }

    fn append_null(&mut self) {
        self.inner.append_null();
    }

    fn precision(&self) -> u8 {
        self.precision
    }

    fn scale(&self) -> i8 {
        self.scale
    }

    fn data_type(&self) -> DataType {
        DataType::Decimal128(self.precision, self.scale)
    }
}

impl ArrayBuilder for Decimal128BuilderWrapper {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.inner.finish())
    }

    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.inner.finish_cloned())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

struct Decimal256BuilderWrapper {
    inner: Box<Decimal256Builder>,
    precision: u8,
    scale: i8,
}

impl Decimal256BuilderWrapper {
    fn new(capacity: usize, precision: u8, scale: i8) -> std::result::Result<Self, ArrowError> {
        let inner = Decimal256Builder::with_capacity(capacity)
            .with_precision_and_scale(precision, scale)?;

        Ok(Self {
            inner: Box::new(inner),
            precision,
            scale,
        })
    }

    fn append_value(&mut self, value: i256) {
        self.inner.append_value(value);
    }

    fn append_null(&mut self) {
        self.inner.append_null();
    }

    fn precision(&self) -> u8 {
        self.precision
    }

    fn scale(&self) -> i8 {
        self.scale
    }

    fn data_type(&self) -> DataType {
        DataType::Decimal256(self.precision, self.scale)
    }
}

impl ArrayBuilder for Decimal256BuilderWrapper {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.inner.finish())
    }

    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.inner.finish_cloned())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

fn create_list_builder_for_field(
    inner_field: &Field,
    capacity: usize,
) -> Result<Box<dyn ArrayBuilder>> {
    // let values_builder = create_arrow_builder_for_field(inner_field, capacity);
    // Ok(Box::new(ListBuilder::new(values_builder)))

    match inner_field.data_type() {
        DataType::Boolean => {
            let values_builder: Box<dyn ArrayBuilder> =
                Box::new(BooleanBuilder::with_capacity(capacity * 4));
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Int8 => {
            let values_builder: Box<dyn ArrayBuilder> =
                Box::new(Int8Builder::with_capacity(capacity * 4));
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Int16 => {
            let values_builder = Int16Builder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Int32 => {
            let values_builder = Int32Builder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Int64 => {
            let values_builder = Int64Builder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Float32 => {
            let values_builder = Float32Builder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Float64 => {
            let values_builder = Float64Builder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Utf8 => {
            let values_builder = StringBuilder::with_capacity(capacity * 4, 1024);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::LargeUtf8 => {
            let values_builder = LargeStringBuilder::with_capacity(capacity * 4, 1024);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Binary => {
            let values_builder = BinaryBuilder::with_capacity(capacity * 4, 1024);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Date32 => {
            let values_builder = Date32Builder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let values_builder = Time64NanosecondBuilder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let values_builder = TimestampMicrosecondBuilder::with_capacity(capacity * 4);
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Decimal128(precision, scale) => {
            let values_builder = Decimal128BuilderWrapper::new(capacity * 4, *precision, *scale)
                .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Decimal256(precision, scale) => {
            let values_builder = Decimal256BuilderWrapper::new(capacity * 4, *precision, *scale)
                .map_err(|e| Error::FailedToBuildRecordBatch { source: e })?;
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        DataType::Null => {
            let values_builder = NullBuilder::new();
            Ok(Box::new(ListBuilder::new(values_builder)))
        }
        _ => {
            // Fallback to string for unsupported inner types
            let values_builder = StringBuilder::with_capacity(capacity * 4, 1024);
            Ok(Box::new(ListBuilder::new(values_builder)))
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
                .downcast_mut::<Decimal128BuilderWrapper>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Decimal128BuilderWrapper".to_string(),
                })?;
            append_decimal128_value(decimal_builder, value)?;
        }
        DataType::Decimal256(_, _) => {
            let decimal_builder = builder
                .as_any_mut()
                .downcast_mut::<Decimal256BuilderWrapper>()
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Decimal256BuilderWrapper".to_string(),
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

fn append_decimal128_value(builder: &mut Decimal128BuilderWrapper, value: Option<&Value>) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(decimal_str)) => {
            if let Ok(big_decimal) = decimal_str.parse::<BigDecimal>() {
                // Get precision and scale from the builder's data type
                if let DataType::Decimal128(_, scale) = builder.data_type() {
                    let scale_factor = BigDecimal::from(10_i128.pow(scale as u32));
                    let scaled_decimal = big_decimal * scale_factor;

                    if let Some(decimal_value) = scaled_decimal.to_i128() {
                        builder.append_value(decimal_value);
                    } else {
                        builder.append_null();
                    }
                } else {
                    // Fallback - use the original value
                    if let Some(decimal_value) = big_decimal.to_i128() {
                        builder.append_value(decimal_value);
                    } else {
                        builder.append_null();
                    }
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

fn append_decimal256_value(builder: &mut Decimal256BuilderWrapper, value: Option<&Value>) -> Result<()> {
    match value {
        Some(v) if v.is_null() => builder.append_null(),
        Some(Value::String(decimal_str)) => {
            if let Ok(big_decimal) = decimal_str.parse::<BigDecimal>() {
                // Get precision and scale from the builder's data type
                if let DataType::Decimal256(_, scale) = builder.data_type() {
                    let scale_factor = BigDecimal::from(10_i128.pow(scale as u32));
                    let scaled_decimal = big_decimal * scale_factor;
                    builder.append_value(to_decimal_256(&scaled_decimal));
                } else {
                    // Fallback - use the original value
                    builder.append_value(to_decimal_256(&big_decimal));
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

fn append_list_value(builder: &mut dyn ArrayBuilder, value: Option<&Value>) -> Result<()> {
    match value {
        Some(v) if v.is_null() => {
            append_null_to_list_builder(builder)?;
        }
        Some(Value::Array(arr)) => {
            append_array_to_list_builder(builder, arr)?;
        }
        Some(_) => {
            append_null_to_list_builder(builder)?;
        }
        None => {
            append_null_to_list_builder(builder)?;
        }
    }
    Ok(())
}

fn append_null_to_list_builder(builder: &mut dyn ArrayBuilder) -> Result<()> {
    // Try to downcast to various list builder types
    if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<StringBuilder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<LargeStringBuilder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<BooleanBuilder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int8Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int16Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int32Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int64Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Float32Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Float64Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<BinaryBuilder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Date32Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Time64NanosecondBuilder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<TimestampMicrosecondBuilder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Decimal128Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Decimal256Builder>>()
    {
        list_builder.append_null();
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<NullBuilder>>()
    {
        list_builder.append_null();
    } else {
        return Err(Error::BuilderDowncastError {
            expected: "ListBuilder<T>".to_string(),
        });
    }
    Ok(())
}

fn append_array_to_list_builder(builder: &mut dyn ArrayBuilder, arr: &Vec<Value>) -> Result<()> {
    // Handle different list builder types
    if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<StringBuilder>>()
    {
        for item in arr {
            append_string_value(list_builder.values(), Some(item));
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<LargeStringBuilder>>()
    {
        for item in arr {
            append_large_string_value(list_builder.values(), Some(item));
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<BooleanBuilder>>()
    {
        for item in arr {
            match item {
                Value::Bool(b) => list_builder.values().append_value(*b),
                Value::Null => list_builder.values().append_null(),
                _ => list_builder.values().append_null(),
            }
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int8Builder>>()
    {
        for item in arr {
            append_int8_value(list_builder.values(), Some(item));
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int16Builder>>()
    {
        for item in arr {
            append_int16_value(list_builder.values(), Some(item));
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int32Builder>>()
    {
        for item in arr {
            append_int32_value(list_builder.values(), Some(item));
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Int64Builder>>()
    {
        for item in arr {
            append_int64_value(list_builder.values(), Some(item));
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Float32Builder>>()
    {
        for item in arr {
            append_float32_value(list_builder.values(), Some(item));
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Float64Builder>>()
    {
        for item in arr {
            append_float64_value(list_builder.values(), Some(item));
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<BinaryBuilder>>()
    {
        for item in arr {
            append_binary_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Date32Builder>>()
    {
        for item in arr {
            append_date32_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Time64NanosecondBuilder>>()
    {
        for item in arr {
            append_time64_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<TimestampMicrosecondBuilder>>()
    {
        for item in arr {
            append_timestamp_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Decimal128BuilderWrapper>>()
    {
        for item in arr {
            append_decimal128_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Decimal256BuilderWrapper>>()
    {
        for item in arr {
            append_decimal256_value(list_builder.values(), Some(item))?;
        }
        list_builder.append(true);
    } else if let Some(list_builder) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<NullBuilder>>()
    {
        for _ in arr {
            list_builder.values().append_null();
        }
        list_builder.append(true);
    } else {
        return Err(Error::BuilderDowncastError {
            expected: "ListBuilder<T>".to_string(),
        });
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
                .field_builder::<Decimal128BuilderWrapper>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Decimal128BuilderWrapper".to_string(),
                })?;
            append_decimal128_value(field_builder, value)?;
        }
        DataType::Decimal256(_, _) => {
            let field_builder = builder
                .field_builder::<Decimal256BuilderWrapper>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "Decimal256BuilderWrapper".to_string(),
                })?;
            append_decimal256_value(field_builder, value)?;
        }
        // DataType::List(_) => {
        //     // For lists in structs, we need to handle this differently
        //     // Get the list builder and call append_list_value on it
        //     let field_builders = builder.field_builders();
        //     if let Some(list_builder) = field_builders.get_mut(field_index) {
        //         append_list_value(list_builder.as_mut(), value)?;
        //     } else {
        //         return Err(Error::BuilderDowncastError {
        //             expected: format!("ListBuilder at index {}", field_index),
        //         });
        //     }
        // }
        DataType::Struct(nested_fields) => {
            let nested_struct_builder = builder
                .field_builder::<StructBuilder>(field_index)
                .ok_or_else(|| Error::BuilderDowncastError {
                    expected: "StructBuilder".to_string(),
                })?;
            append_struct_value(nested_struct_builder, value, nested_fields)?;
        }
        // DataType::Map(_, _) => {
        //     // For maps in structs, similar approach
        //     let field_builders = builder.field_builders();
        //     if let Some(map_builder) = field_builders.get_mut(field_index) {
        //         append_map_value(map_builder.as_mut(), value)?;
        //     } else {
        //         return Err(Error::BuilderDowncastError {
        //             expected: format!("MapBuilder at index {}", field_index),
        //         });
        //     }
        // }
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



#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use serde_json::{json, Value};

    fn create_test_columns(columns: Vec<(&str, &str)>) -> Vec<TrinoColumn> {
        columns
            .into_iter()
            .map(|(name, type_name)| TrinoColumn {
                name: name.to_string(),
                type_name: type_name.to_string(),
            })
            .collect()
    }

    fn assert_boolean_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<bool>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {}", i);
        }
    }

    fn assert_int8_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<i8>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {}", i);
        }
    }

    fn assert_int16_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<i16>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {}", i);
        }
    }

    fn assert_int32_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<i32>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {}", i);
        }
    }

    fn assert_int64_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<i64>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {}", i);
        }
    }

    fn assert_float32_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<f32>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert!((array.value(i) - expected_value).abs() < f32::EPSILON,
                    "Mismatch at index {}: expected {}, got {}", i, expected_value, array.value(i));
        }
    }

    fn assert_float64_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<f64>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert!((array.value(i) - expected_value).abs() < f64::EPSILON,
                    "Mismatch at index {}: expected {}, got {}", i, expected_value, array.value(i));
        }
    }

    fn assert_string_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<&str>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {}", i);
        }
    }

    fn assert_int32_array_with_nulls(record_batch: &RecordBatch, column_index: usize, expected: Vec<Option<i32>>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            match expected_value {
                Some(val) => {
                    assert!(!array.is_null(i), "Expected non-null at index {}", i);
                    assert_eq!(array.value(i), *val, "Mismatch at index {}", i);
                }
                None => {
                    assert!(array.is_null(i), "Expected null at index {}", i);
                }
            }
        }
    }

    fn assert_string_array_with_nulls(record_batch: &RecordBatch, column_index: usize, expected: Vec<Option<&str>>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            match expected_value {
                Some(val) => {
                    assert!(!array.is_null(i), "Expected non-null at index {}", i);
                    assert_eq!(array.value(i), *val, "Mismatch at index {}", i);
                }
                None => {
                    assert!(array.is_null(i), "Expected null at index {}", i);
                }
            }
        }
    }

    fn assert_date32_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<i32>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {}", i);
        }
    }

    fn assert_time64_nanosecond_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<i64>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Time64NanosecondArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {}", i);
        }
    }

    fn assert_timestamp_microsecond_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<i64>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {}", i);
        }
    }

    fn assert_decimal128_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<i128>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {}", i);
        }
    }

    fn assert_decimal256_array(record_batch: &RecordBatch, column_index: usize, expected: Vec<arrow::datatypes::i256>) {
        let array = record_batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();

        assert_eq!(array.len(), expected.len(), "Array length mismatch");
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(array.value(i), *expected_value, "Mismatch at index {}", i);
        }
    }

    #[test]
    fn test_empty_rows_empty_columns() {
        let rows: Vec<Vec<Value>> = vec![];
        let columns: Vec<TrinoColumn> = vec![];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 0);
    }

    #[test]
    fn test_empty_rows_with_columns() {
        let rows: Vec<Vec<Value>> = vec![];
        let columns = create_test_columns(vec![
            ("id", "bigint"),
            ("name", "varchar"),
            ("active", "boolean"),
        ]);

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 3);

        let schema = result.schema();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "active");
    }

    #[test]
    fn test_basic_data_types() {
        let columns = create_test_columns(vec![
            ("bool_col", "boolean"),
            ("int8_col", "tinyint"),
            ("int16_col", "smallint"),
            ("int32_col", "integer"),
            ("int64_col", "bigint"),
            ("float32_col", "real"),
            ("float64_col", "double"),
            ("string_col", "varchar"),
        ]);

        let rows = vec![
            vec![
                json!(true),
                json!(127),
                json!(32767),
                json!(2147483647),
                json!(9223372036854775807i64),
                json!(3.14f32),
                json!(2.718281828),
                json!("hello"),
            ],
            vec![
                json!(false),
                json!(-128),
                json!(-32768),
                json!(-2147483648),
                json!(-9223372036854775808i64),
                json!(-1.23f32),
                json!(-9.876543210),
                json!("world"),
            ],
        ];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 8);

        assert_boolean_array(&result, 0, vec![true, false]);
        assert_int8_array(&result, 1, vec![127, -128]);
        assert_int16_array(&result, 2, vec![32767, -32768]);
        assert_int32_array(&result, 3, vec![2147483647, -2147483648]);
        assert_int64_array(&result, 4, vec![9223372036854775807i64, -9223372036854775808i64]);
        assert_float32_array(&result, 5, vec![3.14f32, -1.23f32]);
        assert_float64_array(&result, 6, vec![2.718281828, -9.876543210]);
        assert_string_array(&result, 7, vec!["hello", "world"]);

    }

    #[test]
    fn test_null_values() {
        let columns = create_test_columns(vec![
            ("nullable_int", "integer"),
            ("nullable_string", "varchar"),
        ]);

        let rows = vec![
            vec![json!(42), json!("test")],
            vec![Value::Null, Value::Null],
            vec![json!(100), json!("another")],
        ];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 3);

        assert_int32_array_with_nulls(&result, 0, vec![Some(42), None, Some(100)]);
        assert_string_array_with_nulls(&result, 1, vec![Some("test"), None, Some("another")]);
    }

    #[test]
    fn test_date_and_time_types() {
        let columns = create_test_columns(vec![
            ("date_col", "date"),
            ("time_col", "time"),
            ("timestamp_col", "timestamp"),
        ]);

        let rows = vec![
            vec![
                json!("2023-12-25"),
                json!("14:30:45.123456789"),
                json!("2023-12-25T14:30:45.123456Z"),
            ],
            vec![
                json!("1970-01-01"),
                json!("00:00:00.000000000"),
                json!("1970-01-01T00:00:00.000000Z"),
            ],
        ];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 3);

        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let date1 = NaiveDate::from_ymd_opt(2023, 12, 25).unwrap().signed_duration_since(epoch).num_days() as i32;
        let date2 = 0; // 1970-01-01 is day 0

        fn time_to_nanos(time_str: &str) -> i64 {
            let time = chrono::NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f").unwrap();
            time.num_seconds_from_midnight() as i64 * 1_000_000_000 + time.nanosecond() as i64
        }

        let time1 = time_to_nanos("14:30:45.123456789");
        let time2 = time_to_nanos("00:00:00.000000000");

        // Timestamp: microseconds since Unix epoch
        let timestamp1 = chrono::DateTime::parse_from_rfc3339("2023-12-25T14:30:45.123456Z").unwrap().timestamp_micros();
        let timestamp2 = 0; // 1970-01-01T00:00:00.000000Z

        assert_date32_array(&result, 0, vec![date1, date2]);
        assert_time64_nanosecond_array(&result, 1, vec![time1, time2]);
        assert_timestamp_microsecond_array(&result, 2, vec![timestamp1, timestamp2]);
    }

    #[test]
    fn test_invalid_date_format() {
        let columns = create_test_columns(vec![("date_col", "date")]);
        let rows = vec![vec![json!("invalid-date")]];

        let result = rows_to_arrow(&rows, &columns);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_time_format() {
        let columns = create_test_columns(vec![("time_col", "time")]);
        let rows = vec![vec![json!("invalid-time")]];

        let result = rows_to_arrow(&rows, &columns);
        assert!(result.is_err());
    }

    #[test]
    fn test_decimal_types() {
        let columns = create_test_columns(vec![
            ("decimal128_col", "decimal(10,2)"),
            ("decimal256_col", "decimal(42,4)"),
        ]);

        let rows = vec![
            vec![
                json!("123.45"),
                json!("999999999999.9999")
            ],
            vec![
                json!("0.00"),
                json!("0.0000")
            ],
        ];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2);

        // Helper function to convert decimal string to scaled integer
        fn decimal_to_scaled_int128(decimal_str: &str, scale: u8) -> i128 {
            let decimal = decimal_str.parse::<bigdecimal::BigDecimal>().unwrap();
            let scale_factor = 10_i128.pow(scale as u32);
            (decimal * bigdecimal::BigDecimal::from(scale_factor)).to_i128().unwrap()
        }

        fn decimal_to_scaled_int256(decimal_str: &str, scale: u8) -> arrow::datatypes::i256 {
            let decimal = decimal_str.parse::<bigdecimal::BigDecimal>().unwrap();
            let scale_factor = bigdecimal::BigDecimal::from(10_i128.pow(scale as u32));
            let scaled_decimal = decimal * scale_factor;

            // Convert BigDecimal to i256 (this is what your to_decimal_256 function does)
            let (bigint_value, _) = scaled_decimal.as_bigint_and_exponent();
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
            arrow::datatypes::i256::from_le_bytes(array)
        }

        // Calculate expected values
        // decimal(10,2) means scale=2, so 123.45 becomes 12345
        let decimal128_1 = decimal_to_scaled_int128("123.45", 2);
        let decimal128_2 = decimal_to_scaled_int128("0.00", 2);

        // decimal(42,4) means scale=4, so 999999999999.9999 becomes 9999999999999999
        let decimal256_1 = decimal_to_scaled_int256("999999999999.9999", 4);
        let decimal256_2 = decimal_to_scaled_int256("0.0000", 4);

        assert_decimal128_array(&result, 0, vec![decimal128_1, decimal128_2]);
        assert_decimal256_array(&result, 1, vec![decimal256_1, decimal256_2]);
    }

    #[test]
    fn test_invalid_decimal_format() {
        let columns = create_test_columns(vec![("decimal_col", "decimal(10,2)")]);
        let rows = vec![vec![json!("not-a-number")]];

        let result = rows_to_arrow(&rows, &columns);
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_data() {
        let columns = create_test_columns(vec![("binary_col", "varbinary")]);

        let base64_data = base64::encode(b"hello world");
        let rows = vec![vec![json!(base64_data)], vec![json!("plain text")]];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 2);

        let binary_array = result
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!(!binary_array.is_null(0));
        assert!(!binary_array.is_null(1));
    }

    #[test]
    fn test_list_type() {
        let columns = create_test_columns(vec![("list_col", "array(varchar)")]);

        let rows = vec![
            vec![json!(["item1", "item2", "item3"])],
            vec![json!(["single"])],
            vec![Value::Null],
        ];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_struct_type() {
        let columns = create_test_columns(vec![("struct_col", "row(name varchar, age integer)")]);

        let rows = vec![
            vec![json!({"name": "Alice", "age": 30})],
            vec![json!(["Bob", 25])], // Array format
            vec![Value::Null],
        ];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_map_type() {
        let columns = create_test_columns(vec![("map_col", "map(varchar, integer)")]);

        let rows = vec![
            vec![json!({"key1": 1, "key2": 2})],
            vec![json!([{"key": "key3", "value": 3}])], // Array of key-value pairs
            vec![Value::Null],
        ];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_integer_overflow_handling() {
        let columns = create_test_columns(vec![
            ("int8_col", "tinyint"),
            ("int16_col", "smallint"),
            ("int32_col", "integer"),
        ]);

        // Values that exceed the respective integer type limits
        let rows = vec![vec![
            json!(1000),                   // Exceeds i8::MAX (127)
            json!(100000),                 // Exceeds i16::MAX (32767)
            json!(9223372036854775807i64), // Exceeds i32::MAX
        ]];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 1);

        // These should be null due to overflow
        let int8_array = result
            .column(0)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        assert!(int8_array.is_null(0));

        let int16_array = result
            .column(1)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert!(int16_array.is_null(0));

        let int32_array = result
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(int32_array.is_null(0));
    }

    #[test]
    fn test_type_coercion_fallback() {
        let columns = create_test_columns(vec![
            ("bool_col", "boolean"),
            ("int_col", "integer"),
            ("string_col", "varchar"),
        ]);

        // Send wrong types - these should mostly become nulls or coerced
        let rows = vec![vec![
            json!("not a boolean"), // Wrong type for boolean
            json!("not a number"),  // Wrong type for integer
            json!(42),              // Number for string (should be coerced)
        ]];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 1);

        // Boolean with wrong type should be null
        let bool_array = result
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(bool_array.is_null(0));

        // Integer with wrong type should be null
        let int_array = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(int_array.is_null(0));

        // Number should be coerced to string
        let string_array = result
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(!string_array.is_null(0));
        assert_eq!(string_array.value(0), "42");
    }

    #[test]
    fn test_large_dataset() {
        let columns = create_test_columns(vec![("id", "bigint"), ("value", "varchar")]);

        // Create 1000 rows of test data
        let mut rows = Vec::new();
        for i in 0..1000 {
            rows.push(vec![json!(i), json!(format!("value_{}", i))]);
        }

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 1000);
        assert_eq!(result.num_columns(), 2);

        // Verify first and last rows
        let id_array = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 0);
        assert_eq!(id_array.value(999), 999);

        let value_array = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(value_array.value(0), "value_0");
        assert_eq!(value_array.value(999), "value_999");
    }

    #[test]
    fn test_mixed_null_and_valid_data() {
        let columns =
            create_test_columns(vec![("mixed_int", "integer"), ("mixed_string", "varchar")]);

        let rows = vec![
            vec![json!(1), json!("first")],
            vec![Value::Null, json!("second")],
            vec![json!(3), Value::Null],
            vec![Value::Null, Value::Null],
            vec![json!(5), json!("fifth")],
        ];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 5);

        let int_array = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Check null pattern
        assert!(!int_array.is_null(0));
        assert!(int_array.is_null(1));
        assert!(!int_array.is_null(2));
        assert!(int_array.is_null(3));
        assert!(!int_array.is_null(4));

        // Check values
        assert_eq!(int_array.value(0), 1);
        assert_eq!(int_array.value(2), 3);
        assert_eq!(int_array.value(4), 5);
    }

    #[test]
    fn test_row_column_count_mismatch() {
        let columns = create_test_columns(vec![
            ("col1", "integer"),
            ("col2", "varchar"),
            ("col3", "boolean"),
        ]);

        // Row with fewer values than columns
        let rows = vec![
            vec![json!(1), json!("test")], // Missing third column
        ];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 3);

        // The missing column should be null
        let bool_array = result
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(bool_array.is_null(0));
    }

    #[test]
    fn test_null_builder_type() {
        let columns = create_test_columns(vec![("null_col", "null")]);
        let rows = vec![vec![Value::Null], vec![Value::Null]];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 1);

        let null_array = result
            .column(0)
            .as_any()
            .downcast_ref::<NullArray>()
            .unwrap();
        assert_eq!(null_array.len(), 2);
    }

    #[test]
    fn test_edge_case_timestamps() {
        let columns = create_test_columns(vec![("ts_col", "timestamp")]);

        let rows = vec![
            vec![json!("2023-01-01T00:00:00Z")],
            vec![json!("2023-12-31 23:59:59.999999")],
            vec![Value::Null],
        ];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 3);

        let ts_array = result
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        assert!(!ts_array.is_null(0));
        assert!(!ts_array.is_null(1));
        assert!(ts_array.is_null(2));
    }

    #[test]
    fn test_schema_building() {
        let columns = create_test_columns(vec![
            ("field1", "bigint"),
            ("field2", "varchar"),
            ("field3", "boolean"),
        ]);

        let schema = build_schema_from_columns(&columns).unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "field1");
        assert_eq!(schema.field(1).name(), "field2");
        assert_eq!(schema.field(2).name(), "field3");

        // All fields should be nullable
        assert!(schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
        assert!(schema.field(2).is_nullable());
    }

    #[test]
    fn test_complex_nested_struct() {
        let columns = create_test_columns(vec![(
            "nested_struct",
            "row(person row(name varchar, age integer), active boolean)",
        )]);

        let rows = vec![vec![json!({
            "person": {"name": "John", "age": 30},
            "active": true
        })]];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 1);
    }

    #[test]
    fn test_complex_nested_list() {
        let columns = create_test_columns(vec![(
            "nested_list",
            "array(row(id integer, tags array(varchar)))",
        )]);

        let rows = vec![vec![json!([
            {
                "id": 1,
                "tags": ["rust", "arrow", "data"]
            },
            {
                "id": 2,
                "tags": ["programming", "testing"]
            }
        ])]];

        let result = rows_to_arrow(&rows, &columns).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 1);
    }

    // #[test]
    // fn test_complex_nested_map() {
    //     let columns = create_test_columns(vec![(
    //         "nested_map",
    //         "map(varchar, row(count integer, metadata array(varchar)))",
    //     )]);
    //
    //     let rows = vec![vec![json!({
    //         "users": {
    //             "count": 100,
    //             "metadata": ["active", "verified"]
    //         },
    //         "orders": {
    //             "count": 250,
    //             "metadata": ["pending", "completed"]
    //         }
    //     })]];
    //
    //     let result = rows_to_arrow(&rows, &columns).unwrap();
    //     assert_eq!(result.num_rows(), 1);
    //     assert_eq!(result.num_columns(), 1);
    // }
    //
    // #[test]
    // fn test_list_of_structs_with_nested_lists() {
    //     let columns = create_test_columns(vec![(
    //         "complex_nested",
    //         "array(row(user_id integer, permissions array(varchar), profile row(name varchar, settings array(varchar))))",
    //     )]);
    //
    //     let rows = vec![vec![json!([
    //     {
    //         "user_id": 1,
    //         "permissions": ["read", "write"],
    //         "profile": {
    //             "name": "Alice",
    //             "settings": ["dark_mode", "notifications"]
    //         }
    //     },
    //     {
    //         "user_id": 2,
    //         "permissions": ["read"],
    //         "profile": {
    //             "name": "Bob",
    //             "settings": ["light_mode"]
    //         }
    //     }
    // ])]];
    //
    //     let result = rows_to_arrow(&rows, &columns).unwrap();
    //     assert_eq!(result.num_rows(), 1);
    //     assert_eq!(result.num_columns(), 1);
    // }
    //
    // #[test]
    // fn test_map_with_nested_structs_and_lists() {
    //     let columns = create_test_columns(vec![(
    //         "deeply_nested_map",
    //         "map(varchar, row(info row(description varchar, tags array(varchar)), stats row(count integer, active boolean)))",
    //     )]);
    //
    //     let rows = vec![vec![json!({
    //     "project_alpha": {
    //         "info": {
    //             "description": "First project",
    //             "tags": ["experimental", "rust"]
    //         },
    //         "stats": {
    //             "count": 42,
    //             "active": true
    //         }
    //     },
    //     "project_beta": {
    //         "info": {
    //             "description": "Second project",
    //             "tags": ["stable", "production"]
    //         },
    //         "stats": {
    //             "count": 128,
    //             "active": false
    //         }
    //     }
    // })]];
    //
    //     let result = rows_to_arrow(&rows, &columns).unwrap();
    //     assert_eq!(result.num_rows(), 1);
    //     assert_eq!(result.num_columns(), 1);
    // }
}
