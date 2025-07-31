use super::{Error, Result};
use arrow::datatypes::DataType;
use arrow_schema::{Field, Fields, TimeUnit};
use std::sync::Arc;

pub(crate) fn trino_data_type_to_arrow_type(trino_type: &str) -> Result<DataType> {
    let normalized_type = trino_type.to_lowercase();

    match normalized_type.as_str() {
        "boolean" => Ok(DataType::Boolean),
        "tinyint" => Ok(DataType::Int8),
        "smallint" => Ok(DataType::Int16),
        "integer" => Ok(DataType::Int32),
        "bigint" => Ok(DataType::Int64),
        "real" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "varchar" | "char" | "varbinary" => Ok(DataType::Utf8),
        "json" => Ok(DataType::LargeUtf8),
        "date" => Ok(DataType::Date32),
        "time" => Ok(DataType::Time64(TimeUnit::Nanosecond)),
        "timestamp" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "timestamp with time zone" => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        _ if normalized_type.starts_with("decimal") || normalized_type.starts_with("numeric") => {
            parse_decimal_type(&normalized_type)
        }
        _ if normalized_type.starts_with("varchar") => Ok(DataType::Utf8),
        _ if normalized_type.starts_with("char") => Ok(DataType::Utf8),
        _ if normalized_type.starts_with("varbinary") => Ok(DataType::Binary),
        _ if normalized_type.starts_with("array") => parse_array_type(&normalized_type),
        _ if normalized_type.starts_with("map") => parse_map_type(&normalized_type),
        _ if normalized_type.starts_with("row") => parse_row_type(&normalized_type),
        _ => Err(Error::UnsupportedTrinoType {
            trino_type: trino_type.to_string(),
        }),
    }
}

fn parse_decimal_type(type_str: &str) -> Result<DataType> {
    // Parse "decimal(precision,scale)" or "decimal(precision)" or just "decimal"
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.find(')') {
            let params = &type_str[start + 1..end];
            let parts: Vec<&str> = params.split(',').collect();

            let precision = parts[0].trim().parse::<u8>().unwrap_or(38);
            let scale = if parts.len() > 1 {
                parts[1].trim().parse::<i8>().unwrap_or(0)
            } else {
                0
            };

            if precision > 38 {
                Ok(DataType::Decimal256(precision, scale))
            } else {
                Ok(DataType::Decimal128(precision, scale))
            }
        } else {
            Ok(DataType::Decimal128(38, 0))
        }
    } else {
        Ok(DataType::Decimal128(38, 0))
    }
}

fn parse_array_type(type_str: &str) -> Result<DataType> {
    // Parse "array(element_type)"
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.rfind(')') {
            let element_type_str = &type_str[start + 1..end];
            let element_type = trino_data_type_to_arrow_type(element_type_str)?;
            return Ok(DataType::List(Arc::new(Field::new(
                "item",
                element_type,
                true,
            ))));
        }
    }
    Err(Error::UnsupportedTrinoType {
        trino_type: type_str.to_string(),
    })
}

pub(crate) fn parse_map_type(type_str: &str) -> Result<DataType> {
    // Parse "map(key_type, value_type)"
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.rfind(')') {
            let inner = &type_str[start + 1..end];
            // Simple parsing - would need more sophisticated parsing for nested types
            if let Some(comma_pos) = inner.find(',') {
                let key_type_str = inner[..comma_pos].trim();
                let value_type_str = inner[comma_pos + 1..].trim();

                let key_type = trino_data_type_to_arrow_type(key_type_str)?;
                let value_type = trino_data_type_to_arrow_type(value_type_str)?;

                return Ok(DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", key_type, false),
                            Field::new("value", value_type, true),
                        ])),
                        false,
                    )),
                    false,
                ));
            }
        }
    }
    Err(Error::UnsupportedTrinoType {
        trino_type: type_str.to_string(),
    })
}

pub(crate) fn parse_row_type(type_str: &str) -> Result<DataType> {
    // Parse "row(field1 type1, field2 type2, ...)"
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.rfind(')') {
            let inner = &type_str[start + 1..end];
            let mut fields = Vec::new();

            // Simple parsing - would need more sophisticated parsing for complex nested types
            for field_def in inner.split(',') {
                let parts: Vec<&str> = field_def.trim().split_whitespace().collect();
                if parts.len() >= 2 {
                    let field_name = parts[0];
                    let field_type = parts[1..].join(" ");
                    let arrow_type = trino_data_type_to_arrow_type(&field_type)?;
                    fields.push(Field::new(field_name, arrow_type, true));
                }
            }

            return Ok(DataType::Struct(Fields::from(fields)));
        }
    }
    Err(Error::UnsupportedTrinoType {
        trino_type: type_str.to_string(),
    })
}
