use super::{Error, Result};
use arrow::datatypes::DataType;
use arrow_schema::{Field, Fields, TimeUnit};
use std::sync::Arc;

pub(crate) fn trino_data_type_to_arrow_type(trino_type: &str) -> Result<DataType> {
    let normalized_type = trino_type.to_lowercase();

    match normalized_type.as_str() {
        "null" => Ok(DataType::Null),
        "boolean" => Ok(DataType::Boolean),
        "tinyint" => Ok(DataType::Int8),
        "smallint" => Ok(DataType::Int16),
        "integer" => Ok(DataType::Int32),
        "bigint" => Ok(DataType::Int64),
        "real" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "varchar" | "char" => Ok(DataType::Utf8),
        "varbinary" => Ok(DataType::Binary),
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
        _ if normalized_type.starts_with("map") => Ok(DataType::Utf8),
        _ if normalized_type.starts_with("row") => parse_row_type(&normalized_type),
        _ => Err(Error::UnsupportedTrinoType {
            trino_type: trino_type.to_string(),
        }),
    }
}

fn parse_decimal_type(type_str: &str) -> Result<DataType> {
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
            Ok(DataType::Decimal128(18, 6))
        }
    } else {
        Ok(DataType::Decimal128(18, 6))
    }
}

fn parse_array_type(type_str: &str) -> Result<DataType> {
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.rfind(')') {
            let element_type_str = &type_str[start + 1..end];
            return match trino_data_type_to_arrow_type(element_type_str)? {
                DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _) => Ok(
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                ),
                inner_arrow_type => Ok(DataType::List(Arc::new(Field::new(
                    "item",
                    inner_arrow_type,
                    true,
                )))),
            };
        }
    }

    Err(Error::UnsupportedTrinoType {
        trino_type: type_str.to_string(),
    })
}

fn parse_row_type(type_str: &str) -> Result<DataType> {
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.rfind(')') {
            let inner = &type_str[start + 1..end];
            let mut fields = Vec::new();

            let field_definitions = split_respecting_parentheses(inner, ',');

            for field_def in field_definitions {
                let field_def = field_def.trim();
                if let Some(space_pos) = field_def.find(' ') {
                    let field_name = field_def[..space_pos].trim();
                    let field_type = field_def[space_pos + 1..].trim();
                    let arrow_type = match trino_data_type_to_arrow_type(field_type)? {
                        DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _) => {
                            DataType::Utf8
                        }
                        inner_arrow_type => inner_arrow_type,
                    };
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

fn split_respecting_parentheses(s: &str, delimiter: char) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0;

    for ch in s.chars() {
        match ch {
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                paren_depth -= 1;
                current.push(ch);
            }
            ch if ch == delimiter && paren_depth == 0 => {
                result.push(current.trim().to_string());
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    if !current.is_empty() {
        result.push(current.trim().to_string());
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn test_basic_types() {
        assert_eq!(
            trino_data_type_to_arrow_type("null").unwrap(),
            DataType::Null
        );
        assert_eq!(
            trino_data_type_to_arrow_type("boolean").unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            trino_data_type_to_arrow_type("tinyint").unwrap(),
            DataType::Int8
        );
        assert_eq!(
            trino_data_type_to_arrow_type("smallint").unwrap(),
            DataType::Int16
        );
        assert_eq!(
            trino_data_type_to_arrow_type("integer").unwrap(),
            DataType::Int32
        );
        assert_eq!(
            trino_data_type_to_arrow_type("bigint").unwrap(),
            DataType::Int64
        );
        assert_eq!(
            trino_data_type_to_arrow_type("real").unwrap(),
            DataType::Float32
        );
        assert_eq!(
            trino_data_type_to_arrow_type("double").unwrap(),
            DataType::Float64
        );
    }

    #[test]
    fn test_string_types() {
        assert_eq!(
            trino_data_type_to_arrow_type("varchar").unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            trino_data_type_to_arrow_type("char").unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            trino_data_type_to_arrow_type("varbinary").unwrap(),
            DataType::Binary
        );
        assert_eq!(
            trino_data_type_to_arrow_type("json").unwrap(),
            DataType::LargeUtf8
        );
    }

    #[test]
    fn test_parametrized_string_types() {
        assert_eq!(
            trino_data_type_to_arrow_type("varchar(255)").unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            trino_data_type_to_arrow_type("char(10)").unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            trino_data_type_to_arrow_type("varbinary(1000)").unwrap(),
            DataType::Binary
        );
    }

    #[test]
    fn test_temporal_types() {
        assert_eq!(
            trino_data_type_to_arrow_type("date").unwrap(),
            DataType::Date32
        );
        assert_eq!(
            trino_data_type_to_arrow_type("time").unwrap(),
            DataType::Time64(TimeUnit::Nanosecond)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            trino_data_type_to_arrow_type("timestamp with time zone").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_case_insensitive() {
        assert_eq!(
            trino_data_type_to_arrow_type("BOOLEAN").unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            trino_data_type_to_arrow_type("Boolean").unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            trino_data_type_to_arrow_type("VARCHAR").unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            trino_data_type_to_arrow_type("TIMESTAMP WITH TIME ZONE").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_decimal_types() {
        assert_eq!(
            trino_data_type_to_arrow_type("decimal").unwrap(),
            DataType::Decimal128(18, 6)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("decimal(10)").unwrap(),
            DataType::Decimal128(10, 0)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("decimal(10,2)").unwrap(),
            DataType::Decimal128(10, 2)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("decimal(50,10)").unwrap(),
            DataType::Decimal256(50, 10)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("numeric(10,2)").unwrap(),
            DataType::Decimal128(10, 2)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("decimal(38,0)").unwrap(),
            DataType::Decimal128(38, 0)
        );

        assert_eq!(
            trino_data_type_to_arrow_type("decimal(39,0)").unwrap(),
            DataType::Decimal256(39, 0)
        );
    }

    #[test]
    fn test_array_types() {
        assert_eq!(
            trino_data_type_to_arrow_type("array(integer)").unwrap(),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );

        assert_eq!(
            trino_data_type_to_arrow_type("array(varchar)").unwrap(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );

        assert_eq!(
            trino_data_type_to_arrow_type("array(decimal(10,2))").unwrap(),
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Decimal128(10, 2),
                true
            )))
        );
    }

    #[test]
    fn test_nested_array_types() {
        // Array of array becomes array of strings
        assert_eq!(
            trino_data_type_to_arrow_type("array(array(integer))").unwrap(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        );

        // Array of maps becomes array of strings
        assert_eq!(
            trino_data_type_to_arrow_type("array(map(varchar, integer))").unwrap(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );

        // Array of maps becomes array of strings
        assert_eq!(
            trino_data_type_to_arrow_type("array(row(name varchar, age integer))").unwrap(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
    }

    #[test]
    fn test_map_types() {
        // Maps are represented as strings

        assert_eq!(
            trino_data_type_to_arrow_type("map(varchar, integer)").unwrap(),
            DataType::Utf8,
        );

        assert_eq!(
            trino_data_type_to_arrow_type("map(integer, double)").unwrap(),
            DataType::Utf8,
        );
    }

    #[test]
    fn test_row_type_simple() {
        let expected = DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]));
        assert_eq!(
            trino_data_type_to_arrow_type("row(name varchar, age integer)").unwrap(),
            expected
        );
    }

    fn test_row_type_complex() {
        let expected_multi = DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("salary", DataType::Decimal128(10, 2), true),
            Field::new("active", DataType::Boolean, true),
        ]));
        assert_eq!(
            trino_data_type_to_arrow_type(
                "row(id bigint, name varchar, salary decimal(10,2), active boolean)"
            )
            .unwrap(),
            expected_multi
        );
    }

    #[test]
    fn test_row_type_with_array() {
        let expected_row_array = DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("scores", DataType::Utf8, true),
        ]));
        assert_eq!(
            trino_data_type_to_arrow_type("row(name varchar, scores array(integer))").unwrap(),
            expected_row_array
        );
    }

    #[test]
    fn test_row_type_with_map() {
        let expected_row_array = DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("scores", DataType::Utf8, true),
        ]));
        assert_eq!(
            trino_data_type_to_arrow_type("row(name varchar, scores map(varchar, integer))")
                .unwrap(),
            expected_row_array
        );
    }

    #[test]
    fn test_row_type_with_nested_row() {
        let expected_row_array = DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("scores", DataType::Utf8, true),
        ]));
        assert_eq!(
            trino_data_type_to_arrow_type("row(name varchar, scores row(value integer))").unwrap(),
            expected_row_array
        );
    }

    #[test]
    fn test_unsupported_types() {
        let result = trino_data_type_to_arrow_type("unknown_type");
        assert!(result.is_err());
        if let Err(Error::UnsupportedTrinoType { trino_type }) = result {
            assert_eq!(trino_type, "unknown_type");
        }
    }
}
