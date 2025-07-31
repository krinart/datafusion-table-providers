use std::convert;
use bigdecimal::BigDecimal;
use snafu::Snafu;

pub mod arrow;
pub mod schema;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch { source: datafusion::arrow::error::ArrowError },

    #[snafu(display("No builder found for index {index}"))]
    NoBuilderForIndex { index: usize },

    #[snafu(display("Failed to downcast builder for type {trino_type}"))]
    FailedToDowncastBuilder { trino_type: String },

    #[snafu(display("Integer overflow when converting u64 to i64: {source}"))]
    FailedToConvertU64toI64 {
        source: <u64 as convert::TryInto<i64>>::Error,
    },

    #[snafu(display("Failed to parse JSON value for column {column}: {source}"))]
    FailedToParseJsonValue {
        column: String,
        source: serde_json::Error,
    },

    #[snafu(display("Cannot represent BigDecimal as i128: {big_decimal}"))]
    FailedToConvertBigDecimalToI128 { big_decimal: BigDecimal },

    #[snafu(display("Failed to find field {column_name} in schema"))]
    FailedToFindFieldInSchema { column_name: String },

    #[snafu(display("No Arrow field found for index {index}"))]
    NoArrowFieldForIndex { index: usize },

    #[snafu(display("No column name for index: {index}"))]
    NoColumnNameForIndex { index: usize },

    #[snafu(display("Unsupported Trino type: {trino_type}"))]
    UnsupportedTrinoType { trino_type: String },

    #[snafu(display("Invalid date value: {value}"))]
    InvalidDateValue { value: String },

    #[snafu(display("Invalid time value: {value}"))]
    InvalidTimeValue { value: String },

    #[snafu(display("Invalid timestamp value: {value}"))]
    InvalidTimestampValue { value: String },

    #[snafu(display("Failed to parse decimal value: {value}"))]
    FailedToParseDecimal { value: String },
}