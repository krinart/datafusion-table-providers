use std::{any::Any, sync::Arc};

use crate::sql::arrow_sql_gen::trino::{self, schema::data_type_to_arrow_type, arrow::rows_to_arrow};
use crate::util::handle_unsupported_type_error;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use async_stream::stream;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use futures::stream;
use futures::StreamExt;
use serde_json::Value;
use snafu::prelude::*;
use tokio::time::sleep;
use crate::UnsupportedTypeAction;
use std::time::Duration;
use arrow_schema::{DataType, TimeUnit};
use super::AsyncDbConnection;
use super::DbConnection;
use super::Result;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Query execution failed.\n{source}\nFor details, refer to the Trino documentation: https://trino.io/docs/"))]
    QueryError { source: reqwest::Error },

    #[snafu(display("Failed to convert query result to Arrow.\n{source}\nReport a bug to request support: https://github.com/datafusion-contrib/datafusion-table-providers/issues"))]
    ConversionError { source: trino::arrow::Error },

    #[snafu(display("Authentication failed."))]
    AuthenticationFailedError,

    #[snafu(display("Trino server error: {status_code} - {message}"))]
    TrinoServerError {
        status_code: u16,
        message: String,
    },

    #[snafu(display("Failed to parse Trino response: {source}"))]
    ResponseParseError { source: serde_json::Error },

    #[snafu(display("Unsupported data type '{data_type}' for field '{column_name}'.\nReport a bug to request support: https://github.com/datafusion-contrib/datafusion-table-providers/issues"))]
    UnsupportedDataTypeError {
        column_name: String,
        data_type: String,
    },

    #[snafu(display("Failed to find the field '{field}'.\nReport a bug to request support: https://github.com/datafusion-contrib/datafusion-table-providers/issues"))]
    MissingField { field: String },

    #[snafu(display("Invalid Trino URL: {url}"))]
    InvalidUrl { url: String },
}

pub struct TrinoConnection {
    client: Arc<reqwest::Client>,
    base_url: String,
    unsupported_type_action: UnsupportedTypeAction,
}

impl<'a> DbConnection<Arc<reqwest::Client>, &'a str> for TrinoConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Arc<reqwest::Client>, &'a str>> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl<'a> AsyncDbConnection<Arc<reqwest::Client>, &'a str> for TrinoConnection {
    fn new(client: Arc<reqwest::Client>) -> Self {
        TrinoConnection {
            client,
            base_url: String::new(),
            unsupported_type_action: UnsupportedTypeAction::default(),
        }
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, super::Error> {
        let sql = format!("DESCRIBE {}", table_reference.to_string());
        // let sql = format!("DESCRIBE tpch.tiny.region");

        let data_rows = self.execute_query(&sql).await.map_err(|e| {
            super::Error::UnableToGetSchema {
                source: Box::new(e),
            }
        })?;

        let mut fields = Vec::new();

        // println!("data rows: {:?}", data_rows);

        for row_data in data_rows {
            if row_data.len() >= 2 {
                let column_name = row_data[0]
                    .as_str()
                    .ok_or_else(|| super::Error::UnableToGetSchema {
                        source: Box::new(Error::MissingField {
                            field: "column_name".to_string(),
                        }),
                    })?;

                let data_type = row_data[1]
                    .as_str()
                    .ok_or_else(|| super::Error::UnableToGetSchema {
                        source: Box::new(Error::MissingField {
                            field: "data_type".to_string(),
                        }),
                    })?;

                let nullable = if row_data.len() > 2 {
                    row_data[2].as_str().unwrap_or("true") != "false"
                } else {
                    true
                };

                let Ok(arrow_type) = data_type_to_arrow_type(data_type) else {
                    handle_unsupported_type_error(
                        self.unsupported_type_action,
                        super::Error::UnsupportedDataType {
                            data_type: data_type.to_string(),
                            field_name: column_name.to_string(),
                        },
                    )?;
                    continue;
                };

                fields.push(Field::new(column_name, arrow_type, nullable));
            }
        }

        let schema = Arc::new(Schema::new(fields));

        println!("schema!!: {:?}", schema);
        Ok(schema)
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _params: &[&'a str],
        projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream> {
        // println!("query_arrow!!: {:?}", sql);

        let data_rows = self.execute_query(sql).await.map_err(|e| {
            super::Error::UnableToQueryArrow {
                source: Box::new(e),
            }
        })?;

        let mut stream = Box::pin(stream! {
            if !data_rows.is_empty() {
                // For the new format, we need to get column information separately
                // This might require a separate DESCRIBE query or the column info
                // should be returned along with the data from execute_query

                // For now, we'll need to infer columns or get them from projected_schema
                let columns = if let Some(ref schema) = projected_schema {
                    // Build column info from projected schema
                    schema.fields().iter().map(|field| {
                        serde_json::json!({
                            "name": field.name(),
                            "type": arrow_type_to_trino_type_string(field.data_type())
                        })
                    }).collect::<Vec<_>>()
                } else {
                    // If no projected schema, we'll need to infer from first row
                    // This is a limitation of the new format
                    vec![]
                };

                // Convert Vec<Value> rows to Value array format expected by rows_to_arrow
                let json_rows: Vec<serde_json::Value> = data_rows
                    .iter()
                    .map(|row| serde_json::Value::Array(row.clone()))
                    .collect();

                // Convert data in chunks
                let chunk_size = 4_000;
                for chunk in json_rows.chunks(chunk_size) {
                    let rec = rows_to_arrow(chunk, &columns, &projected_schema)
                        .map_err(|e| Error::ConversionError { source: e })?;
                    yield Ok::<_, Error>(rec);
                }
            }
        });

        let Some(first_chunk) = stream.next().await else {
            println!("No data found");
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                stream::empty(),
            )));
        };

        let first_chunk = first_chunk.map_err(|e| {
            super::Error::UnableToQueryArrow {
                source: Box::new(e),
            }
        })?;
        let schema = first_chunk.schema();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, {
            stream! {
            yield Ok(first_chunk);
            while let Some(batch) = stream.next().await {
                yield batch
                    .map_err(|e| DataFusionError::Execution(format!("Failed to fetch batch: {e}")))
            }
        }
        })))
    }

    async fn execute(&self, query: &str, _params: &[&'a str]) -> Result<u64> {
        Ok(100)
    }
}

fn arrow_type_to_trino_type_string(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "tinyint".to_string(),
        DataType::Int16 => "smallint".to_string(),
        DataType::Int32 => "integer".to_string(),
        DataType::Int64 => "bigint".to_string(),
        DataType::Float32 => "real".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 => "varchar".to_string(),
        DataType::LargeUtf8 => "varchar".to_string(),
        DataType::Binary => "varbinary".to_string(),
        DataType::Date32 => "date".to_string(),
        DataType::Time64(TimeUnit::Nanosecond) => "time".to_string(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp".to_string(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "timestamp with time zone".to_string(),
        DataType::Decimal128(precision, scale) => format!("decimal({},{})", precision, scale),
        DataType::Decimal256(precision, scale) => format!("decimal({},{})", precision, scale),
        _ => "varchar".to_string(), // fallback
    }
}

impl TrinoConnection {
    pub fn new_with_config(
        client: Arc<reqwest::Client>,
        base_url: String,
        // password: Option<String>,
    ) -> Result<Self, Error> {
        // Validate URL
        if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
            return Err(Error::InvalidUrl { url: base_url });
        }

        Ok(TrinoConnection {
            client,
            base_url,
            unsupported_type_action: UnsupportedTypeAction::default(),
        })
    }

    #[must_use]
    pub fn with_unsupported_type_action(mut self, action: UnsupportedTypeAction) -> Self {
        self.unsupported_type_action = action;
        self
    }

    async fn execute_query(&self, sql: &str) -> Result<Vec<Vec<Value>>, Error> {
        let url = format!("{}/v1/statement", self.base_url);

        // Step 1: Submit the query
        let response = self.client.clone()
            .post(&url)
            .body(sql.to_string())
            .send()
            .await
            .context(QuerySnafu)?;

        if !response.status().is_success() {
            let status_code = response.status().as_u16();
            let message = response.text().await.unwrap_or_default();

            return if status_code == 401 {
                Err(Error::AuthenticationFailedError)
            } else {
                Err(Error::TrinoServerError {
                    status_code,
                    message
                })
            };
        }

        let mut result: Value = response.json().await.context(QuerySnafu)?;
        let mut all_data: Vec<Vec<Value>> = Vec::new();

        loop {
            if let Some(data) = result.get("data").and_then(|d| d.as_array()) {
                // println!("Data found");
                for row in data {
                    if let Some(row_array) = row.as_array() {
                        all_data.push(row_array.clone());
                    }
                }
            }

            let state = result["stats"]["state"].as_str().unwrap_or("");

            println!("State: {}, next uri: {}", state, result.get("nextUri").and_then(|v| v.as_str()).unwrap_or(""));

            // Check if query is finished
            if state == "FINISHED" {
                break;
            } else if state == "FAILED" {
                return Err(Error::TrinoServerError {
                    status_code: 500,
                    message: "Query failed".to_string()
                });
            } else if state == "CANCELED" {
                return Err(Error::TrinoServerError {
                    status_code: 499,
                    message: "Query was canceled".to_string()
                });
            }

            if let Some(next_uri) = result.get("nextUri").and_then(|u| u.as_str()) {
                // Wait before polling
                sleep(Duration::from_millis(50)).await;

                let response = self.client.clone()
                    .get(next_uri)
                    .send()
                    .await
                    .context(QuerySnafu)?;

                if !response.status().is_success() {
                    let status_code = response.status().as_u16();
                    let message = response.text().await.unwrap_or_default();
                    return Err(Error::TrinoServerError {
                        status_code,
                        message
                    });
                }

                result = response.json().await.context(QuerySnafu)?;
            } else {
                if state != "FINISHED" {
                    return Err(Error::TrinoServerError {
                        status_code: 500,
                        message: format!("Query stuck in state: {}", state)
                    });
                }
                break;
            }
        }

        Ok(all_data)
    }
}
