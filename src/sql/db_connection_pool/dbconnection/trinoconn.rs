use std::{any::Any, sync::Arc};

use super::AsyncDbConnection;
use super::DbConnection;
use super::Result;
use crate::sql::arrow_sql_gen::trino::{
    self,
    arrow::{rows_to_arrow, TrinoColumn},
    schema::trino_data_type_to_arrow_type,
};
use crate::util::handle_unsupported_type_error;
use crate::UnsupportedTypeAction;
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
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Clone)]
pub struct TrinoQueryResult {
    pub data: Vec<Vec<Value>>,
    pub columns: Vec<TrinoColumn>,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Query execution failed.\n{source}\nFor details, refer to the Trino documentation: https://trino.io/docs/"))]
    QueryError { source: reqwest::Error },

    #[snafu(display("Failed to convert query result to Arrow.\n{source}\nReport a bug to request support: https://github.com/datafusion-contrib/datafusion-table-providers/issues"))]
    ConversionError { source: trino::Error },

    #[snafu(display("Authentication failed."))]
    AuthenticationFailedError,

    #[snafu(display("Trino server error: {status_code} - {message}"))]
    TrinoServerError { status_code: u16, message: String },

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

        let query_result =
            self.execute_query(&sql)
                .await
                .map_err(|e| super::Error::UnableToGetSchema {
                    source: Box::new(e),
                })?;

        let mut fields = Vec::new();

        // println!("data rows: {:?}", data_rows);

        for row_data in query_result.data {
            if row_data.len() >= 2 {
                let column_name =
                    row_data[0]
                        .as_str()
                        .ok_or_else(|| super::Error::UnableToGetSchema {
                            source: Box::new(Error::MissingField {
                                field: "column_name".to_string(),
                            }),
                        })?;

                let data_type =
                    row_data[1]
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

                let Ok(arrow_type) = trino_data_type_to_arrow_type(data_type) else {
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

        Ok(schema)
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _params: &[&'a str],
        _projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream> {
        let query_result =
            self.execute_query(sql)
                .await
                .map_err(|e| super::Error::UnableToQueryArrow {
                    source: Box::new(e),
                })?;

        let data_rows = query_result.data;
        let columns = query_result.columns;

        let mut stream = Box::pin(stream! {
            if !data_rows.is_empty() {
                let chunk_size = 4_000;
                for chunk in data_rows.chunks(chunk_size) {
                    let rec = rows_to_arrow(chunk, &columns)
                        .map_err(|e| Error::ConversionError { source: e })?;
                    yield Ok::<_, Error>(rec);
                }
            }
        });

        let Some(first_chunk) = stream.next().await else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                stream::empty(),
            )));
        };

        let first_chunk = first_chunk.map_err(|e| super::Error::UnableToQueryArrow {
            source: Box::new(e),
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

impl TrinoConnection {
    pub fn new_with_config(client: Arc<reqwest::Client>, base_url: String) -> Result<Self, Error> {
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

    async fn execute_query(&self, sql: &str) -> Result<TrinoQueryResult, Error> {
        println!("Executing query: {}", sql);

        let url = format!("{}/v1/statement", self.base_url);

        // Step 1: Submit the query
        let response = self
            .client
            .clone()
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
                    message,
                })
            };
        }

        let mut result: Value = response.json().await.context(QuerySnafu)?;
        let mut all_data: Vec<Vec<Value>> = Vec::new();
        let mut columns: Vec<TrinoColumn> = Vec::new();

        loop {
            // Extract column information
            if columns.is_empty() {
                if let Some(cols) = result.get("columns").and_then(|c| c.as_array()) {
                    for col in cols {
                        if let Some(col_obj) = col.as_object() {
                            let name = col_obj
                                .get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown")
                                .to_string();

                            let type_name = col_obj
                                .get("type")
                                .and_then(|v| v.as_str())
                                .unwrap_or("varchar")
                                .to_string();

                            columns.push(TrinoColumn { name, type_name });
                        }
                    }
                }
            }

            let state = result["stats"]["state"].as_str().unwrap_or("");
            println!(
                "State: {}, next uri: {}",
                state,
                result.get("nextUri").and_then(|v| v.as_str()).unwrap_or("")
            );

            // Extract data rows
            if let Some(data) = result.get("data").and_then(|d| d.as_array()) {
                println!("data detected");
                for row in data {
                    if let Some(row_array) = row.as_array() {
                        all_data.push(row_array.clone());
                    }
                }
            }

            // Check if query is finished
            if state == "FINISHED" {
                break;
            } else if state == "FAILED" {
                return Err(Error::TrinoServerError {
                    status_code: 500,
                    message: "Query failed".to_string(),
                });
            } else if state == "CANCELED" {
                return Err(Error::TrinoServerError {
                    status_code: 499,
                    message: "Query was canceled".to_string(),
                });
            }

            if let Some(next_uri) = result.get("nextUri").and_then(|u| u.as_str()) {
                // Wait before polling
                sleep(Duration::from_millis(50)).await;

                let response = self
                    .client
                    .clone()
                    .get(next_uri)
                    .send()
                    .await
                    .context(QuerySnafu)?;

                if !response.status().is_success() {
                    let status_code = response.status().as_u16();
                    let message = response.text().await.unwrap_or_default();
                    return Err(Error::TrinoServerError {
                        status_code,
                        message,
                    });
                }

                result = response.json().await.context(QuerySnafu)?;
            } else {
                if state != "FINISHED" {
                    return Err(Error::TrinoServerError {
                        status_code: 500,
                        message: format!("Query stuck in state: {}", state),
                    });
                }
                break;
            }
        }

        Ok(TrinoQueryResult {
            data: all_data,
            columns,
        })
    }
}
