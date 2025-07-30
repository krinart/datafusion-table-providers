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

use crate::UnsupportedTypeAction;

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
        // let table_name = table_reference.table();
        // let schema_name = table_reference.schema().unwrap_or(&self.schema);
        // let catalog_name = table_reference.catalog().unwrap_or(&self.catalog);

        // let sql = format!("DESCRIBE {}", table_reference.to_string());
        let sql = format!("DESCRIBE tpch.tiny.region");
        println!("get_schema: {}", sql);

        let query_result = self.execute_query(&sql).await.map_err(|e| {
            super::Error::UnableToGetSchema {
                source: Box::new(e),
            }
        })?;

        println!("get_schema - query_result: {:?}", query_result);

        let mut fields = Vec::new();

        if let Some(data) = query_result.get("data").and_then(|d| d.as_array()) {
            println!("data!!: {:?}", data);
            for row in data {

                if let Some(row_data) = row.as_array() {
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
        let query_result = self.execute_query(sql).await.map_err(|e| {
            super::Error::UnableToQueryArrow {
                source: Box::new(e),
            }
        })?;

        let mut stream = Box::pin(stream! {
            if let Some(data) = query_result.get("data").and_then(|d| d.as_array()) {
                let empty_vec = Vec::new();
                let columns = query_result
                    .get("columns")
                    .and_then(|c| c.as_array())
                    .unwrap_or(&empty_vec);

                // Convert data in chunks
                let chunk_size = 4_000;
                for chunk in data.chunks(chunk_size) {
                    let rec = rows_to_arrow(chunk, columns, &projected_schema)
                        .context(ConversionSnafu)?;
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

        let first_chunk = first_chunk?;
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
        let query_result = self.execute_query(query).await.map_err(|e| {
            super::Error::UnableToQueryArrow {
                source: Box::new(e),
            }
        })?;

        // For non-SELECT queries, Trino returns update count
        let rows_affected = query_result
            .get("updateCount")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        Ok(rows_affected)
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

    async fn execute_query(&self, sql: &str) -> Result<Value, Error> {
        let url = format!("{}/v1/statement", self.base_url);

        let response = self
            .client
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

        let mut query_result: Value = response.json().await.context(QuerySnafu)?;

        // Handle async query execution - poll until complete
        while let Some(next_uri) = query_result.get("nextUri").and_then(|v| v.as_str()) {
            println!("next_uri!!: {}", next_uri);

            let response = self
                .client
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

            query_result = response.json().await.context(QuerySnafu)?;

            // Small delay to avoid overwhelming the server
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        // Check for query errors
        if let Some(error) = query_result.get("error") {
            let error_message = error
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown Trino error");

            return Err(Error::TrinoServerError {
                status_code: 500,
                message: error_message.to_string(),
            });
        }

        Ok(query_result)
    }
}
