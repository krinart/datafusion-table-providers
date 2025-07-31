use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use reqwest::Client;
use secrecy::{ExposeSecret, SecretBox, SecretString};
use serde_json::Value;
use snafu::{ResultExt, Snafu};
// use tokio_postgres::types::ToSql;

use crate::{
    sql::db_connection_pool::{
        dbconnection::{trinoconn::TrinoConnection, AsyncDbConnection, DbConnection},
        JoinPushDown,
    },
    util::{self, ns_lookup::verify_ns_lookup_and_tcp_connect},
    UnsupportedTypeAction,
};

use super::DbConnectionPool;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Trino connection failed.\n{source}\nFor details, refer to the Trino documentation: https://trino.io/docs/"))]
    TrinoConnectionError { source: reqwest::Error },

    #[snafu(display(
        "Invalid parameter: {parameter_name}. Ensure the parameter name is correct."
    ))]
    InvalidParameterError { parameter_name: String },

    #[snafu(display("Could not parse {parameter_name} into a valid integer. Ensure it is configured with a valid value."))]
    InvalidIntegerParameterError {
        parameter_name: String,
        source: std::num::ParseIntError,
    },

    #[snafu(display("Cannot connect to Trino on {host}:{port}. Ensure the host and port are correct and reachable."))]
    InvalidHostOrPortError {
        source: crate::util::ns_lookup::Error,
        host: String,
        port: u16,
    },

    #[snafu(display("Authentication failed."))]
    AuthenticationFailedError,

    #[snafu(display("Invalid Trino URL: {url}. Ensure it starts with http:// or https://"))]
    InvalidTrinoUrl { url: String },

    #[snafu(display("Missing required parameter: {parameter_name}"))]
    MissingRequiredParameter { parameter_name: String },

    #[snafu(display("Failed to build HTTP client: {source}"))]
    FailedToBuildHttpClient { source: reqwest::Error },

    #[snafu(display("Trino server error: {status_code} - {message}"))]
    TrinoServerError { status_code: u16, message: String },
}

#[derive(Clone)]
pub struct TrinoConnectionPool {
    base_url: String,
    catalog: String,
    schema: String,
    client: Arc<Client>,
    join_push_down: JoinPushDown,
    unsupported_type_action: UnsupportedTypeAction,
    user: Option<String>,
    password: Option<SecretString>,
}

impl std::fmt::Debug for TrinoConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoConnectionPool")
            .field("base_url", &self.base_url)
            .field("catalog", &self.catalog)
            .field("schema", &self.schema)
            .field("join_push_down", &self.join_push_down)
            .field("unsupported_type_action", &self.unsupported_type_action)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl TrinoConnectionPool {
    /// Creates a new instance of `TrinoConnectionPool`.
    ///
    /// # Arguments
    ///
    /// * `params` - A map of parameters to create the connection pool.
    ///   * `url` or `host` + `port` - The Trino coordinator URL or host and port
    ///   * `catalog` - The default catalog to use (required)
    ///   * `schema` - The default schema to use (optional, defaults to "default")
    ///   * `user` - The user to authenticate with (optional)
    ///   * `password` - The password for authentication (optional)
    ///   * `timeout` - Request timeout in seconds (optional, defaults to 300)
    ///   * `ssl_verification` - Whether to verify SSL certificates (optional, defaults to true)
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        // Remove the "trino_" prefix from the keys to keep backward compatibility
        let params = util::remove_prefix_from_hashmap_keys(params, "trino_");

        // Build the base URL
        let base_url = if let Some(url) = params.get("url").map(SecretBox::expose_secret) {
            if !url.starts_with("http://") && !url.starts_with("https://") {
                return Err(Error::InvalidTrinoUrl {
                    url: url.to_string(),
                });
            }
            url.trim_end_matches('/').to_string()
        } else {
            let host = params
                .get("host")
                .map(SecretBox::expose_secret)
                .ok_or_else(|| Error::MissingRequiredParameter {
                    parameter_name: "url or host".to_string(),
                })?;

            let port = params
                .get("port")
                .map(SecretBox::expose_secret)
                .unwrap_or("8080")
                .parse::<u16>()
                .context(InvalidIntegerParameterSnafu {
                    parameter_name: "port",
                })?;

            // Verify connectivity
            verify_ns_lookup_and_tcp_connect(host, port)
                .await
                .context(InvalidHostOrPortSnafu { host, port })?;

            let protocol = if params
                .get("ssl")
                .map(SecretBox::expose_secret)
                .unwrap_or("false")
                .parse::<bool>()
                .unwrap_or(false)
            {
                "https"
            } else {
                "http"
            };

            format!("{}://{}:{}", protocol, host, port)
        };

        // Required parameters
        let catalog = params
            .get("catalog")
            .map(SecretBox::expose_secret)
            .ok_or_else(|| Error::MissingRequiredParameter {
                parameter_name: "catalog".to_string(),
            })?
            .to_string();

        let schema = params
            .get("schema")
            .map(SecretBox::expose_secret)
            .unwrap_or("default")
            .to_string();

        // Optional parameters
        let user = params.get("user").map(|u| u.expose_secret().to_string());
        let password = params.get("password").cloned();

        let timeout_seconds = params
            .get("timeout")
            .map(SecretBox::expose_secret)
            .unwrap_or("300")
            .parse::<u64>()
            .context(InvalidIntegerParameterSnafu {
                parameter_name: "timeout",
            })?;

        let ssl_verification = params
            .get("ssl_verification")
            .map(SecretBox::expose_secret)
            .unwrap_or("true")
            .parse::<bool>()
            .unwrap_or(true);

        // Build HTTP client
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("X-Trino-Catalog", catalog.parse().unwrap());
        headers.insert("X-Trino-Schema", schema.parse().unwrap());

        if let Some(ref user) = user {
            headers.insert("X-Trino-User", user.parse().unwrap());
        }

        let mut client_builder = Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(timeout_seconds))
            .danger_accept_invalid_certs(!ssl_verification);

        // Add basic auth if password is provided
        // if let (Some(ref user), Some(ref password)) = (&user, &password) {
        //     client_builder = client_builder.basic_auth(user, Some(password.expose_secret()));
        // }

        let client = client_builder
            .build()
            .context(FailedToBuildHttpClientSnafu)?;

        // Test the connection
        Self::test_connection(&client, &base_url).await?;

        let join_push_down = Self::get_join_context(&base_url, &catalog, &schema, &user);

        Ok(Self {
            base_url,
            catalog,
            schema,
            client: Arc::new(client),
            join_push_down,
            unsupported_type_action: UnsupportedTypeAction::default(),
            user,
            password,
        })
    }

    /// Specify the action to take when an unsupported type is encountered.
    #[must_use]
    pub fn with_unsupported_type_action(mut self, action: UnsupportedTypeAction) -> Self {
        self.unsupported_type_action = action;
        self
    }

    /// Returns a direct connection to the underlying Trino cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection.
    pub async fn connect_direct(&self) -> super::Result<TrinoConnection> {
        let mut connection =
            TrinoConnection::new_with_config(self.client.clone(), self.base_url.clone())
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        connection = connection.with_unsupported_type_action(self.unsupported_type_action);

        Ok(connection)
    }

    async fn test_connection(client: &Client, base_url: &str) -> Result<()> {
        let url = format!("{}/v1/info", base_url);

        let response = client
            .get(&url)
            .send()
            .await
            .context(TrinoConnectionSnafu)?;

        if response.status() == 401 {
            return Err(Error::AuthenticationFailedError);
        }

        if !response.status().is_success() {
            return Err(Error::TrinoServerError {
                status_code: response.status().as_u16(),
                message: format!("Connection test failed with HTTP {}", response.status()),
            });
        }

        Ok(())
    }

    fn get_join_context(
        base_url: &str,
        catalog: &str,
        schema: &str,
        user: &Option<String>,
    ) -> JoinPushDown {
        let mut join_context = format!("url={},catalog={},schema={}", base_url, catalog, schema);
        if let Some(user) = user {
            join_context.push_str(&format!(",user={}", user));
        }

        JoinPushDown::AllowedFor(join_context)
    }

    /// Get the base URL for the Trino coordinator
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Get the default catalog
    #[must_use]
    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    /// Get the default schema
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Get the user (if configured)
    #[must_use]
    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }
}

#[async_trait]
impl DbConnectionPool<Arc<Client>, &'static str> for TrinoConnectionPool {
    async fn connect(&self) -> super::Result<Box<dyn DbConnection<Arc<Client>, &'static str>>> {
        let connection = self.connect_direct().await?;
        Ok(Box::new(connection))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}
