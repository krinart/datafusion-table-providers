use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::{Client, Identity};
use secrecy::{ExposeSecret, SecretString};
use snafu::{ResultExt, Snafu};
use std::{collections::HashMap, fs, sync::Arc, time::Duration};

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
    FailedToBuildTrinoHttpClient { source: reqwest::Error },

    #[snafu(display("Trino server error: {status_code} - {message}"))]
    TrinoServerError { status_code: u16, message: String },

    #[snafu(display("Invalid Trino authentication configuration: {details}"))]
    InvalidAuthConfig { details: String },

    #[snafu(display("Failed to read identity PEM file at '{}': {}", path, source))]
    UnableToReadIdentityPem {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Invalid identity PEM at '{}': {}", path, source))]
    InvalidIdentityPem {
        path: String,
        source: reqwest::Error,
    },
}

#[derive(Clone)]
pub struct TrinoConnectionPool {
    base_url: String,
    catalog: String,
    schema: String,
    client: Arc<Client>,
    join_push_down: JoinPushDown,
    unsupported_type_action: UnsupportedTypeAction,
}

impl std::fmt::Debug for TrinoConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoConnectionPool")
            .field("base_url", &self.base_url)
            .field("catalog", &self.catalog)
            .field("schema", &self.schema)
            .field("join_push_down", &self.join_push_down)
            .field("unsupported_type_action", &self.unsupported_type_action)
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
    ///   * `identity_pem_path` - Path to a PEM file containing both the client certificate and private key for mTLS authentication. (optional)
    ///   * `bearer_token` - Bearer token for authentication (optional)
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        let params = util::remove_prefix_from_hashmap_keys(params, "trino_");

        let base_url = build_base_url(&params)?;
        let (catalog, schema) = get_catalog_and_schema(&params)?;
        let (user, password) = get_user_and_password(&params);
        let bearer_token = params.get("bearer_token").cloned();

        validate_auth_exclusivity(&params, &user, &password)?;

        let headers = build_headers(&catalog, &schema, &user, &password, &bearer_token)?;

        let timeout_seconds = parse_u64_param(&params, "timeout", 300)?;
        let ssl_verification = parse_bool_param(&params, "ssl_verification", true)?;

        let mut client_builder = Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(timeout_seconds))
            .danger_accept_invalid_certs(!ssl_verification);

        if let Some(identity_path) = params.get("identity_pem_path") {
            let pem =
                fs::read(identity_path.expose_secret()).context(UnableToReadIdentityPemSnafu {
                    path: identity_path.expose_secret().to_string(),
                })?;

            let identity = Identity::from_pem(&pem).context(InvalidIdentityPemSnafu {
                path: identity_path.expose_secret().to_string(),
            })?;
            client_builder = client_builder.identity(identity);
        }

        let client = client_builder
            .build()
            .context(FailedToBuildTrinoHttpClientSnafu)?;

        Self::test_connection(&client, &base_url).await?;

        let join_push_down = Self::get_join_context(&base_url, &catalog, &schema, &user);

        Ok(Self {
            base_url,
            catalog,
            schema,
            client: Arc::new(client),
            join_push_down,
            unsupported_type_action: UnsupportedTypeAction::default(),
        })
    }

    #[must_use]
    pub fn with_unsupported_type_action(mut self, action: UnsupportedTypeAction) -> Self {
        self.unsupported_type_action = action;
        self
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
}

#[async_trait]
impl DbConnectionPool<Arc<Client>, &'static str> for TrinoConnectionPool {
    async fn connect(&self) -> super::Result<Box<dyn DbConnection<Arc<Client>, &'static str>>> {
        let mut connection =
            TrinoConnection::new_with_config(self.client.clone(), self.base_url.clone())
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        connection = connection.with_unsupported_type_action(self.unsupported_type_action);

        Ok(Box::new(connection))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

fn build_base_url(params: &HashMap<String, SecretString>) -> Result<String> {
    if let Some(url) = params.get("url").map(ExposeSecret::expose_secret) {
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(Error::InvalidTrinoUrl {
                url: url.to_string(),
            });
        }
        Ok(url.trim_end_matches('/').to_string())
    } else {
        let host = params
            .get("host")
            .map(ExposeSecret::expose_secret)
            .ok_or_else(|| Error::MissingRequiredParameter {
                parameter_name: "url or host".to_string(),
            })?;

        let port = parse_u16_param(params, "port", 8080)?;
        futures::executor::block_on(verify_ns_lookup_and_tcp_connect(host, port))
            .context(InvalidHostOrPortSnafu { host, port })?;

        let protocol = if parse_bool_param(params, "ssl", false)? {
            "https"
        } else {
            "http"
        };

        Ok(format!("{}://{}:{}", protocol, host, port))
    }
}

fn get_catalog_and_schema(params: &HashMap<String, SecretString>) -> Result<(String, String)> {
    let catalog = params
        .get("catalog")
        .map(ExposeSecret::expose_secret)
        .ok_or_else(|| Error::MissingRequiredParameter {
            parameter_name: "catalog".to_string(),
        })?
        .to_string();

    let schema = params
        .get("schema")
        .map(ExposeSecret::expose_secret)
        .unwrap_or("default")
        .to_string();

    Ok((catalog, schema))
}

fn get_user_and_password(
    params: &HashMap<String, SecretString>,
) -> (Option<String>, Option<SecretString>) {
    let user = params.get("user").map(|u| u.expose_secret().to_string());
    let password = params.get("password").cloned();
    (user, password)
}

fn validate_auth_exclusivity(
    params: &HashMap<String, SecretString>,
    user: &Option<String>,
    password: &Option<SecretString>,
) -> Result<()> {
    let has_user_pass = user.is_some() || password.is_some();
    let has_identity = params.contains_key("identity_pem_path");
    let has_token = params.contains_key("bearer_token");

    let auth_count = [has_user_pass, has_identity, has_token]
        .into_iter()
        .filter(|x| *x)
        .count();

    if auth_count != 1 {
        return Err(Error::InvalidAuthConfig {
            details: "Exactly one authentication method must be provided: basic auth, mTLS, or bearer token".into(),
        });
    }
    Ok(())
}

fn build_headers(
    catalog: &str,
    schema: &str,
    user: &Option<String>,
    password: &Option<SecretString>,
    bearer_token: &Option<SecretString>,
) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert("X-Trino-Catalog", catalog.parse().unwrap());
    headers.insert("X-Trino-Schema", schema.parse().unwrap());

    if let Some(user) = user {
        headers.insert("X-Trino-User", user.parse().unwrap());
    }

    if let (Some(user), Some(password)) = (user, password) {
        let credentials = format!("{}:{}", user, password.expose_secret());
        let encoded = BASE64.encode(credentials);
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {}", encoded)).unwrap(),
        );
    } else if let Some(token) = bearer_token {
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", token.expose_secret())).unwrap(),
        );
    }

    Ok(headers)
}

fn parse_u64_param(params: &HashMap<String, SecretString>, key: &str, default: u64) -> Result<u64> {
    params
        .get(key)
        .map(ExposeSecret::expose_secret)
        .unwrap_or(&default.to_string())
        .parse::<u64>()
        .context(InvalidIntegerParameterSnafu {
            parameter_name: key,
        })
}

fn parse_u16_param(params: &HashMap<String, SecretString>, key: &str, default: u16) -> Result<u16> {
    params
        .get(key)
        .map(ExposeSecret::expose_secret)
        .unwrap_or(&default.to_string())
        .parse::<u16>()
        .context(InvalidIntegerParameterSnafu {
            parameter_name: key,
        })
}

fn parse_bool_param(
    params: &HashMap<String, SecretString>,
    key: &str,
    default: bool,
) -> Result<bool> {
    params
        .get(key)
        .map(ExposeSecret::expose_secret)
        .unwrap_or(&default.to_string())
        .parse::<bool>()
        .or_else(|_| Ok(default))
}
