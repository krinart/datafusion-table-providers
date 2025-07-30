use std::{collections::HashMap, sync::Arc};

use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::trinodbpool::TrinoConnectionPool;
use datafusion_table_providers::trino::TrinoTableFactory;
use datafusion_table_providers::util::secrets::to_secret_map;

/// This example demonstrates how to:
/// 1. Create a Trino connection pool
/// 2. Create and use TrinoTableFactory to generate TableProvider
/// 3. Use SQL queries to access Trino table data
///
/// Prerequisites:
/// Start a Trino server using Docker:
/// ```bash
/// docker run --name schema \
/// -p 8080:8080 \
/// -d trinodb/schema:latest
/// # Wait for the Trino server to start
/// sleep 30
///
/// # Create a table in the Trino server using the memory connector
/// # Connect to Trino CLI and create some sample data
/// docker exec -it schema schema --server localhost:8080 --catalog memory --schema default
///
/// # In the Trino CLI, run:
/// CREATE TABLE memory.default.companies (
///   id bigint,
///   name varchar
/// );
///
/// INSERT INTO memory.default.companies VALUES
/// (1, 'Acme Corporation'),
/// (2, 'Global Industries'),
/// (3, 'Tech Solutions Inc');
/// ```
///
/// Alternative setup with Trino + PostgreSQL:
/// ```bash
/// # Start PostgreSQL first
/// docker run --name postgres \
/// -e POSTGRES_PASSWORD=password \
/// -e POSTGRES_DB=testdb \
/// -p 5432:5432 \
/// -d postgres:15
///
/// # Start Trino with PostgreSQL connector
/// docker run --name schema \
/// -p 8080:8080 \
/// -v $(pwd)/schema-config:/etc/schema \
/// -d trinodb/schema:latest
/// ```
#[tokio::main]
async fn main() {
    // Create Trino connection parameters
    // Including coordinator URL, catalog, and authentication settings
    let trino_params = to_secret_map(HashMap::from([
        ("url".to_string(), "http://localhost:8080".to_string()),
        ("catalog".to_string(), "tpch".to_string()),
        ("schema".to_string(), "tiny".to_string()),

        // Optional authentication
        ("user".to_string(), "test".to_string()),
        // ("password".to_string(), "secret".to_string()),

        // Optional settings
        // ("timeout".to_string(), "300".to_string()),
        // ("ssl_verification".to_string(), "false".to_string()),
    ]));

    // Create Trino connection pool
    let trino_pool = Arc::new(
        TrinoConnectionPool::new(trino_params)
            .await
            .expect("unable to create Trino connection pool"),
    );

    // Create Trino table provider factory
    // Used to generate TableProvider instances that can read Trino table data
    let table_factory = TrinoTableFactory::new(trino_pool.clone());

    // Create DataFusion session context
    let ctx = SessionContext::new();

    // Demonstrate direct table provider registration
    // This method registers the table in the default catalog
    // Here we register the Trino "region" table as "region"
    ctx.register_table(
        "region",
        table_factory
            .table_provider(TableReference::bare("region"))
            .await
            .expect("failed to register table provider"),
    )
        .expect("failed to register table");

    let df = ctx
        .sql("SELECT * FROM region")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");
}