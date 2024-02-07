use thiserror::Error;

pub mod client;
pub mod field;
pub mod manager;
pub mod mapper;
pub mod replication;
pub mod replication_slot;
pub mod schema;
pub mod sorter;
pub mod types;
pub mod util;
pub mod validate;
pub mod table_filter;
pub const DB_PREFIX: &str = "pg_cdc_";

#[derive(Debug, Error)]
pub enum PostgresError {
    #[error("Validation Error: {0}")]
    ValidationError(#[from] validate::ValidationError),
    #[error("Failed to connect to DB: {0}")]
    ConnectError(#[from] client::ConnectError),
    #[error("Postgres Error {0}")]
    PostgresError(#[from] tokio_postgres::Error),
}
