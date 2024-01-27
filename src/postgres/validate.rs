use thiserror::Error;
use tokio_postgres::{Client, SimpleQueryMessage};

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Postgres version is too low. Must be >= 10.0")]
    ServerVersionTooLow,
    #[error("Postgres wal_level is not logical. Must be logical")]
    WalLevelIsNotLogical,
    #[error("Postgres error: {0}")]
    PostgresError(#[from] tokio_postgres::Error),
}

pub async fn validate_pg_version(client: &Client) -> Result<(), ValidationError> {
    let valid_version = client
        .simple_query("SHOW server_version;")
        .await?
        .into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>()
        .first()
        .and_then(|i| i.get("server_version"))
        .and_then(|i| i.split_whitespace().next())
        .and_then(|i| i.parse::<f64>().ok())
        .is_some_and(|i| i >= 10.0);

    if valid_version {
        return Ok(());
    }
    Err(ValidationError::ServerVersionTooLow)
}

pub async fn validate_wal_logical(client: &Client) -> Result<(), ValidationError> {
    let valid_wal_level = client
        .simple_query("SHOW wal_level;")
        .await?
        .into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>()
        .first()
        .and_then(|i| i.get("wal_level"))
        .is_some_and(|i| i == "logical");

    if valid_wal_level {
        return Ok(());
    }
    Err(ValidationError::WalLevelIsNotLogical)
}
