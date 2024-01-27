use thiserror::Error;
use tokio_postgres::{config::SslMode, Client, NoTls};

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error("Invalid SslMode: {0:?}")]
    InvalidSslError(SslMode),
    #[error("Postgres Error {0}")]
    PostgresError(#[from] tokio_postgres::Error),
}

pub async fn connect(config: tokio_postgres::Config) -> Result<Client, ConnectError> {
    match config.get_ssl_mode() {
        SslMode::Disable => {
            let (client, connection) = config.connect(NoTls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!("Postgres connection error: {}", e);
                }
            });
            Ok(client)
        }
        //TODO implement TLS
        mode => Err(ConnectError::InvalidSslError(mode)),
    }
}
