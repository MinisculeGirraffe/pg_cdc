use chrono::{TimeZone, Utc};
use futures::{Stream, StreamExt};
use postgres_protocol::escape::{escape_identifier, escape_literal};
use postgres_protocol::{
    message::backend::{LogicalReplicationMessage, ReplicationMessage, XLogDataBody},
    Lsn,
};
use std::{num::NonZeroU8, pin::Pin, time::SystemTime};
use thiserror::Error;

use tokio_postgres::{types::PgLsn, Client};
use tracing::{debug, info, instrument, warn};

use super::util::is_network_failure;

#[derive(Debug, Error)]
pub enum ReplicationError {
    #[error("Failed to send keep-alive to database")]
    KeepAliveFailed,
}

pub struct ReplicationProgress {
    pub start_lsn: PgLsn,
    pub begin_lsn: Lsn,
    pub offset_lsn: Lsn,
    pub last_commit_lsn: Lsn,
    pub offset: u64,
    pub seq_no: u64,
}
pub struct ReplicationSlotParams {
    pub slot_name: String,
    pub start_lsn: PgLsn,
    /// The protocol version to use for the replication connection.
    /// The default is 1, which is the only protocol version supported by PostgreSQL 10.
    /// The newest version is 4, but I don't think the library we're using supports it.
    pub proto_version: NonZeroU8,
    pub publication_names: String,
}

/// The primary stream used to retrieve logical replication messages from the database.
/// This stream will handle keep alive messages and record keeping and only return xlog data messages.
/// Downstream consumers can wrap this stream to perform data transformation.
/// This is still not a very high level abstraction, as you must still handle the xlog data messages yourself.
pub struct LogicalReplicationStream {
    progress: ReplicationProgress,
    inner: Pin<Box<tokio_postgres::replication::LogicalReplicationStream>>,
}

impl LogicalReplicationStream {
    pub async fn start(
        client: Client,
        options: ReplicationSlotParams,
    ) -> Result<
        impl Stream<Item = Result<XLogDataBody<LogicalReplicationMessage>, tokio_postgres::Error>>,
        tokio_postgres::Error,
    > {
        let inner = Box::pin(Self::open_replication_stream(&client, &options).await?);

        let progress = ReplicationProgress {
            start_lsn: options.start_lsn,
            begin_lsn: 0,
            offset_lsn: 0,
            last_commit_lsn: 0,
            offset: 0,
            seq_no: 0,
        };

        let stream = Self {
            progress,
            inner,
        };

        let stream = futures::stream::unfold(stream, |mut stream| {
            Box::pin(async {
                loop {
                    match stream.next().await {
                        //The stream is alive, we got some data, and it's a message we want to pass on.
                        Some(Ok(Some(data))) => {
                            return Some((Ok(data), stream));
                        }
                        // The stream is alive, we got some data, but it's not a message we want to pass on.
                        // Keep alive messages are handled for record keeping and don't need to be sent down stream
                        Some(Ok(None)) => {
                            continue;
                        }
                        // The stream is alive, but we got an error. If it's a network error, restart the stream.
                        // Otherwise, pass the error down stream.
                        Some(Err(e)) => {
                            warn!("Error in replication stream: {}", e);
                            if is_network_failure(&e) {
                                warn!("Network failure, restarting stream");
                                return None;
                            }
                            return Some((Err(e), stream));
                        }
                        // The stream is dead, pass None down stream.
                        None => {
                            warn!("Stream ended");
                            return None;
                        }
                    }
                }
            })
        });

        Ok(stream)
    }
    #[instrument(skip(client, options), level = "info")]
    async fn open_replication_stream(
        client: &Client,
        options: &ReplicationSlotParams,
    ) -> Result<tokio_postgres::replication::LogicalReplicationStream, tokio_postgres::Error> {
        let query = format!(
            r#"START_REPLICATION SLOT {} LOGICAL {} ("proto_version" {},"publication_names" {})"#,
            escape_identifier(&options.slot_name),
            &options.start_lsn,
            escape_literal(&options.proto_version.to_string()),
            escape_literal(&options.publication_names)
        );
        info!("Starting replication stream: {}", query);
        let copy_stream = client.copy_both_simple::<bytes::Bytes>(&query).await?;

        Ok(tokio_postgres::replication::LogicalReplicationStream::new(
            copy_stream,
        ))
    }
    #[instrument(skip(self))]
    pub async fn standby_status_update(
        &mut self,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        apply_lsn: PgLsn,
        ts: i64,
        reply: u8,
    ) -> Result<(), tokio_postgres::Error> {
        debug!("Sending standby status update");
        self.inner
            .as_mut()
            .standby_status_update(write_lsn, flush_lsn, apply_lsn, ts, reply)
            .await?;
        {}
        Ok(())
    }

    pub async fn next(
        &mut self,
    ) -> Option<Result<Option<XLogDataBody<LogicalReplicationMessage>>, tokio_postgres::Error>>
    {
        let message = self.inner.next().await?;

        match message {
            Ok(message) => match message {
                ReplicationMessage::XLogData(data) => {
                    match data.data() {
                        LogicalReplicationMessage::Begin(_) => {
                            self.progress.begin_lsn = data.wal_start();
                            self.progress.seq_no = 0;
                        }
                        LogicalReplicationMessage::Commit(msg) => {
                            self.progress.last_commit_lsn = msg.end_lsn();
                            if let Err(e) = self.send_keep_alive().await {
                                warn!("Failed to send keep-alive to database: {}", e);
                                return Some(Err(e));
                            }
                        }

                        LogicalReplicationMessage::Insert(_)
                        | LogicalReplicationMessage::Update(_)
                        | LogicalReplicationMessage::Delete(_)
                        | LogicalReplicationMessage::Truncate(_) => {
                            self.progress.seq_no += 1;
                        }
                        _ => {}
                    }

                    Some(Ok(Some(data)))
                }
                ReplicationMessage::PrimaryKeepAlive(msg) => {
                    if msg.reply() != 0 {
                        if let Err(e) = self.send_keep_alive().await {
                            warn!("Failed to send keep-alive to database: {}", e);
                            return Some(Err(e));
                        }
                    };

                    Some(Ok(None))
                }
                _ => {
                    warn!("Unknown Message Received");
                    Some(Ok(None))
                }
            },
            Err(e) => Some(Err(e)),
        }
    }

    async fn send_keep_alive(&mut self) -> Result<(), tokio_postgres::Error> {
        let ts = SystemTime::now()
            .duration_since(SystemTime::from(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap(),
            ))
            .expect("Time went backwards")
            .as_millis() as i64;

        self.standby_status_update(
            self.progress.last_commit_lsn.into(),
            self.progress.last_commit_lsn.into(),
            self.progress.last_commit_lsn.into(),
            ts,
            0,
        )
        .await
    }
}
