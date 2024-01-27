use std::str::FromStr;

use postgres_protocol::escape::escape_identifier;
use postgres_types::PgLsn;
use tokio_postgres::{Client, SimpleQueryMessage};

pub async fn create_replication_slot(
    client: &Client,
    slot_name: &str,
) -> Result<Option<PgLsn>, tokio_postgres::Error> {
    let slot_query = format!(
        "CREATE_REPLICATION_SLOT {} LOGICAL \"pgoutput\"",
        escape_identifier(slot_name)
    );

    let consistent_point = client
        .simple_query(&slot_query)
        .await?
        .into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>()
        .first()
        .and_then(|i| i.get("consistent_point"))
        .map(PgLsn::from_str)
        .transpose()
        .ok()
        .flatten();

    Ok(consistent_point)
}

pub async fn drop_replication_slot(
    client: &Client,
    slot_name: &str,
) -> Result<(), tokio_postgres::Error> {
    let slot_query = format!(
        "DROP_REPLICATION_SLOT {} WAIT;",
        escape_identifier(slot_name)
    );

    client.simple_query(&slot_query).await?;

    Ok(())
}

/// Returns the `restart_lsn` of the replication slot.
/// If the replication slot does not exist, returns None.
pub async fn get_replication_slot(
    client: &Client,
    slot_name: &str,
) -> Result<Option<PgLsn>, tokio_postgres::Error> {
    let lsn = client
        .query(
            "SELECT * FROM pg_replication_slots where slot_name = $1;",
            &[&slot_name],
        )
        .await?
        .into_iter()
        .next()
        .and_then(|row| row.get::<&str, Option<PgLsn>>("restart_lsn"));

    Ok(lsn)
}

