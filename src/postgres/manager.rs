use futures::Stream;
use tokio_postgres::config::ReplicationMode;
use tracing::info;

use crate::postgres::{
    client,
    mapper::XlogStreamMapper,
    replication::ReplicationSlotParams,
    replication_slot::get_replication_slot,
    schema::SchemaHelper,
    validate::{validate_pg_version, validate_wal_logical},
    DB_PREFIX,
};

use super::{replication_slot::create_replication_slot, types::OperationMessage, PostgresError};

// Check if there is at least one replication slot available
// -- Fetch max replication slots
// SHOW max_replication_slots;
//
// --- Get current used replication slots count
// SELECT COUNT(*) FROM pg_replication_slots;
//
// -- If there is no empty slot, you can drop any unused slot with
// SELECT * FROM pg_drop_replication_slot('slot_name');
//
// Check if we have a publication. If not create one.

// Check if we have a replication slot. If not create one.

// Start the replication stream.

pub async fn setup(
    mut config: tokio_postgres::Config,
    pubication_name: String,
) -> Result<impl Stream<Item = OperationMessage>, PostgresError> {
    let client = client::connect(config.clone()).await?;
    // Check Server version > 10;
    //   SHOW server_version;
    validate_pg_version(&client).await?;
    // Check WAL level is logical
    //   SHOW wal_level;
    validate_wal_logical(&client).await?;

    //TODO check_publication

    let helper = SchemaHelper::new(config.clone(), None).await;
    let tables = helper.get_tables(None).await.unwrap();
    //let slot_name = "slot";
    let slot_name = format!("{}{}", DB_PREFIX, "test_slot");

    let lsn = get_replication_slot(&client, &slot_name).await?;
    info!("LSN: {:#?}", lsn);

    config.replication_mode(ReplicationMode::Logical);
    let client = client::connect(config).await?;

    let lsn = if let Some(lsn) = lsn {
        lsn
    } else {
        let lsn = create_replication_slot(&client, &slot_name).await?;
        lsn.unwrap()
    };

    let options = ReplicationSlotParams {
        slot_name,
        start_lsn: lsn,
        proto_version: 1.try_into().expect("Can't be 0"),
        publication_names: pubication_name,
    };

    let stream = XlogStreamMapper::start(tables, client, options).await?;

    Ok(stream)
}
