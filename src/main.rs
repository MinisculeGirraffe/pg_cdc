#![warn(clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::must_use_candidate
)]
use futures::StreamExt;
use tracing_subscriber::EnvFilter;

use crate::threadpool::TokioRayonHandle;
pub mod config;
pub mod nats;
pub mod postgres;
pub mod threadpool;

fn get_config() -> tokio_postgres::Config {
    let mut config = tokio_postgres::Config::new();
    config.dbname("postgres");
    config.host("localhost");
    config.port(5432);
    config.user("postgres");
    config.password("password");
    config.ssl_mode(tokio_postgres::config::SslMode::Disable);

    config
}

fn init_tracing() {
    let sub = tracing_subscriber::fmt::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(sub).unwrap();
}

#[tokio::main]
async fn main() {
    init_tracing();

    let client = async_nats::connect("localhost:4222").await.unwrap();

    postgres::manager::setup(get_config(), "cdc_test".to_string())
        .await
        .unwrap()
        .then(|msg| {
            TokioRayonHandle::spawn(move || {
                (msg.nats_subject(), serde_json::to_string(&msg).unwrap())
            })
        })
        .then(|(sub, json)| async {
            client.publish(sub, json.into()).await.unwrap();
        })
        .collect::<()>()
        .await;
}
