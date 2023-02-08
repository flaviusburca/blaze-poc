// Copyright(C) Mundis.
use anyhow::{Context, Result};
use tokio::sync::mpsc::channel;

use mundis_core::executor::Executor;
use mundis_core::master::MasterNode;
use mundis_core::worker::WorkerNode;
use mundis_ledger::Store;
use mundis_model::certificate::Certificate;
use mundis_model::config::ValidatorConfig;
use mundis_model::signature::Signer;
use mundis_model::WorkerId;

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct Validator {}

impl Validator {
    pub async fn start_validator(config: &ValidatorConfig) -> Result<Validator> {
        // Channels the sequence of certificates.
        let (tx_output, rx_output) = channel::<Certificate>(CHANNEL_CAPACITY);

        let primary_store_path = format!("{}/primary", config.ledger_path);
        let primary_store =
            Store::new(&primary_store_path).context("Could not create the primary ledger store")?;

        MasterNode::spawn(&config, primary_store, tx_output)?;

        let executor_store_path = format!("{}/executor", config.ledger_path);
        let executor_store = Store::new(&executor_store_path)
            .context("Could not create the primary ledger store")?;

        Executor::spawn(
            config.identity.pubkey(),
            config.initial_committee.clone(),
            executor_store,
            rx_output,
        )?;

        let workers = config.initial_committee.our_workers(&config.identity.pubkey())?;
        for i in 0..workers.len() {
            let worker_store_path = format!("{}/worker_{}", config.ledger_path, i);
            let worker_store = Store::new(&worker_store_path).context(format!(
                "Could not create worker ledger store for worker {}",
                i
            ))?;

            WorkerNode::spawn(
                config.identity.pubkey(),
                i as WorkerId,
                config.initial_committee.clone(),
                worker_store,
            )?;
        }

        println!("Started validator {}", config.identity.pubkey());

        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }
    }
}
