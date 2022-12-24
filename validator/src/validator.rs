use anyhow::{Context, Result};
use tokio::sync::mpsc::channel;

use mundis_core::consensus::Consensus;
use mundis_core::executor::Executor;
use mundis_core::primary::Primary;
use mundis_core::worker::Worker;
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
        let (tx_new_certificates, rx_new_certificates) = channel::<Certificate>(CHANNEL_CAPACITY);
        let (tx_feedback, rx_feedback) = channel::<Certificate>(CHANNEL_CAPACITY);

        let primary_store_path = format!("{}_primary", config.ledger_path);
        let primary_store = Store::new(&primary_store_path)
            .context("Could not create the primary ledger store")?;

        Primary::spawn(
            &config,
            primary_store,
            tx_new_certificates,
            rx_feedback
        )?;

        Consensus::spawn(
            config.initial_committee.clone(),
            50,
            rx_new_certificates,
            tx_feedback,
            tx_output
        );

        let executor_store_path = format!("{}_executor", config.ledger_path);
        let executor_store = Store::new(&executor_store_path)
            .context("Could not create the primary ledger store")?;

        Executor::spawn(
            config.identity.pubkey(),
            config.initial_committee.clone(),
            executor_store,
            rx_output
        )?;

        for i in 0..config.num_workers {
            let worker_store_path = format!("{}_worker_{}", config.ledger_path, i);
            let worker_store = Store::new(&worker_store_path)
                .context(format!("Could not create worker ledger store for worker {}", i))?;

            Worker::spawn(
                config.identity.pubkey(),
                i as WorkerId,
                config.initial_committee.clone(),
                worker_store
            )?;
        }


        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }
    }
}