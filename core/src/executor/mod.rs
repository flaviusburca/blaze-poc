use std::error::Error;
use async_trait::async_trait;
use bytes::Bytes;
use log::info;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use mundis_ledger::Store;
use mundis_model::certificate::Certificate;
use mundis_model::committee::Committee;
use mundis_model::pubkey::Pubkey;
use mundis_network::receiver::{MessageHandler, NetworkReceiver, Writer};
use crate::executor::batch_loader::{BatchLoader, SerializedBatchMessage};
use crate::executor::core::{Core, CoreMessage};

mod batch_loader;
mod core;

/// The default channel capacity for each channel of the executor.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct Executor;

impl Executor {
    pub fn spawn(
        authority: Pubkey,
        committee: Committee,
        store: Store,
        rx_consensus: Receiver<Certificate>,
    ) -> anyhow::Result<()> {
        let (tx_worker, rx_worker) = channel::<SerializedBatchMessage>(CHANNEL_CAPACITY);
        let (tx_core, rx_core) = channel::<CoreMessage>(CHANNEL_CAPACITY);

        // Spawn the network receiver listening to messages from the other primaries.
        let address = committee
            .executor(&authority)
            .expect("Our public key is not in the committee")
            .worker_to_executor;
        NetworkReceiver::spawn(
            address,
            /* handler */
            ExecutorReceiverHandler { tx_worker },
        );
        info!("Executor {} listening to batches on {}", authority, address);

        // The `BatchLoader` download the batches of all certificates referenced by sequenced
        // certificates into the local store.
        BatchLoader::spawn(
            authority,
            committee,
            store,
            rx_consensus,
            rx_worker,
            tx_core,
            5_000,
        );

        // The execution `Core` execute every sequenced transaction.
        Core::spawn(/* rx_batch_loader */ rx_core);

        Ok(())
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct ExecutorReceiverHandler {
    tx_worker: Sender<SerializedBatchMessage>,
}

#[async_trait]
impl MessageHandler for ExecutorReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        self.tx_worker
            .send(serialized.to_vec())
            .await
            .expect("failed to send batch to executor");
        Ok(())
    }
}