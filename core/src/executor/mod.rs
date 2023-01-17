use {
    crate::executor::{
        batch_loader::{BatchLoader, SerializedBatchMessage},
        executor_core::{ExecutorCore, ExecutorCoreMessage},
    },
    async_trait::async_trait,
    bytes::Bytes,
    log::info,
    mundis_ledger::Store,
    mundis_model::{certificate::Certificate, committee::Committee, pubkey::Pubkey},
    mundis_network::receiver::{MessageHandler, NetworkReceiver, Writer},
    std::error::Error,
    tokio::sync::mpsc::{channel, Receiver, Sender},
};

mod batch_loader;
mod executor_core;

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
        let (tx_executor_core, rx_executor_core) = channel::<ExecutorCoreMessage>(CHANNEL_CAPACITY);

        // Spawn the network receiver listening to messages from the other workers.
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

        // The `BatchLoader` downloads the batches of all certificates referenced by sequenced
        // certificates into the local store.
        BatchLoader::spawn(
            authority,
            committee,
            store,
            rx_consensus,
            rx_worker,
            tx_executor_core,
            5_000,
        );

        // The execution `Core` execute every sequenced transaction.
        ExecutorCore::spawn(/* rx_batch_loader */ rx_executor_core);

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
