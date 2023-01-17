use {
    crate::{
        primary::PrimaryWorkerMessage,
        worker::{
            batch_maker::{Batch, BatchMaker, Transaction},
            worker_helper::{ExecutorHelper, WorkerHelper},
            primary_connector::PrimaryConnector,
            processor::{SerializedBatchMessage, WorkerProcessor},
            quorum_waiter::{QuorumWaiter, QuorumWaiterMessage},
            worker_synchronizer::WorkerSynchronizer,
        },
    },
    async_trait::async_trait,
    bytes::Bytes,
    futures::SinkExt as _,
    log::{error, info, warn},
    mundis_ledger::Store,
    mundis_model::{committee::Committee, hash::Hash, pubkey::Pubkey, WorkerId},
    mundis_network::receiver::{MessageHandler, NetworkReceiver, Writer},
    serde::{Deserialize, Serialize},
    std::error::Error,
    tokio::sync::mpsc::{channel, Sender},
};

mod batch_maker;
mod worker_helper;
mod primary_connector;
mod processor;
mod quorum_waiter;
mod worker_synchronizer;

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// Indicates a serialized `WorkerPrimaryMessage` message.
pub type SerializedBatchDigestMessage = Vec<u8>;

/// The message exchanged between workers.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Batch(Batch),
    BatchRequest(Vec<Hash>, /* origin */ Pubkey),
    ExecutorRequest(Vec<Hash>, /* origin */ Pubkey),
}

pub struct Worker {
    /// The public key of this authority.
    authority: Pubkey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
}

impl Worker {
    pub fn spawn(
        authority: Pubkey,
        id: WorkerId,
        committee: Committee,
        store: Store,
    ) -> anyhow::Result<()> {
        // Define a worker instance.
        let worker = Self {
            authority,
            id,
            committee,
            store,
        };

        // Spawn all worker tasks.
        let (tx_primary, rx_primary) = channel(CHANNEL_CAPACITY);
        worker.handle_primary_messages();
        worker.handle_clients_transactions(tx_primary.clone());
        worker.handle_workers_messages(tx_primary);

        // The `PrimaryConnector` allows the worker to send messages to its primary.
        PrimaryConnector::spawn(
            worker
                .committee
                .primary_address(&worker.authority)
                .expect("Our public key is not in the committee")
                .worker_to_primary,
            rx_primary,
        );

        info!("Worker {} successfully started", worker.id);

        Ok(())
    }

    /// Spawn all tasks responsible to handle messages from our primary.
    fn handle_primary_messages(&self) {
        let (tx_synchronizer, rx_synchronizer) = channel::<PrimaryWorkerMessage>(CHANNEL_CAPACITY);

        // Receive incoming messages from our primary.
        let address = self
            .committee
            .worker(&self.authority, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_worker;
        NetworkReceiver::spawn(
            address,
            /* handler */
            PrimaryReceiverHandler { tx_synchronizer },
        );
        // The `Synchronizer` is responsible to keep the worker in sync with the others. It handles the commands
        // it receives from the primary (which are mainly notifications that we are out of sync).
        WorkerSynchronizer::spawn(
            self.authority,
            self.id,
            self.committee.clone(),
            self.store.clone(),
            50,
            5_000,
            3,
            /* rx_message */ rx_synchronizer,
        );
    }

    /// Spawn all tasks responsible to handle clients transactions.
    fn handle_clients_transactions(&self, tx_primary: Sender<SerializedBatchDigestMessage>) {
        let (tx_batch_maker, rx_batch_maker) = channel::<Transaction>(CHANNEL_CAPACITY);
        let (tx_quorum_waiter, rx_quorum_waiter) = channel::<QuorumWaiterMessage>(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        // We first receive clients' transactions from the network.
        let address = self
            .committee
            .worker(&self.authority, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;
        NetworkReceiver::spawn(
            address,
            /* handler */ TxReceiverHandler { tx_batch_maker },
        );

        // The transactions are sent to the `BatchMaker` that assembles them into batches. It then broadcasts
        // (in a reliable manner) the batches to all other workers that share the same `id` as us. Finally, it
        // gathers the 'cancel handlers' of the messages and send them to the `QuorumWaiter`.
        BatchMaker::spawn(
            500_000,
            100,
            /* rx_transaction */ rx_batch_maker,
            /* tx_message */ tx_quorum_waiter,
            /* workers_addresses */
            self.committee
                .others_workers(&self.authority, &self.id)
                .iter()
                .map(|(name, addresses)| (*name, addresses.worker_to_worker))
                .collect(),
        );

        // The `QuorumWaiter` waits for 2f authorities to acknowledge reception of the batch. It then forwards
        // the batch to the `Processor`.
        QuorumWaiter::spawn(
            self.committee.clone(),
            /* stake */ self.committee.stake(&self.authority),
            /* rx_message */ rx_quorum_waiter,
            /* tx_batch */ tx_processor,
        );

        // The `Processor` hashes and stores the batch. It then forwards the batch's digest to the `PrimaryConnector`
        // that will send it to our primary machine.
        WorkerProcessor::spawn(
            self.id,
            self.store.clone(),
            /* rx_batch */ rx_processor,
            /* tx_digest */ tx_primary,
            /* own_batch */ true,
        );

        info!(
            "Worker {} listening to client transactions on {}",
            self.id, address
        );
    }

    /// Spawn all tasks responsible to handle messages from other workers.
    fn handle_workers_messages(&self, tx_primary: Sender<SerializedBatchDigestMessage>) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);
        let (tx_executor_helper, rx_executor_helper) = channel(CHANNEL_CAPACITY);

        // Receive incoming messages from other workers.
        let address = self
            .committee
            .worker(&self.authority, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_worker;
        NetworkReceiver::spawn(
            address,
            /* handler */
            WorkerReceiverHandler {
                tx_helper,
                tx_processor,
                tx_executor_helper,
            },
        );

        // The `Helper` is dedicated to reply to batch requests from other workers.
        WorkerHelper::spawn(
            self.id,
            self.committee.clone(),
            self.store.clone(),
            /* rx_request */ rx_helper,
        );

        // Used by the executor of this authority to fetch transactions' data.
        ExecutorHelper::spawn(
            self.authority,
            self.committee.clone(),
            self.store.clone(),
            /* rx_request */ rx_executor_helper,
        );

        // This `Processor` hashes and stores the batches we receive from the other workers. It then forwards the
        // batch's digest to the `PrimaryConnector` that will send it to our primary.
        WorkerProcessor::spawn(
            self.id,
            self.store.clone(),
            /* rx_batch */ rx_processor,
            /* tx_digest */ tx_primary,
            /* own_batch */ false,
        );

        info!(
            "Worker {} listening to worker messages on {}",
            self.id, address
        );
    }
}

/// Defines how the network receiver handles incoming transactions.
#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<Transaction>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Send the transaction to the batch maker.
        self.tx_batch_maker
            .send(message.to_vec())
            .await
            .expect("Failed to send transaction");

        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_helper: Sender<(Vec<Hash>, Pubkey)>,
    tx_processor: Sender<SerializedBatchMessage>,
    tx_executor_helper: Sender<(Vec<Hash>, Pubkey)>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized) {
            Ok(WorkerMessage::Batch(..)) => self
                .tx_processor
                .send(serialized.to_vec())
                .await
                .expect("Failed to send batch"),
            Ok(WorkerMessage::BatchRequest(missing, requestor)) => self
                .tx_helper
                .send((missing, requestor))
                .await
                .expect("Failed to send batch request"),
            Ok(WorkerMessage::ExecutorRequest(missing, requestor)) => self
                .tx_executor_helper
                .send((missing, requestor))
                .await
                .expect("Failed to send executor request"),
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_synchronizer: Sender<PrimaryWorkerMessage>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // Deserialize the message and send it to the synchronizer.
        match bincode::deserialize(&serialized) {
            Err(e) => error!("Failed to deserialize primary message: {}", e),
            Ok(message) => self
                .tx_synchronizer
                .send(message)
                .await
                .expect("Failed to send transaction"),
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/common.rs"]
mod common;

#[cfg(test)]
#[path = "tests/worker_tests.rs"]
pub mod worker_tests;
