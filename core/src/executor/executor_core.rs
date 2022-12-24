use std::time::Duration;
use log::{debug, info};
use tokio::sync::mpsc::Receiver;
use tokio::time::Instant;
use mundis_model::certificate::Certificate;
use mundis_model::hash::Hash;
use mundis_model::transaction::Transaction;
use crate::executor::batch_loader::SerializedBatchMessage;
use crate::worker::WorkerMessage;

#[derive(Debug)]
pub struct ExecutorCoreMessage {
    /// The serialized batch message.
    pub batch: SerializedBatchMessage,
    /// The digest of the batch.
    pub digest: Hash,
    /// The certificate referencing this batch.
    pub certificate: Certificate,
}

/// Executes batches of transactions.
pub struct ExecutorCore {
    /// Input channel to receive (serialized) batches to execute.
    rx_batch_loader: Receiver<ExecutorCoreMessage>,
}

impl ExecutorCore {
    pub fn spawn(rx_batch_loader: Receiver<ExecutorCoreMessage>) {
        tokio::spawn(async move {
            Self { rx_batch_loader }.run().await;
        });
    }

    /// Main loop receiving batches to execute.
    async fn run(&mut self) {
        while let Some(core_message) = self.rx_batch_loader.recv().await {
            let ExecutorCoreMessage {
                batch,
                digest,
                certificate,
            } = core_message;

            match bincode::deserialize(&batch) {
                Ok(WorkerMessage::Batch(batch)) => {
                    debug!("Executing batch {digest} ({} tx)", batch.len());

                    // Deserialize each transaction.
                    for serialized_tx in batch {
                        let bytes = &serialized_tx;
                        let transaction: Transaction = match bincode::deserialize(bytes) {
                            Ok(x) => x,
                            Err(e) => {
                                log::warn!("Failed to deserialize transaction: {e}");
                                continue;
                            }
                        };

                        // Execute the transaction.
                        if transaction.execution_time != 0 {
                            let duration = Duration::from_millis(transaction.execution_time);
                            Self::burn_cpu(duration).await;
                        }

                        info!("Executed {}", certificate.header);
                        let _digest = digest.clone();
                    }
                }
                Ok(_) => panic!("Unexpected protocol message"),
                Err(e) => panic!("Failed to deserialize batch: {}", e),
            }
        }
    }

    /// Simple function simulating a CPU-intensive execution.
    async fn burn_cpu(duration: Duration) {
        let now = Instant::now();
        let mut _x = 0;
        loop {
            while _x < 1_000_000 {
                _x += 1;
            }
            _x = 0;
            if now.elapsed() >= duration {
                return;
            }
        }
    }
}