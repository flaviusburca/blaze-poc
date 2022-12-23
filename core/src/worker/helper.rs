use bytes::Bytes;
use log::{debug, error, warn};
use tokio::sync::mpsc::Receiver;
use mundis_ledger::Store;
use mundis_model::committee::Committee;
use mundis_model::hash::Hash;
use mundis_model::pubkey::Pubkey;
use mundis_model::WorkerId;
use mundis_network::simple_sender::SimpleSender;

/// A task dedicated to help other authorities by replying to their batch requests.
pub struct Helper {
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive batch requests.
    rx_request: Receiver<(Vec<Hash>, Pubkey)>,
    /// A network sender to send the batches to the other workers.
    network: SimpleSender,
}

impl Helper {
    pub fn spawn(
        id: WorkerId,
        committee: Committee,
        store: Store,
        rx_request: Receiver<(Vec<Hash>, Pubkey)>,
    ) {
        tokio::spawn(async move {
            Self {
                id,
                committee,
                store,
                rx_request,
                network: SimpleSender::new(),
            }
                .run()
                .await;
        });
    }

    async fn run(&mut self) {
        while let Some((digests, origin)) = self.rx_request.recv().await {
            // TODO [issue #7]: Do some accounting to prevent bad nodes from monopolizing our resources.

            // Get the requestors address.
            let address = match self.committee.worker(&origin, &self.id) {
                Ok(x) => x.worker_to_worker,
                Err(e) => {
                    warn!("Unexpected batch request: {}", e);
                    continue;
                }
            };

            // Reply to the request (the best we can).
            for digest in digests {
                match self.store.read(digest.to_vec()).await {
                    Ok(Some(data)) => self.network.send(address, Bytes::from(data)).await,
                    Ok(None) => (),
                    Err(e) => error!("{}", e),
                }
            }
        }
    }
}

pub struct ExecutorHelper {
    /// The public key of this authority.
    authority: Pubkey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive batch requests.
    rx_request: Receiver<(Vec<Hash>, Pubkey)>,
    /// A network sender to send the batches to the other workers.
    network: SimpleSender,
}

impl ExecutorHelper {
    pub fn spawn(
        authority: Pubkey,
        committee: Committee,
        store: Store,
        rx_request: Receiver<(Vec<Hash>, Pubkey)>,
    ) {
        tokio::spawn(async move {
            Self {
                authority,
                committee,
                store,
                rx_request,
                network: SimpleSender::new(),
            }
                .run()
                .await;
        });
    }

    async fn run(&mut self) {
        while let Some((digests, origin)) = self.rx_request.recv().await {
            // Only the executor of this validator can send requests.
            if origin != self.authority {
                warn!("Unexpected batch request from: {}", origin);
                continue;
            }

            debug!("Received batch request from executor");

            // Reply to the request (the best we can).
            let address = self
                .committee
                .executor(&self.authority)
                .expect("Our public key is not in the committee")
                .worker_to_executor;

            for digest in digests {
                match self.store.read(digest.to_vec()).await {
                    Ok(Some(data)) => self.network.send(address, Bytes::from(data)).await,
                    Ok(None) => (),
                    Err(e) => error!("{}", e),
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/helper_tests.rs"]
pub mod helper_tests;