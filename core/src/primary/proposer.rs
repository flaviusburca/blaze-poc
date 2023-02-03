use std::cmp::Ordering;
use log::error;
// Copyright(C) Mundis
use {
    log::debug,
    mundis_model::{
        base_types::Epoch,
        certificate::{Certificate, Header},
        committee::Committee,
        hash::{Hash, Hashable},
        keypair::Keypair,
        pubkey::Pubkey,
        signature::Signer,
        Round, View, WorkerId,
    },
    std::time::Duration,
    tokio::{
        sync::mpsc::{Receiver, Sender},
        time::{sleep, Instant},
    },
};

/// The proposer creates new headers and sends them to the core for broadcasting and further processing.
pub struct Proposer {
    /// The public key of this primary.
    authority: Keypair,
    /// The committee information.
    committee: Committee,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: u64,

    /// Receives the parents to include in the next header (along with their round number).
    rx_core: Receiver<(Vec<Certificate>, Round)>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(Hash, WorkerId)>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,
    tx_commit_view: Sender<Certificate>,

    // The current round
    round: Round,

    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Certificate>,
    /// Holds the batches' digests waiting to be included in the next header.
    digests: Vec<(Hash, WorkerId)>,
    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,
}

impl Proposer {
    pub fn spawn(
        authority: Keypair,
        committee: Committee,
        header_size: usize,
        max_header_delay: u64,
        rx_core: Receiver<(Vec<Certificate>, Round)>,
        rx_workers: Receiver<(Hash, WorkerId)>,
        tx_core: Sender<Header>,
        tx_commit_view: Sender<Certificate>,
    ) {
        let genesis = Certificate::genesis(&committee);
        tokio::spawn(async move {
            Self {
                authority,
                committee,
                header_size,
                max_header_delay,
                rx_core,
                rx_workers,
                tx_core,
                tx_commit_view,
                round: 1,
                last_parents: genesis,
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
            }
            .run()
            .await;
        });
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        error!("Dag starting round {}", self.round);

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);

        loop {
            // Check if we can propose a new header. We propose a new header when one of the following conditions is met:
            // 1. We have a quorum of certificates from the previous round and enough batches' digests;
            // 2. We have a quorum of certificates from the previous round and the specified maximum inter-header delay has passed.
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = self.payload_size >= self.header_size;
            let timer_expired = timer.is_elapsed();

            if (timer_expired || enough_digests) && enough_parents {
                // Make a new header.
                self.make_header().await;
                self.payload_size = 0;

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                Some((parents, round)) = self.rx_core.recv() => {
                    if round < self.round {
                        continue;
                    }

                    // Advance to the next round.
                    self.round = round + 1;

                    // error!("Advancing to round {}", self.round);

                    // Signal that we have enough parent certificates to propose a new header.
                    self.last_parents = parents;
                }
                Some((digest, worker_id)) = self.rx_workers.recv() => {
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                }
                () = &mut timer => {
                    // Nothing to do.
                }
            }
        }
    }

    async fn make_header(&mut self) {
        // Make a new header.
        let mut header = Header::new(
            self.authority.clone(),
            self.round,
            self.digests.drain(..).collect(),
            self.last_parents.drain(..).map(|x| x.hash()).collect(),
        );

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
        }

        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }
}

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;
