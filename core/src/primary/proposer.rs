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
        Round,
        WorkerId,
    },
    std::time::Duration,
    tokio::{
        sync::mpsc::{Receiver, Sender},
        time::{Instant, sleep},
    },
};
use mundis_model::View;

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
    rx_core: Receiver<(Vec<Certificate>, Round, View)>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(Hash, WorkerId)>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,
    tx_commit_view: Sender<Certificate>,

    // The current round
    round: Round,
    /// The current view
    view: View,

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
        rx_core: Receiver<(Vec<Certificate>, Round, i64)>,
        rx_workers: Receiver<(Hash, WorkerId)>,
        tx_core: Sender<Header>,
        tx_commit_view: Sender<Certificate>
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
                view: 1,
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
        debug!("Dag starting at view {} and round {}", self.view, self.round);

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
                Some((parents, round, view)) = self.rx_core.recv() => {
                    // at this point the round has 2f+1 certificates

                    // Compare the parents' round number with our current round.
                    if round < self.round {
                        continue;
                    }

                    if view < self.view {
                        // Advance to the next round.
                        self.round += 1;

                        // Signal that we have enough parent certificates to propose a new header.
                        self.last_parents = parents;

                        continue;
                    }

                    debug!("View={}, Round={}, enough_parents={}, enough_digests={}, timer_expired={}",
                        self.view.abs(), round, enough_parents, enough_digests, timer_expired
                    );

                    if !parents.is_empty() {
                        if self.view >= 0 {
                            let (leader_key, leader_name) = self.elect_leader();
                            debug!("(v={}, r={}) Elected leader={}", self.view, round, leader_name);

                            let leader_certificate: Option<&Certificate> = parents
                                .iter()
                                .find(|x| (x.view() == self.view) && (x.origin() == leader_key))
                                .clone();

                            if leader_certificate.is_some() {
                                let leader_view = leader_certificate.unwrap().header.view + 1;
                                debug!("(v={}, r={}) Committing view {} and advancing to leader's proposed view {}",
                                    self.view, round, self.view, leader_view
                                );
                                self.tx_commit_view.send(leader_certificate.unwrap().clone())
                                    .await
                                    .expect("Failed to commit certificate");

                                self.view = leader_view;
                            } else {
                                debug!("(v={}, r={}) Marking a complaint for view {}", self.view, round, self.view);
                                self.view = -self.view;
                            }
                        } else {
                            let mut votes = 0;
                            let mut latest_view = self.view;
                            for certificate in &parents {
                                let stake = self.committee.stake(&certificate.origin());
                                if certificate.header.view.abs() >= self.view.abs() {
                                    votes += stake;
                                    latest_view = certificate.header.view.abs();
                                }
                            }

                            if votes >= self.committee.quorum_threshold() {
                                let next_view = latest_view + 1;
                                debug!("(v={}, r={}) We have 2f+1 complaints, advancing the view to {}", self.view.abs(), round, next_view);
                                self.view = next_view;
                            } else {
                                self.view = view;
                            }
                        }
                    }

                    // Advance to the next round.
                    self.round += 1;

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
        let header = Header::new(
            self.authority.clone(),
            self.round,
            self.view,
            0 as Epoch,
            self.digests.drain(..).collect(),
            self.last_parents.drain(..).map(|x| x.hash()).collect(),
        );

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
        }

        debug!("(v={}, r={}) Broadcast header with v={}", self.view.abs(), self.round, header.view);

        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    fn elect_leader(&mut self) -> (Pubkey, String) {
        self.committee.leader(self.view as usize)
    }
}

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;
