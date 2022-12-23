use std::cmp::Ordering;
use std::time::Duration;
use log::{debug, log_enabled, warn};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{Instant, sleep};
use mundis_model::certificate::{Certificate, Header};
use mundis_model::committee::Committee;
use mundis_model::{Round, WorkerId};
use mundis_model::base_types::Epoch;
use mundis_model::hash::{Hash, Hashable};
use mundis_model::keypair::Keypair;

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
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

    /// The current round of the dag.
    round: Round,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Certificate>,
    /// Holds the certificate of the last leader (if any).
    last_leader: Option<Certificate>,
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
                round: 0,
                last_parents: genesis,
                last_leader: None,
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
            }
                .run()
                .await;
        });
    }

    /// Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.round);
        let mut advance = true;

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);

        loop {
            // Check if we can propose a new header. We propose a new header when we have a quorum of parents
            // and one of the following conditions is met:
            // (i) the timer expired (we timed out on the leader or gave up gather votes for the leader),
            // (ii) we have enough digests (minimum header size) and we are on the happy path (we can vote for
            // the leader or the leader has enough votes to enable a commit).
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = self.payload_size >= self.header_size;
            let timer_expired = timer.is_elapsed();

            if (timer_expired || (enough_digests && advance)) && enough_parents {
                if timer_expired {
                    warn!("Timer expired for round {}", self.round);
                }

                // Advance to the next round.
                self.round += 1;
                debug!("Dag moved to round {}", self.round);

                // Make a new header.
                self.make_header().await;
                self.payload_size = 0;

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                Some((parents, round)) = self.rx_core.recv() => {
                    // Compare the parents' round number with our current round.
                    match round.cmp(&self.round) {
                        Ordering::Greater => {
                            // We accept round bigger than our current round to jump ahead in case we were
                            // late (or just joined the network).
                            self.round = round;
                            self.last_parents = parents;
                        },
                        Ordering::Less => {
                            // Ignore parents from older rounds.
                        },
                        Ordering::Equal => {
                            // The core gives us the parents the first time they are enough to form a quorum.
                            // Then it keeps giving us all the extra parents.
                            self.last_parents.extend(parents)
                        }
                    }

                    // Check whether we can advance to the next round. Note that if we timeout,
                    // we ignore this check and advance anyway.
                    advance = match self.round % 2 {
                        0 => self.update_leader(),
                        _ => self.enough_votes(),
                    }
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
            0 as Epoch,
            self.digests.drain(..).collect(),
            self.last_parents.drain(..).map(|x| x.hash()).collect(),
        );
        debug!("Created {:?}", header);

        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    /// Update the last leader.
    fn update_leader(&mut self) -> bool {
        let leader_name = self.committee.leader(self.round as usize);
        self.last_leader = self
            .last_parents
            .iter()
            .find(|x| x.origin() == leader_name)
            .cloned();

        if let Some(leader) = self.last_leader.as_ref() {
            debug!("Got leader {} for round {}", leader.origin(), self.round);
        }

        self.last_leader.is_some()
    }

    /// Check whether if we have (i) 2f+1 votes for the leader, (ii) f+1 nodes not voting for the leader,
    /// or (iii) there is no leader to vote for.
    fn enough_votes(&self) -> bool {
        let leader = match &self.last_leader {
            Some(x) => x.hash(),
            None => return true,
        };

        let mut votes_for_leader = 0;
        let mut no_votes = 0;
        for certificate in &self.last_parents {
            let stake = self.committee.stake(&certificate.origin());
            if certificate.header.parents.contains(&leader) {
                votes_for_leader += stake;
            } else {
                no_votes += stake;
            }
        }

        let mut enough_votes = votes_for_leader >= self.committee.quorum_threshold();
        if log_enabled!(log::Level::Debug) && enough_votes {
            if let Some(leader) = self.last_leader.as_ref() {
                debug!(
                    "Got enough support for leader {} at round {}",
                    leader.origin(),
                    self.round
                );
            }
        }
        enough_votes |= no_votes >= self.committee.validity_threshold();
        enough_votes
    }
}

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;