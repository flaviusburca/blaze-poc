use log::error;
use {
    log::{debug, info, log_enabled, warn},
    mundis_model::{
        base_types::Epoch,
        certificate::{Certificate, Header},
        committee::Committee,
        hash::{Hash, Hashable},
        keypair::Keypair,
        pubkey::Pubkey,
        signature::Signer,
        view::View,
        Round, WorkerId,
    },
    std::{cmp::Ordering, time::Duration},
    tokio::{
        sync::mpsc::{Receiver, Sender},
        time::{sleep, Instant, Sleep},
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
    rx_core: Receiver<(Vec<Certificate>, Round, i64)>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(Hash, WorkerId)>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,

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
                view: View::new(1),
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
        debug!("Dag starting at view {} and round {}", self.view.current, self.view.round);

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
                    // Compare the parents' round number with our current round.
                    if round < self.view.round || view < self.view.current {
                        continue;
                    }

                     // Signal that we have enough parent certificates to propose a new header.
                    self.last_parents = parents.to_owned();

                    error!("View={}, Round={}, enough_parents={}, enough_digests={}, timer_expired={}",
                        self.view.current.abs(), round, enough_parents, enough_digests, timer_expired
                    );

                    // doar daca avem istoric
                    if !parents.is_empty() {
                        if self.view.current >= 0 {
                            // daca view-ul curent nu este complain, alegem leader
                            (self.view.leader, self.view.leader_name) = self.elect_leader();
                            error!("(v={}, r={}) elected leader={}", self.view.current, round, self.view.leader_name);

                            // cautam certificatul leader-ului
                            let leader_certificate = parents
                                .iter()
                                .find(|x| (x.header.view >= self.view.current) && (x.origin() == self.view.leader))
                                .clone();

                            if leader_certificate.is_some() {
                                let leader_view = leader_certificate.unwrap().header.view + 1;
                                // intram in urmatorul view pe baza view-ului propus de leader
                                error!("(v={}, r={}) Comitem view-ul {} si avansam la view-ul liderului {}", self.view.current, round, self.view.current, leader_view);
                                 // TODO: comitem certificatul leader-ului is istoricul lui

                                self.view.current = leader_view;
                            } else {
                                error!("(v={}, r={}) Marcam complaint", self.view.current, round);
                                self.view.current = -self.view.current;
                            }
                        } else {
                             // vedem daca avem 2f+1 complaints
                            let mut votes = 0;
                            let mut latest_view = self.view.current;
                            for certificate in &parents {
                                let stake = self.committee.stake(&certificate.origin());
                                // daca intram tarziu in joc, certificatul va fi de la un view mai mare
                                // si trebuie sa-l preluam si noi
                                if certificate.header.view.abs() >= self.view.current.abs() {
                                    votes += stake;
                                    latest_view = certificate.header.view.abs();
                                }
                            }

                            if votes >= self.committee.quorum_threshold() {
                                error!("(v={}, r={}) Avem 2f+1 complaints, avansam view-ul la {}", self.view.current.abs(), round, latest_view + 1);
                                self.view.current = latest_view + 1;
                            } else {
                                // TODO: de cercetat asta, ar trebui sa se intample ?

                                // aici putem ajunge daca nodul intra mai tarziu, caz in care nu va putea face quorum nici pe voturi nici pe complaints
                                // setam view-ul primit
                                // error!("(v={}, r={}) Nu avem nici lider nici complaints, setam view-ul {}", self.view.current.abs(), self.view.round, view);
                                // self.view.current = view;
                                for certificate in &parents {
                                    error!("(v={}, r={}) CHV={}", self.view.current, round, certificate.header.view);
                                }

                                panic!("(v={}, r={}) Not enough votes: votes({}) <= quorum_threshold({})",
                                    self.view.current.abs(), round,
                                    votes, self.committee.quorum_threshold()
                                );
                            }
                        }
                    } else {
                        debug!("Nu avem istoric");
                    }

                    // Advance to the next round.
                    self.view.round += 1;
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
            self.view.round,
            self.view.current,
            0 as Epoch,
            self.digests.drain(..).collect(),
            self.last_parents.drain(..).map(|x| x.hash()).collect(),
        );

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
        }

        error!("(v={}, r={}) Broadcast header cu v={}", self.view.current.abs(), self.view.round, header.view);

        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    fn elect_leader(&mut self) -> (Pubkey, String) {
        self.committee.leader(self.view.current as usize)
    }
}

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;
