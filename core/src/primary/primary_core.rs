use std::pin::Pin;
use futures::future::err;
use futures::TryFuture;
use tokio::time::Instant;
use {
    mundis_model::View,
    std::time::Duration,
    tokio::time::{sleep, Sleep},
};
// Copyright(C) Facebook, Inc. and its affiliates.
use {
    crate::primary::{
        aggregators::{CertificatesAggregator, VotesAggregator},
        primary_synchronizer::PrimarySynchronizer,
        PrimaryMessage,
    },
    async_recursion::async_recursion,
    bytes::Bytes,
    log::{debug, error, warn},
    mundis_ledger::Store,
    mundis_model::{
        certificate::{Certificate, DagError, DagResult, Header},
        committee::Committee,
        hash::{Hash, Hashable},
        keypair::Keypair,
        pubkey::Pubkey,
        signature::Signer,
        vote::Vote,
        Round,
    },
    mundis_network::reliable_sender::{CancelHandler, ReliableSender},
    std::{
        collections::{HashMap, HashSet},
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
    },
    tokio::sync::mpsc::{Receiver, Sender},
};
use crate::primary::aggregators::{ConsensusComplaintsAggregator, ConsensusVotesAggregator};

const CONSENSUS_TIMER_MS: u64 = 1000;

type Dag = HashMap<Round, HashMap<Pubkey, (Hash, Certificate)>>;

#[derive()]
pub struct PrimaryCore {
    /// The keypair of this primary.
    authority: Keypair,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Handles synchronization with other nodes and our workers.
    synchronizer: PrimarySynchronizer,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Round,

    /// Receiver for dag messages (headers, votes, certificates).
    rx_primaries: Receiver<PrimaryMessage>,
    /// Receives loopback headers from the `HeaderWaiter`.
    rx_header_waiter: Receiver<Header>,
    /// Receives loopback certificates from the `CertificateWaiter`.
    rx_certificate_waiter: Receiver<Certificate>,
    /// Receives our newly created headers from the `Proposer`.
    rx_proposer: Receiver<Header>,
    /// Output all certificates to the consensus layer.
    tx_consensus: Sender<Certificate>,
    /// Send valid a quorum of certificates' ids to the `Proposer` (along with their round).
    tx_proposer: Sender<(Vec<Certificate>, Round)>,

    /// The last garbage collected round.
    gc_round: Round,
    /// The authors of the last voted headers.
    last_voted: HashMap<Round, HashSet<Pubkey>>,
    /// The set of headers we are currently processing.
    processing: HashMap<Round, HashSet<Hash>>,
    /// The last header we proposed (for which we are waiting votes).
    current_header: Header,
    /// Aggregates votes into a certificate.
    votes_aggregator: VotesAggregator,
    /// Aggregates certificates to use as parents for new headers.
    certificates_aggregators: HashMap<Round, Box<CertificatesAggregator>>,
    /// A network sender to send the batches to the other workers.
    network: ReliableSender,
    /// Keeps the cancel handlers of the messages we sent.
    cancel_handlers: HashMap<Round, Vec<CancelHandler>>,

    round: Round,
    view: View,
    meta: View,
    dag: Dag,
    last_broadcasted_meta: View,
    header_aggregator: Box<ConsensusVotesAggregator>,
    complaint_aggregator: Box<ConsensusComplaintsAggregator>,
    last_committed_round: Round
}

impl PrimaryCore {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        authority: Keypair,
        committee: Committee,
        store: Store,
        synchronizer: PrimarySynchronizer,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        rx_primaries: Receiver<PrimaryMessage>,
        rx_header_waiter: Receiver<Header>,
        rx_certificate_waiter: Receiver<Certificate>,
        rx_proposer: Receiver<Header>,
        tx_consensus: Sender<Certificate>,
        tx_proposer: Sender<(Vec<Certificate>, Round)>,
    ) {
        let genesis = Certificate::genesis(&committee);
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.hash(), x)))
            .collect::<HashMap<_, _>>();

        tokio::spawn(async move {
            Self {
                authority,
                committee,
                store,
                synchronizer,
                consensus_round,
                gc_depth,
                rx_primaries,
                rx_header_waiter,
                rx_certificate_waiter,
                rx_proposer,
                tx_consensus,
                tx_proposer,
                gc_round: 0,
                last_voted: HashMap::with_capacity(2 * gc_depth as usize),
                processing: HashMap::with_capacity(2 * gc_depth as usize),
                current_header: Header::default(),
                votes_aggregator: VotesAggregator::new(),
                certificates_aggregators: HashMap::with_capacity(2 * gc_depth as usize),
                network: ReliableSender::new(),
                cancel_handlers: HashMap::with_capacity(2 * gc_depth as usize),

                round: 1,
                view: 1,
                meta: 0,
                dag: [(0, genesis)].iter().cloned().collect(),
                last_broadcasted_meta: 0,
                header_aggregator: Box::new(ConsensusVotesAggregator::new()),
                complaint_aggregator: Box::new(ConsensusComplaintsAggregator::new()),
                last_committed_round: 0
            }
            .run()
            .await;
        });
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        // and start the timer
        let timer = sleep(Duration::from_millis(CONSENSUS_TIMER_MS));
        tokio::pin!(timer);

        loop {
            let result = tokio::select! {
                 // We receive here messages from other primaries.
                Some(message) = self.rx_primaries.recv() => {
                    match message {
                        PrimaryMessage::Header(header) => {
                            match self.sanitize_header(&header) {
                                Ok(()) => self.process_header(&header, &timer).await,
                                error => error
                            }

                        },
                        PrimaryMessage::Vote(vote) => {
                            match self.sanitize_vote(&vote) {
                                Ok(()) => self.process_vote(vote, &timer).await,
                                error => error
                            }
                        },
                        PrimaryMessage::Certificate(certificate) => {
                            match self.sanitize_certificate(&certificate) {
                                Ok(()) =>  self.process_certificate(certificate, &timer).await,
                                error => error
                            }
                        },
                        _ => panic!("Unexpected core message")
                    }
                },

                // We receive here loopback headers from the `HeaderWaiter`. Those are headers for which we interrupted
                // execution (we were missing some of their dependencies) and we are now ready to resume processing.
                Some(header) = self.rx_header_waiter.recv() => self.process_header(&header, &timer).await,

                // We receive here loopback certificates from the `CertificateWaiter`. Those are certificates for which
                // we interrupted execution (we were missing some of their ancestors) and we are now ready to resume
                // processing.
                Some(certificate) = self.rx_certificate_waiter.recv() => self.process_certificate(certificate, &timer).await,

                // We also receive here our new headers created by the `Proposer`.
                Some(header) = self.rx_proposer.recv() => self.process_own_header(header, &timer).await,

                () = &mut timer => {
                    // do nothing
                    Ok(())
                }
            };

            match result {
                Ok(()) => (),
                Err(DagError::StoreError(e)) => {
                    error!("{}", e);
                    panic!("Storage failure: killing node.");
                }
                Err(e @ DagError::TooOld(..)) => debug!("{}", e),
                Err(e) => warn!("{}", e),
            }

            // Cleanup internal state.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let gc_round = round - self.gc_depth;
                self.last_voted.retain(|k, _| k >= &gc_round);
                self.processing.retain(|k, _| k >= &gc_round);
                self.certificates_aggregators.retain(|k, _| k >= &gc_round);
                self.cancel_handlers.retain(|k, _| k >= &gc_round);
                self.gc_round = gc_round;
            }
        }
    }

    #[async_recursion]
    async fn process_certificate(&mut self, certificate: Certificate, timer: &Pin<&mut Sleep>) -> DagResult<()> {
        // error!(
        //     "(v={}, r={}) Processing certificate {} with header {} for view {}",
        //     self.view,
        //     self.round,
        //     certificate.hash(),
        //     certificate.header.id,
        //     certificate.view()
        // );

        // Process the header embedded in the certificate if we haven't already voted for it (if we already
        // voted, it means we already processed it). Since this header got certified, we are sure that all
        // the data it refers to (ie. its payload and its parents) are available. We can thus continue the
        // processing of the certificate even if we don't have them in store right now.
        if !self
            .processing
            .get(&certificate.header.round)
            .map_or_else(|| false, |x| x.contains(&certificate.header.id))
        {
            // This function may still throw an error if the storage fails.
            self.process_header(&certificate.header, timer).await?;
        }

        // Ensure we have all the ancestors of this certificate yet. If we don't, the synchronizer will gather
        // them and trigger re-processing of this certificate.
        if !self.synchronizer.deliver_certificate(&certificate).await? {
            debug!(
                "Processing of {:?} suspended: missing ancestors",
                certificate
            );
            return Ok(());
        }

        // Store the certificate.
        let bytes = bincode::serialize(&certificate).expect("Failed to serialize certificate");
        self.store.write(certificate.hash().to_vec(), bytes).await;

        let leader = self.elect_leader(self.view);
        if certificate.meta > self.meta && certificate.origin() == leader {
            error!("(v={}, r={}) I received the leader {} certificate with meta={}", self.view, self.round, leader, certificate.meta);
            self.meta = certificate.meta;
        }

        // Check if we have enough certificates to enter a new dag round and propose a header.
        if let Some(parents) = self
            .certificates_aggregators
            .entry(certificate.round())
            .or_insert_with(|| Box::new(CertificatesAggregator::new()))
            .append(certificate.clone(), &self.committee)?
        {
            let round = certificate.round();

            if round >= self.round {
                self.round = round + 1;
            }

            // Send it to the `Proposer` to create a new header for the next round
            self.tx_proposer
                .send((parents.clone(), round))
                .await
                .expect("Failed to send certificate");
        }

        // Add the new certificate to the local storage.
        self.dag
            .entry(certificate.round())
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.hash(), certificate));

        if self.last_committed_round > 0 && self.round > self.last_committed_round + 2 {
            // error!("(v={}, r={}) Timer expired", self.view, self.round);
            self.meta = -self.view;
        }

        Ok(())
    }

    #[async_recursion]
    async fn process_vote(&mut self, vote: Vote, timer: &Pin<&mut Sleep>) -> DagResult<()> {
        // debug!("Processing vote {:?}", vote);

        // Add it to the votes' aggregator and try to make a new certificate.
        if let Some(certificate) =
            self.votes_aggregator
                .append(vote, &self.committee, &self.current_header)?
        {
            let mut cert = certificate;
            let leader = self.elect_leader(self.view);
            if self.last_broadcasted_meta != self.view && self.authority.pubkey() == leader {
                cert.meta = self.view;
                self.last_broadcasted_meta = self.view;
                error!("(v={}, r={}) Broadcast LEADER certificate with meta={}", self.view, self.round, cert.meta);
            } else {
                // cert.meta = self.meta;
                // error!("(v={}, r={}) Broadcast certificate with meta={}", self.view, self.round, cert.meta);
            }

            // Broadcast the certificate.
            let addresses = self
                .committee
                .others_primary_addresses(&self.authority.pubkey())
                .iter()
                .map(|(_, x)| x.primary_to_primary)
                .collect();
            let bytes = bincode::serialize(&PrimaryMessage::Certificate(cert.clone()))
                .expect("Failed to serialize our own certificate");
            // debug!("RELIABLE BROADCAST PrimaryMessage::Certificate");

            // error!(
            //     "(r={}) Broadcast certificate {} for view {}",
            //     cert.round(),
            //     cert.hash(),
            //     cert.view()
            // );

            let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
            self.cancel_handlers
                .entry(cert.round())
                .or_insert_with(Vec::new)
                .extend(handlers);

            // Process the new certificate.
            self.process_certificate(cert, timer)
                .await
                .expect("Failed to process valid certificate");
        }
        Ok(())
    }

    #[async_recursion]
    async fn process_header(&mut self, header: &Header, timer: &Pin<&mut Sleep>) -> DagResult<()> {
        // Indicate that we are processing this header.
        self.processing
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.id.clone());

        // Ensure we have the parents. If at least one parent is missing, the synchronizer returns an empty
        // vector; it will gather the missing parents (as well as all ancestors) from other nodes and then
        // reschedule processing of this header.
        let parents = self.synchronizer.get_parents(header).await?;
        if parents.is_empty() {
            debug!("Processing of {} suspended: missing parent(s)", header.id);
            return Ok(());
        }

        // Check the parent certificates. Ensure the parents form a quorum and are all from the previous round.
        let mut stake = 0;
        for x in &parents {
            if x.round() + 1 != header.round {
                return Err(DagError::MalformedHeader(header.id.clone()));
            }
            stake += self.committee.stake(&x.origin());
        }

        if stake < self.committee.quorum_threshold() {
            return Err(DagError::HeaderRequiresQuorum(header.id.clone()));
        }

        // Ensure we have the payload. If we don't, the synchronizer will ask our workers to get it, and then
        // reschedule processing of this header once we have it.
        if self.synchronizer.missing_payload(header).await? {
            debug!("Processing of {} suspended: missing payload", header);
            return Ok(());
        }

        // Store the header.
        let bytes = bincode::serialize(header).expect("Failed to serialize header");
        self.store.write(header.id.to_vec(), bytes).await;

        // Check if we can vote for this header.
        if self
            .last_voted
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.author)
        {
            // error!("Voting header {}", header.id);

            // Make a vote and send it to the header's creator.
            let vote = Vote::new(header, &self.authority).await;
            // info!("Created vote: {:?}", vote);
            if vote.origin == self.authority.pubkey() {
                self.process_vote(vote, timer)
                    .await
                    .expect("Failed to process our own vote");
            } else {
                let address = self
                    .committee
                    .primary_address(&header.author)
                    .expect("Author of valid header is not in the committee")
                    .primary_to_primary;
                // debug!("RELIABLE SEND PrimaryMessage::Vote for header={}, round={}", vote.id, vote.round);
                let bytes = bincode::serialize(&PrimaryMessage::Vote(vote))
                    .expect("Failed to serialize our own vote");
                let handler = self.network.send(address, Bytes::from(bytes)).await;
                self.cancel_handlers
                    .entry(header.round)
                    .or_insert_with(Vec::new)
                    .push(handler);
            }

            if header.view == self.view {
                if self.header_aggregator.append(&self.committee, &header)?.is_some() {
                    error!("(r={}) COMMIT view {}", self.round, self.view);
                    self.view = self.view + 1;
                    self.header_aggregator = Box::new(ConsensusVotesAggregator::new());
                    self.complaint_aggregator = Box::new(ConsensusComplaintsAggregator::new());
                    self.last_committed_round = self.round;
                }
            }

            if header.view == -self.view {
                if self.complaint_aggregator.append(&self.committee, &header)?.is_some() {
                    error!("(r={}) COMMIT COMPLAINT view {}", self.round, self.view);
                    self.view = self.view + 1;
                    self.header_aggregator = Box::new(ConsensusVotesAggregator::new());
                    self.complaint_aggregator = Box::new(ConsensusComplaintsAggregator::new());
                    self.last_committed_round = self.round;
                }
            }
        }

        if self.last_committed_round > 0 && self.round > self.last_committed_round + 2 {
            error!("(v={}, r={}) Timer expired", self.view, self.round);
            self.meta = -self.view;
        }

        Ok(())
    }

    async fn process_own_header(&mut self, mut header: Header, timer: &Pin<&mut Sleep>) -> DagResult<()> {
        // mundis
        header.view = self.meta;

        // Reset the votes aggregator.
        self.current_header = header.clone();
        self.votes_aggregator = VotesAggregator::new();

        error!(
            "(v={}, r={}) Broadcast header with meta={}",
            self.view, self.round, header.view
        );

        // Broadcast the new header in a reliable manner.
        let addresses = self
            .committee
            .others_primary_addresses(&self.authority.pubkey())
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone()))
            .expect("Failed to serialize our own header");
        //debug!("RELIABLE BROADCAST PrimaryMessage::Header with id={}, round={}", header.id, header.round);
        let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
        self.cancel_handlers
            .entry(header.round)
            .or_insert_with(Vec::new)
            .extend(handlers);

        // Process the header.
        self.process_header(&header, timer).await
    }

    fn sanitize_certificate(&mut self, certificate: &Certificate) -> DagResult<()> {
        if self.gc_round > certificate.round() {
            return Err(DagError::TooOld(certificate.hash(), certificate.round()));
        }

        // Verify the certificate (and the embedded header).
        certificate.verify(&self.committee).map_err(DagError::from)
    }

    fn sanitize_vote(&mut self, vote: &Vote) -> DagResult<()> {
        if self.current_header.round > vote.round {
            return Err(DagError::TooOld(vote.hash(), vote.round));
        }

        // Ensure we receive a vote on the expected header.
        if vote.id != self.current_header.id
            || vote.origin != self.current_header.author
            || vote.round != self.current_header.round
        {
            return Err(DagError::UnexpectedVote(vote.id.clone()));
        }

        // Verify the vote.
        vote.verify(&self.committee).map_err(DagError::from)
    }

    fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
        if self.gc_round > header.round {
            return Err(DagError::TooOld(header.id.clone(), header.round));
        }

        // Verify the header's signature.
        header.verify(&self.committee)?;

        // TODO [issue #3]: Prevent bad nodes from sending junk headers with high round numbers.

        Ok(())
    }

    fn elect_leader(&mut self, view: View) -> Pubkey {
        let (leader_key, _) = self.committee.leader(view as usize);
        leader_key
    }
}

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;
