use std::pin::Pin;
use futures::future::err;
use futures::TryFuture;
use tokio::time::Instant;
use {
    mundis_model::View,
    std::time::Duration,
    tokio::time::{sleep, Sleep},
};
use {
    crate::master::{
        aggregators::{CertificatesAggregator, VotesAggregator},
        master_synchronizer::MasterSynchronizer,
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
use mundis_model::Stake;
use crate::master::aggregators::{BlazeComplaintsAggregator, BlazeVotesAggregator};
use crate::master::state::State;

const TIMEOUT_NUM_ROUNDS: u64 = 2;

#[derive()]
pub struct MasterCore {
    /// The keypair of this primary.
    authority: Keypair,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Handles synchronization with other nodes and our workers.
    synchronizer: MasterSynchronizer,
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

    /// The genesis certificates.
    genesis: Vec<Certificate>,
    // Send the sequence of certificates in the right order to the Garbage Collector.
    tx_gc: Sender<Certificate>,

    round: Round,
    view: View,
    meta: View,
    state: State,
    last_broadcasted_meta: View,
    blaze_votes: Box<BlazeVotesAggregator>,
    blaze_complaints: Box<BlazeComplaintsAggregator>,
    last_view_round: Round,
}

impl MasterCore {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        authority: Keypair,
        committee: Committee,
        store: Store,
        synchronizer: MasterSynchronizer,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        rx_primaries: Receiver<PrimaryMessage>,
        rx_header_waiter: Receiver<Header>,
        rx_certificate_waiter: Receiver<Certificate>,
        rx_proposer: Receiver<Header>,
        tx_proposer: Sender<(Vec<Certificate>, Round)>,
        tx_gc: Sender<Certificate>
    ) {
        let genesis = Certificate::genesis(&committee);

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
                tx_proposer,
                gc_round: 0,
                last_voted: HashMap::with_capacity(2 * gc_depth as usize),
                processing: HashMap::with_capacity(2 * gc_depth as usize),
                current_header: Header::default(),
                votes_aggregator: VotesAggregator::new(),
                certificates_aggregators: HashMap::with_capacity(2 * gc_depth as usize),
                network: ReliableSender::new(),
                cancel_handlers: HashMap::with_capacity(2 * gc_depth as usize),
                genesis: genesis.clone(),
                tx_gc,
                round: 1,
                view: 1,
                meta: 1,
                last_broadcasted_meta: 0,
                blaze_votes: Box::new(BlazeVotesAggregator::new()),
                blaze_complaints: Box::new(BlazeComplaintsAggregator::new()),
                last_view_round: 0,
                state: State::new(genesis, gc_depth),
            }
            .run()
            .await;
        });
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        // self.state.dump(Some("\nGENESIS ".to_string()));

        loop {
            let result = tokio::select! {
                 // We receive here messages from other primaries.
                Some(message) = self.rx_primaries.recv() => {
                    match message {
                        PrimaryMessage::Header(header) => {
                            match self.sanitize_header(&header) {
                                Ok(()) => self.process_header(&header).await,
                                error => error
                            }

                        },
                        PrimaryMessage::Vote(vote) => {
                            match self.sanitize_vote(&vote) {
                                Ok(()) => self.process_vote(vote).await,
                                error => error
                            }
                        },
                        PrimaryMessage::Certificate(certificate) => {
                            match self.sanitize_certificate(&certificate) {
                                Ok(()) =>  self.process_certificate(certificate).await,
                                error => error
                            }
                        },
                        _ => panic!("Unexpected core message")
                    }
                },

                // We receive here loopback headers from the `HeaderWaiter`. Those are headers for which we interrupted
                // execution (we were missing some of their dependencies) and we are now ready to resume processing.
                Some(header) = self.rx_header_waiter.recv() => self.process_header(&header).await,

                // We receive here loopback certificates from the `CertificateWaiter`. Those are certificates for which
                // we interrupted execution (we were missing some of their ancestors) and we are now ready to resume
                // processing.
                Some(certificate) = self.rx_certificate_waiter.recv() => self.process_certificate(certificate).await,

                // We also receive here our new headers created by the `Proposer`.
                Some(header) = self.rx_proposer.recv() => self.process_own_header(header).await,
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
    async fn process_certificate(&mut self, certificate: Certificate) -> DagResult<()> {
        debug!(
            "(v={}, r={}) Processing certificate {}",
            self.view,
            self.round,
            certificate.hash(),
        );

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
            self.process_header(&certificate.header).await?;
        }

        // Ensure we have all the ancestors of this certificate yet. If we don't, the synchronizer will gather
        // them and trigger re-processing of this certificate.
        if !self.synchronizer.fetch_ancestors(&certificate).await? {
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
        if certificate.header.meta > self.meta && certificate.origin() == leader {
            debug!("(v={}, r={}) received certificate of leader {} with meta={}", self.view, self.round, leader, certificate.header.meta);
            self.meta = certificate.header.meta;
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
                self.state.rounds.insert(self.round, self.view);
            }

            // Send it to the `Proposer` to create a new header for the next round
            self.tx_proposer
                .send((parents.clone(), round))
                .await
                .expect("Failed to send certificate");
        }

        // Add the new certificate to the local storage.
        let leader_key = self.elect_leader(self.view);
        if certificate.origin() == leader_key {
            error!("(r={}, v={}) Adding leader certificate", self.round, self.view);
        }

        self.state.add_certificate(certificate);

        // Timeout is in TIMEOUT_NUM_ROUNDS consecutive rounds
        if self.last_view_round > 0 && self.round > self.last_view_round + TIMEOUT_NUM_ROUNDS {
            error!("(r={}, v={}) View timer expired", self.round, self.view);
            self.meta = -self.view;
        }

        Ok(())
    }

    #[async_recursion]
    async fn process_vote(&mut self, vote: Vote) -> DagResult<()> {
        debug!("Processing vote {:?}", vote);

        // Add it to the votes' aggregator and try to make a new certificate.
        if let Some(certificate) =
            self.votes_aggregator
                .append(vote, &self.committee, &self.current_header)?
        {
            let mut cert = certificate;
            let leader = self.elect_leader(self.view);
            if self.last_broadcasted_meta != self.view && self.authority.pubkey() == leader {
                cert.header.meta = self.view;
                self.last_broadcasted_meta = self.view;
                debug!("(v={}, r={}) Broadcast LEADER certificate with meta={}", self.view, self.round, cert.header.meta);
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

            debug!(
                "(r={}) Broadcast certificate {} for view {}",
                cert.round(),
                cert.hash(),
                cert.view()
            );

            let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
            self.cancel_handlers
                .entry(cert.round())
                .or_insert_with(Vec::new)
                .extend(handlers);

            // Process the new certificate.
            self.process_certificate(cert)
                .await
                .expect("Failed to process valid certificate");
        }
        Ok(())
    }

    #[async_recursion]
    async fn process_header(&mut self, header: &Header) -> DagResult<()> {
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
        if self.synchronizer.payload_complete(header).await? {
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
            debug!("Voting header {}", header.id);

            // Make a vote and send it to the header's creator.
            let vote = Vote::new(header, &self.authority).await;
            // info!("Created vote: {:?}", vote);
            if vote.origin == self.authority.pubkey() {
                self.process_vote(vote)
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

            if header.meta == self.view {
                if self.blaze_votes.append(&self.committee, &header)?.is_some() {
                    self.commit_view().await;
                    self.advance_view();
                }
            } else if header.meta == -self.view {
                if self.blaze_complaints.append(&self.committee, &header)?.is_some() {
                    self.advance_view();
                }
            }
        }

        if self.round > self.last_view_round + TIMEOUT_NUM_ROUNDS {
            self.meta = -self.view;
        }

        Ok(())
    }

    async fn process_own_header(&mut self, mut header: Header) -> DagResult<()> {
        header.meta = self.meta;

        // Reset the votes aggregator.
        self.current_header = header.clone();
        self.votes_aggregator = VotesAggregator::new();

        debug!(
            "(v={}, r={}) Broadcast header with meta={}",
            self.view, self.round, header.meta
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
        self.process_header(&header).await
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

    fn elect_leader(&self, view: View) -> Pubkey {
        let (leader_key, _) = self.committee.leader(view as usize);
        leader_key
    }

    fn advance_view(&mut self) {
        error!("(r={}, v={}) Advancing view", self.round, self.view);
        self.view = self.view + 1;
        self.blaze_votes = Box::new(BlazeVotesAggregator::new());
        self.blaze_complaints = Box::new(BlazeComplaintsAggregator::new());
        self.last_view_round = self.round;
    }

    async fn commit_view(&mut self) {
        if self.view < 2 {
            return;
        }

        // If we already ordered this leader, do nothing
        if self.view <= self.state.last_committed_view as View {
            return;
        }

        let leader_key = self.elect_leader(self.view);
        error!("(v={}) Leader is {}", self.view, leader_key);

        let (leader_cert_hash, leader_cert) = self.state.dag
            .get(&self.view)
            .map(|x| x.get(&leader_key))
            .flatten()
            .expect("No leader certificate found for previous view");

        let mut sequence: Vec<Certificate> = Vec::new();
        for leader in self.order_leaders(leader_cert).iter().rev() {
            // Starting from the oldest leader, flatten the sub-dag referenced by the leader.
            for x in self.order_dag(leader) {
                // Update and clean up internal state.
                self.state.update(&x, self.gc_depth);
                // Add the certificate to the sequence.
                sequence.push(x);
            }
        }

        // Output the sequence in the right order.
        for certificate in sequence {
            self.tx_gc
                .send(certificate.clone())
                .await
                .expect("Failed to send certificate to the Garbage Collector");
        }
    }

    fn order_leaders(&self, leader: &Certificate) -> Vec<Certificate> {
        let mut to_commit = vec![leader.clone()];
        let mut leader = leader;
        for v in (self.state.last_committed_view + 1 ..= self.view - 1)
            .rev()
        {
            // Get the certificate proposed by the previous leader.
            let prev_leader_key = self.elect_leader(v);

            let (_, prev_leader) = match self.state.dag.get(&v)
                .map(|x| x.get(&prev_leader_key))
                .flatten() {
                Some(x) => x,
                None => continue,
            };

            // Check whether there is a path between the last two leaders.
            if self.linked(leader, prev_leader) {
                to_commit.push(prev_leader.clone());
                leader = prev_leader;
            }
        }
        to_commit
    }

    fn linked(&self, leader: &Certificate, prev_leader: &Certificate) -> bool {
        let mut parents = vec![leader];

        for v in (prev_leader.view()..leader.view()).rev() {
            parents = self.state.dag
                .get(&v)
                .expect("We should have the whole history by now")
                .values()
                .filter(|(hash, _)| parents.iter().any(|x| x.header.parents.contains(hash)))
                .map(|(_, certificate)| certificate)
                .collect();
        }
        parents.contains(&prev_leader)
    }

    fn order_dag(&self, leader: &Certificate) -> Vec<Certificate> {
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();

        let mut buffer = vec![leader];
        while let Some(x) = buffer.pop() {
            ordered.push(x.clone());
            for parent in &x.header.parents {
                let (hash, certificate) = match self.state
                    .dag
                    .get(&(x.view() - 1))
                    .map(|x| x.values().find(|(x, _)| x == parent))
                    .flatten()
                {
                    Some(x) => x,
                    None => continue, // We already ordered or GC up to here.
                };

                // We skip the certificate if we (1) already processed it or (2) we reached a round that we already
                // committed for this authority.
                let mut skip = already_ordered.contains(&hash);
                skip |= self.state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or_else(|| false, |r| r == &certificate.view());
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(hash);
                }
            }
        }

        // Ensure we do not commit garbage collected certificates.
        ordered.retain(|x| x.round() + self.gc_depth >= self.state.last_committed_round());

        // Ordering the output by round is not really necessary but it makes the commit sequence prettier.
        ordered.sort_by_key(|x| x.view());
        ordered
    }
}
