use crate::primary::certificate_waiter::CertificateWaiter;
use crate::primary::garbage_collector::GarbageCollector;
use crate::primary::header_waiter::{HeaderWaiter, WaiterMessage};
use crate::primary::helper::Helper;
use crate::primary::payload_receiver::PayloadReceiver;
use crate::primary::primary_core::PrimaryCore;
use crate::primary::proposer::Proposer;
use crate::primary::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use futures::SinkExt;
use log::info;
use mundis_ledger::Store;
use mundis_model::certificate::{Certificate, DagError, Header};
use mundis_model::config::ValidatorConfig;
use mundis_model::hash::Hash;
use mundis_model::pubkey::Pubkey;
use mundis_model::signature::Signer;
use mundis_model::vote::Vote;
use mundis_model::{Round, WorkerId};
use mundis_network::receiver::{MessageHandler, NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, Sender};

mod aggregators;
mod certificate_waiter;
mod garbage_collector;
mod header_waiter;
mod helper;
mod payload_receiver;
mod primary_core;
mod proposer;
mod synchronizer;

/// The default channel capacity for each channel of the primary.
pub const CHANNEL_CAPACITY: usize = 1_000;

#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryMessage {
    Header(Header),
    Vote(Vote),
    Certificate(Certificate),
    CertificatesRequest(Vec<Hash>, /* requestor */ Pubkey),
}

/// The messages sent by the primary to its workers.
#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryWorkerMessage {
    /// The primary indicates that the worker need to sync the target missing batches.
    Synchronize(Vec<Hash>, /* target */ Pubkey),
    /// The primary indicates a round update.
    Cleanup(Round),
}

/// The messages sent by the workers to their primary.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerPrimaryMessage {
    /// The worker indicates it sealed a new batch.
    OurBatch(Hash, WorkerId),
    /// The worker indicates it received a batch's digest from another authority.
    OthersBatch(Hash, WorkerId),
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_primary_messages: Sender<PrimaryMessage>,
    tx_cert_requests: Sender<(Vec<Hash>, Pubkey)>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            PrimaryMessage::CertificatesRequest(missing, requestor) => self
                .tx_cert_requests
                .send((missing, requestor))
                .await
                .expect("Failed to send primary message"),
            // PrimaryMessage::Header
            // PrimaryMessage::Vote
            // PrimaryMessage::Certificate
            primary_message => self
                .tx_primary_messages
                .send(primary_message)
                .await
                .expect("Failed to send certificate"),
        }
        Ok(())
    }
}

pub struct Primary;

impl Primary {
    pub fn spawn(
        config: &ValidatorConfig,
        store: Store,
        tx_consensus: Sender<Certificate>,
        rx_consensus: Receiver<Certificate>,
    ) -> anyhow::Result<()> {
        info!("Starting primary....");

        let (tx_primary_messages, rx_primary_messages) =
            channel::<PrimaryMessage>(CHANNEL_CAPACITY);
        let (tx_cert_requests, rx_cert_requests) = channel::<(Vec<Hash>, Pubkey)>(CHANNEL_CAPACITY);
        let (tx_sync_headers, rx_sync_headers) = channel::<WaiterMessage>(CHANNEL_CAPACITY);
        let (tx_sync_certificates, rx_sync_certificates) = channel::<Certificate>(CHANNEL_CAPACITY);
        let (tx_our_digests, rx_our_digests) = channel::<(Hash, WorkerId)>(CHANNEL_CAPACITY);
        let (tx_others_digests, rx_others_digests) = channel::<(Hash, WorkerId)>(CHANNEL_CAPACITY);
        let (tx_headers_loopback, rx_headers_loopback) = channel::<Header>(CHANNEL_CAPACITY);
        let (tx_headers, rx_headers) = channel::<Header>(CHANNEL_CAPACITY);
        let (tx_certificates_loopback, rx_certificates_loopback) =
            channel::<Certificate>(CHANNEL_CAPACITY);
        let (tx_parents, rx_parents) = channel::<(Vec<Certificate>, Round)>(CHANNEL_CAPACITY);

        // Spawn the network receiver listening to messages from the other primaries.
        let address = config
            .initial_committee
            .primary(&config.identity.pubkey())
            .expect("Our public key or worker id is not in the committee")
            .primary_to_primary;

        NetworkReceiver::spawn(
            address,
            /* handler */
            PrimaryReceiverHandler {
                tx_primary_messages,
                tx_cert_requests,
            },
        );

        info!(
            "Primary {} listening to primary messages on {}",
            config.identity.pubkey(),
            address
        );

        // Spawn the network receiver listening to messages from our workers.
        let address = config
            .initial_committee
            .primary(&config.identity.pubkey())
            .expect("Our public key or worker id is not in the committee")
            .worker_to_primary;
        NetworkReceiver::spawn(
            address,
            /* handler */
            WorkerReceiverHandler {
                tx_our_digests,
                tx_others_digests,
            },
        );

        // The `Synchronizer` provides auxiliary methods helping to `Core` to sync.
        let synchronizer = Synchronizer::new(
            config.identity.pubkey(),
            &config.initial_committee,
            store.clone(),
            /* tx_header_waiter */ tx_sync_headers,
            /* tx_certificate_waiter */ tx_sync_certificates,
        );

        // Atomic variable use to synchronizer all tasks with the latest consensus round. This is only
        // used for cleanup. The only tasks that write into this variable is `GarbageCollector`.
        let consensus_round = Arc::new(AtomicU64::new(0));

        // The `Core` receives and handles headers, votes, and certificates from the other primaries.
        PrimaryCore::spawn(
            config.identity.clone(),
            config.initial_committee.clone(),
            store.clone(),
            synchronizer,
            consensus_round.clone(),
            50,
            /* rx_primaries */ rx_primary_messages,
            /* rx_header_waiter */ rx_headers_loopback,
            /* rx_certificate_waiter */ rx_certificates_loopback,
            /* rx_proposer */ rx_headers,
            tx_consensus,
            /* tx_proposer */ tx_parents,
        );

        // Keeps track of the latest consensus round and allows other tasks to clean up their their internal state
        GarbageCollector::spawn(
            &config.identity.pubkey(),
            &config.initial_committee,
            consensus_round.clone(),
            rx_consensus,
        );

        // Receives batch digests from other workers. They are only used to validate headers.
        PayloadReceiver::spawn(store.clone(), /* rx_workers */ rx_others_digests);

        // Whenever the `Synchronizer` does not manage to validate a header due to missing parent certificates of
        // batch digests, it commands the `HeaderWaiter` to synchronizer with other nodes, wait for their reply, and
        // re-schedule execution of the header once we have all missing data.
        HeaderWaiter::spawn(
            config.identity.pubkey(),
            config.initial_committee.clone(),
            store.clone(),
            consensus_round,
            50,
            5_000,
            3,
            /* rx_synchronizer */ rx_sync_headers,
            /* tx_core */ tx_headers_loopback,
        );

        // The `CertificateWaiter` waits to receive all the ancestors of a certificate before looping it back to the
        // `Core` for further processing.
        CertificateWaiter::spawn(
            store.clone(),
            /* rx_synchronizer */ rx_sync_certificates,
            /* tx_core */ tx_certificates_loopback,
        );

        // When the `Core` collects enough parent certificates, the `Proposer` generates a new header with new batch
        // digests from our workers and it back to the `Core`.
        Proposer::spawn(
            config.identity.clone(),
            config.initial_committee.clone(),
            1_000,
            100,
            /* rx_core */ rx_parents,
            /* rx_workers */ rx_our_digests,
            /* tx_core */ tx_headers,
        );

        // The `Helper` is dedicated to reply to certificates requests from other primaries.
        Helper::spawn(config.initial_committee.clone(), store, rx_cert_requests);

        info!("Primary successfully started");

        Ok(())
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_our_digests: Sender<(Hash, WorkerId)>,
    tx_others_digests: Sender<(Hash, WorkerId)>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // Deserialize and parse the message.
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            WorkerPrimaryMessage::OurBatch(digest, worker_id) => self
                .tx_our_digests
                .send((digest, worker_id))
                .await
                .expect("Failed to send workers' digests"),
            WorkerPrimaryMessage::OthersBatch(digest, worker_id) => self
                .tx_others_digests
                .send((digest, worker_id))
                .await
                .expect("Failed to send workers' digests"),
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/common.rs"]
mod common;
