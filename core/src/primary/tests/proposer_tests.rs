use super::*;
use crate::primary::common::{committee, keys};
use tokio::sync::mpsc::channel;
use mundis_model::signature::Signer;

#[tokio::test]
async fn propose_empty() {
    let keypair = keys().pop().unwrap();

    let (_tx_parents, rx_parents) = channel(1);
    let (_tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);

    // Spawn the proposer.
    Proposer::spawn(
        keypair.clone(),
        committee(),
        /* header_size */ 1_000,
        /* max_header_delay */ 20,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        /* tx_core */ tx_headers,
    );

    // Ensure the proposer makes a correct empty header.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round, 1);
    assert!(header.payload.is_empty());
    assert!(header.verify(&committee()).is_ok());
}

#[tokio::test]
async fn propose_payload() {
    let keypair = keys().pop().unwrap();

    let (_tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);

    // Spawn the proposer.
    Proposer::spawn(
        keypair.clone(),
        committee(),
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        /* tx_core */ tx_headers,
    );

    // Send enough digests for the header payload.
    let digest = Hash::new(keypair.pubkey().as_ref());
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    // Ensure the proposer makes a correct header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round, 1);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee()).is_ok());
}
