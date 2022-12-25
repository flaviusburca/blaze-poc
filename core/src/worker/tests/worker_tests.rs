use super::*;
use crate::primary::WorkerPrimaryMessage;
use crate::worker::common::{batch_digest, committee_with_base_port, keys, listener, transaction};
use mundis_model::signature::Signer;
use mundis_network::simple_sender::SimpleSender;
use std::fs;

#[tokio::test]
async fn handle_clients_transactions() {
    let authority = keys().pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(11_000);

    // Create a new test store.
    let path = ".db_test_handle_clients_transactions";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `Worker` instance.
    Worker::spawn(authority.pubkey(), id, committee.clone(), store).unwrap();

    // Spawn a network listener to receive our batch's digest.
    let primary_address = committee
        .primary(&authority.pubkey())
        .unwrap()
        .worker_to_primary;
    let expected = bincode::serialize(&WorkerPrimaryMessage::OurBatch(batch_digest(), id)).unwrap();
    let handle = listener(primary_address, Some(Bytes::from(expected)));

    // Spawn enough workers' listeners to acknowledge our batches.
    for (_, addresses) in committee.others_workers(&authority.pubkey(), &id) {
        let address = addresses.worker_to_worker;
        let _ = listener(address, /* expected */ None);
    }

    // Send enough transactions to create a batch.
    let mut network = SimpleSender::new();
    let address = committee
        .worker(&authority.pubkey(), &id)
        .unwrap()
        .transactions;
    network.send(address, Bytes::from(transaction())).await;
    network.send(address, Bytes::from(transaction())).await;

    // Ensure the primary received the batch's digest (ie. it did not panic).
    assert!(handle.await.is_ok());
}
