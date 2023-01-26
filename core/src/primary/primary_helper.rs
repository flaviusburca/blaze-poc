use log::info;
use {
    crate::primary::PrimaryMessage,
    bytes::Bytes,
    log::{error, warn},
    mundis_ledger::Store,
    mundis_model::{committee::Committee, hash::Hash, pubkey::Pubkey},
    mundis_network::simple_sender::SimpleSender,
    tokio::sync::mpsc::Receiver,
};
use mundis_model::certificate::Certificate;

/// A task dedicated to help other authorities by replying to their certificates requests.
pub struct PrimaryHelper {
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive certificates requests.
    rx_primaries: Receiver<(Vec<Hash>, Pubkey)>,
    /// A network sender to reply to the sync requests.
    network: SimpleSender,
}

impl PrimaryHelper {
    pub fn spawn(
        committee: Committee,
        store: Store,
        rx_primaries: Receiver<(Vec<Hash>, Pubkey)>
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                store,
                rx_primaries,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some((digests, origin)) = self.rx_primaries.recv().await {
            // TODO [issue #195]: Do some accounting to prevent bad nodes from monopolizing our resources.

            // get the requestors address.
            let address = match self.committee.primary_address(&origin) {
                Ok(x) => x.primary_to_primary,
                Err(e) => {
                    warn!("Unexpected certificate request: {}", e);
                    continue;
                }
            };

            info!("READING certificates from DB");

            // Reply to the request (the best we can).
            for digest in digests {
                match self.store.read(digest.to_vec()).await {
                    Ok(Some(data)) => {
                        // TODO: Remove this deserialization-serialization in the critical path.
                        let certificate: Certificate = bincode::deserialize(&data)
                            .expect("Failed to deserialize our own certificate");
                        info!("FOUND certificate {} in DB", certificate.header);
                        let bytes = bincode::serialize(&PrimaryMessage::Certificate(certificate))
                            .expect("Failed to serialize our own certificate");
                        info!("SIMPLE SEND PrimaryMessage::Certificate");
                        self.network.send(address, Bytes::from(bytes)).await;
                    }
                    Ok(None) => info!("Certificate not found in DB"),
                    Err(e) => error!("{}", e),
                }
            }
        }
    }
}
