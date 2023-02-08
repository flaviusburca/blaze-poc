use log::info;
use {
    crate::worker::SerializedBatchDigestMessage, bytes::Bytes,
    mundis_network::simple_sender::SimpleSender, std::net::SocketAddr, tokio::sync::mpsc::Receiver,
};

// Send batches' digests to the primary.
pub struct MasterConnector {
    /// The network address of the master.
    master_address: SocketAddr,
    /// Input channel to receive the digests to send to the primary.
    rx_digest: Receiver<SerializedBatchDigestMessage>,
    /// A network sender to send the baches' digests to the primary.
    network: SimpleSender,
}

impl MasterConnector {
    pub fn spawn(master_address: SocketAddr, rx_digest: Receiver<SerializedBatchDigestMessage>) {
        tokio::spawn(async move {
            Self {
                master_address: master_address,
                rx_digest,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some(digest) = self.rx_digest.recv().await {
            // Send the digest through the network.
            info!("SIMPLE SEND SerializedBatchDigestMessage");
            self.network
                .send(self.master_address, Bytes::from(digest))
                .await;
        }
    }
}
