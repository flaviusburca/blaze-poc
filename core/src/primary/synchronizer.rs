use std::collections::HashMap;
use futures::TryFutureExt;
use tokio::sync::mpsc::Sender;
use mundis_ledger::Store;
use mundis_model::certificate::{Certificate, DagError, DagResult, Header};
use mundis_model::certificate::DagError::StoreError;
use mundis_model::committee::Committee;
use mundis_model::hash::{Hash, Hashable};
use mundis_model::pubkey::Pubkey;
use crate::primary::header_waiter::WaiterMessage;

/// The `Synchronizer` checks if we have all batches and parents referenced by a header. If we don't, it sends
/// a command to the `Waiter` to request the missing data.
pub struct Synchronizer {
    /// The public key of this primary.
    authority: Pubkey,
    /// The persistent storage.
    store: Store,
    /// Send commands to the `HeaderWaiter`.
    tx_header_waiter: Sender<WaiterMessage>,
    /// Send commands to the `CertificateWaiter`.
    tx_certificate_waiter: Sender<Certificate>,
    /// The genesis and its digests.
    genesis: Vec<(Hash, Certificate)>,
}

impl Synchronizer {
    pub fn new(
        authority: Pubkey,
        committee: &Committee,
        store: Store,
        tx_header_waiter: Sender<WaiterMessage>,
        tx_certificate_waiter: Sender<Certificate>,
    ) -> Self {
        Self {
            authority,
            store,
            tx_header_waiter,
            tx_certificate_waiter,
            genesis: Certificate::genesis(committee)
                .into_iter()
                .map(|x| (x.hash(), x))
                .collect(),
        }
    }

    /// Returns `true` if we have all transactions of the payload. If we don't, we return false,
    /// synchronize with other nodes (through our workers), and re-schedule processing of the
    /// header for when we will have its complete payload.
    pub async fn missing_payload(&mut self, header: &Header) -> DagResult<bool> {
        // We don't store the payload of our own workers.
        if header.author == self.authority {
            return Ok(false);
        }

        let mut missing = HashMap::new();
        for (digest, worker_id) in header.payload.iter() {
            // Check whether we have the batch. If one of our worker has the batch, the primary stores the pair
            // (digest, worker_id) in its own storage. It is important to verify that we received the batch
            // from the correct worker id to prevent the following attack:
            //      1. A Bad node sends a batch X to 2f good nodes through their worker #0.
            //      2. The bad node proposes a malformed block containing the batch X and claiming it comes
            //         from worker #1.
            //      3. The 2f good nodes do not need to sync and thus don't notice that the header is malformed.
            //         The bad node together with the 2f good nodes thus certify a block containing the batch X.
            //      4. The last good node will never be able to sync as it will keep sending its sync requests
            //         to workers #1 (rather than workers #0). Also, clients will never be able to retrieve batch
            //         X as they will be querying worker #1.
            let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
            if self.store.read(key).map_err(|e| DagError::StoreError(e.to_string())).await?.is_none() {
                missing.insert(digest.clone(), *worker_id);
            }
        }

        if missing.is_empty() {
            return Ok(false);
        }

        self.tx_header_waiter
            .send(WaiterMessage::SyncBatches(missing, header.clone()))
            .await
            .expect("Failed to send sync batch request");
        Ok(true)
    }

    /// Returns the parents of a header if we have them all. If at least one parent is missing,
    /// we return an empty vector, synchronize with other nodes, and re-schedule processing
    /// of the header for when we will have all the parents.
    pub async fn get_parents(&mut self, header: &Header) -> DagResult<Vec<Certificate>> {
        let mut missing = Vec::new();
        let mut parents = Vec::new();
        for hash in &header.parents {
            if let Some(genesis) = self
                .genesis
                .iter()
                .find(|(x, _)| x == hash)
                .map(|(_, x)| x)
            {
                parents.push(genesis.clone());
                continue;
            }

            match self.store.read(hash.to_vec()).map_err(|e| StoreError(e.to_string())).await? {
                Some(certificate) => parents.push(bincode::deserialize(&certificate)?),
                None => missing.push(hash.clone()),
            };
        }

        if missing.is_empty() {
            return Ok(parents);
        }

        self.tx_header_waiter
            .send(WaiterMessage::SyncParents(missing, header.clone()))
            .await
            .expect("Failed to send sync parents request");
        Ok(Vec::new())
    }

    /// Check whether we have all the ancestors of the certificate. If we don't, send the certificate to
    /// the `CertificateWaiter` which will trigger re-processing once we have all the missing data.
    pub async fn deliver_certificate(&mut self, certificate: &Certificate) -> DagResult<bool> {
        for digest in &certificate.header.parents {
            if self.genesis.iter().any(|(x, _)| x == digest) {
                continue;
            }

            if self.store.read(digest.to_vec()).map_err(|e| StoreError(e.to_string())).await?.is_none() {
                self.tx_certificate_waiter
                    .send(certificate.clone())
                    .await
                    .expect("Failed to send sync certificate request");
                return Ok(false);
            };
        }
        Ok(true)
    }
}

