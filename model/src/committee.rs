// Copyright(C) Mundis.
use std::fs;
use std::path::PathBuf;
use {
    crate::{base_types::Epoch, config::ConfigError, pubkey::Pubkey, Stake, WorkerId},
    serde::{Deserialize, Serialize},
    std::{
        collections::{BTreeMap, HashMap},
        net::SocketAddr,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct PrimaryAddresses {
    /// Address to receive messages from other primaries (WAN).
    pub primary_to_primary: SocketAddr,
    /// Address to receive messages from our workers (LAN).
    pub worker_to_primary: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, Hash, PartialEq)]
pub struct WorkerAddresses {
    /// Address to receive client transactions (WAN).
    pub transactions: SocketAddr,
    /// Address to receive messages from other workers (WAN).
    pub worker_to_worker: SocketAddr,
    /// Address to receive messages from our primary (LAN).
    pub primary_to_worker: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExecutorAddresses {
    /// Address to receive messages from our workers (WAN).
    pub worker_to_executor: SocketAddr,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct Authority {
    /// The voting power of this authority.
    pub stake: Stake,
    pub name: String,
    /// The network addresses of the primary.
    pub primary: PrimaryAddresses,
    /// Map of workers' id and their network addresses.
    pub workers: HashMap<WorkerId, WorkerAddresses>,
    /// The network addresses of the executor.
    pub executor: ExecutorAddresses,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct Committee {
    pub authorities: BTreeMap<Pubkey, Authority>,
    /// The epoch number of this committee
    pub epoch: Epoch,
}

impl Committee {
    /// Returns the current epoch.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Returns the number of authorities.
    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    /// Return the stake of a specific authority.
    pub fn stake(&self, name: &Pubkey) -> Stake {
        self.authorities.get(name).map_or_else(|| 0, |x| x.stake)
    }

    /// Returns the stake of all authorities except `myself`.
    pub fn others_stake(&self, myself: &Pubkey) -> Vec<(Pubkey, Stake)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.stake))
            .collect()
    }

    /// Returns the stake required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        2 * total_votes / 3 + 1
    }

    /// Returns the stake required to reach availability (f+1).
    pub fn validity_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        (total_votes + 2) / 3
    }

    /// Returns a leader node as a weighted choice seeded by the provided integer
    pub fn leader(&self, seed: usize) -> (Pubkey, String) {
        let mut keys: Vec<_> = self.authorities.keys().cloned().collect();
        keys.sort();
        let leader = keys[seed % self.size()];
        let name = self.authorities.get(&leader).unwrap().name.clone();
        (leader, name)
    }

    /// Returns the primary address of the target primary.
    pub fn primary_address(&self, authority: &Pubkey) -> Result<PrimaryAddresses, ConfigError> {
        self.authorities
            .get(&authority.clone())
            .map(|x| x.primary.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(*authority))
    }

    /// Returns the addresses of all primaries except `myself`.
    pub fn others_primary_addresses(&self, myself: &Pubkey) -> Vec<(Pubkey, PrimaryAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.primary.clone()))
            .collect()
    }

    /// Returns the addresses of a specific worker (`id`) of a specific authority (`to`).
    pub fn worker(
        &self,
        authority: &Pubkey,
        id: &WorkerId,
    ) -> Result<WorkerAddresses, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &authority)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee(*authority))?
            .workers
            .iter()
            .find(|(worker_id, _)| worker_id == &id)
            .map(|(_, worker)| worker.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(*authority))
    }

    /// Returns the addresses of all our workers.
    pub fn our_workers(&self, myself: &Pubkey) -> Result<Vec<WorkerAddresses>, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &myself)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee(*myself))?
            .workers
            .values()
            .cloned()
            .map(Ok)
            .collect()
    }

    /// Returns the addresses of all workers with a specific id except the ones of the authority
    /// specified by `myself`.
    pub fn others_workers(&self, myself: &Pubkey, id: &WorkerId) -> Vec<(Pubkey, WorkerAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .filter_map(|(name, authority)| {
                authority
                    .workers
                    .iter()
                    .find(|(worker_id, _)| worker_id == &id)
                    .map(|(_, addresses)| (*name, addresses.clone()))
            })
            .collect()
    }

    /// Returns the address of our executor.
    pub fn executor(&self, myself: &Pubkey) -> Result<ExecutorAddresses, ConfigError> {
        self.authorities
            .get(myself)
            .map(|x| x.executor.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(*myself))
    }

    pub fn for_testing(pubkey: Pubkey, base_port: u32, num_workers: u8) -> Self {
        let mut authorities = BTreeMap::new();
        authorities.insert(
            pubkey,
            Authority {
                stake: 100,
                name: "".to_string(),
                primary: PrimaryAddresses {
                    primary_to_primary: format!("0.0.0.0:{}", base_port).parse().unwrap(),
                    worker_to_primary: format!("0.0.0.0:{}", base_port + 1).parse().unwrap(),
                },
                executor: ExecutorAddresses {
                    worker_to_executor: format!("0.0.0.0:{}", base_port + 2).parse().unwrap(),
                },
                workers: (0..num_workers)
                    .map(|i| {
                        let i = i as u32;
                        (
                            i as WorkerId,
                            WorkerAddresses {
                                transactions: format!("0.0.0.0:{}", base_port + (i + 1) * 100 + 1).parse().unwrap(),
                                worker_to_worker: format!("0.0.0.0:{}", base_port + (i + 1) * 100 + 4).parse().unwrap(),
                                primary_to_worker: format!("0.0.0.0:{}", base_port + (i + 1) * 100 + 5).parse()
                                .unwrap(),
                            },
                        )
                    })
                    .collect(),
            },
        );

        Self {
            epoch: 0,
            authorities,
        }
    }

    pub fn load(file: PathBuf) -> Self {
        let data = fs::read(file).expect("Could not read Committee");
        serde_json::from_slice(data.as_slice()).expect("Could not read Committee")
    }
}

impl Default for Committee {
    fn default() -> Self {
        Self {
            epoch: 0,
            authorities: BTreeMap::default(),
        }
    }
}
