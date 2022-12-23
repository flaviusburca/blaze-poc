use std::collections::BTreeMap;
use crate::keypair::Keypair;
use thiserror::Error;
use crate::committee::Committee;
use crate::pubkey::Pubkey;

pub struct ValidatorConfig {
    pub identity: Keypair,
    pub ledger_path: String,
    pub initial_committee: Committee,
    pub num_workers: u8,
    // pub protocol_key_pair: Keypair,
    // pub worker_key_pair: Keypair,
    // pub account_key_pair: Keypair,
    // pub network_key_pair: Keypair,
    // pub network_address: Multiaddr,
    // pub p2p_config: P2pConfig,
}

impl Default for ValidatorConfig {
    fn default() -> Self {
        Self {
            identity: Keypair::new(),
            // protocol_key_pair: Keypair::new(),
            // worker_key_pair: Keypair::new(),
            // account_key_pair: Keypair::new(),
            // network_key_pair: Keypair::new(),
            // network_address: Multiaddr::from_str("/ip4/127.0.0.1/tcp/20000/http").unwrap(),
            // p2p_config: P2pConfig::default(),
            ledger_path: "/tmp".to_string(),
            initial_committee: Committee {
                authorities: BTreeMap::default(),
                epoch: 0,
            },
            num_workers: 1
        }
    }
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Validator {0} is not in the committee")]
    NotInCommittee(Pubkey),
}