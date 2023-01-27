// Copyright(C) Mundis.
use {
    crate::{committee::Committee, keypair::Keypair, pubkey::Pubkey},
    std::collections::BTreeMap,
    thiserror::Error,
};

pub struct ValidatorConfig {
    pub identity: Keypair,
    pub ledger_path: String,
    pub initial_committee: Committee,
}

impl Default for ValidatorConfig {
    fn default() -> Self {
        Self {
            identity: Keypair::new(),
            ledger_path: "/tmp".to_string(),
            initial_committee: Committee {
                authorities: BTreeMap::default(),
                epoch: 0,
            },
        }
    }
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Validator {0} is not in the committee")]
    NotInCommittee(Pubkey),
}
