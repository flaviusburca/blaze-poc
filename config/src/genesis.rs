use {
    mundis_model::{
        account::Account,
        base_types::{ChainId, UnixTimestamp, CHAIN_LOCAL},
        pubkey::Pubkey,
    },
    serde::{Deserialize, Serialize},
    std::{
        collections::BTreeMap,
        time::{SystemTime, UNIX_EPOCH},
    },
};

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct GenesisConfig {
    /// when the network (bootstrap validator) was started relative to the UNIX Epoch
    pub creation_time: UnixTimestamp,
    /// initial accounts
    pub accounts: BTreeMap<Pubkey, Account>,
    pub chain_id: ChainId,
}

impl Default for GenesisConfig {
    fn default() -> Self {
        Self {
            creation_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as UnixTimestamp,
            accounts: BTreeMap::default(),
            chain_id: CHAIN_LOCAL,
        }
    }
}
