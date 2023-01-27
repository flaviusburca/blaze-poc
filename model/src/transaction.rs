// Copyright(C) Mundis.
use {
    crate::{account::Account, hash::Hash},
    serde::{Deserialize, Serialize},
};

/// A transaction updating or creating objects.
#[derive(Serialize, Deserialize, Debug)]
pub struct Transaction {
    /// The unique id of the transaction.
    pub id: Hash,
    /// The list of objects that this transaction reads or modifies.
    pub inputs: Vec<Account>,
    /// Represents the smart contract to execute. In this fake transaction,
    /// it determines the number of ms of CPU time needed to execute it.
    pub execution_time: u64,
}
