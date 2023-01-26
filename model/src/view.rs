use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::pubkey::Pubkey;
use crate::Round;

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct View {
    pub current: i64,
    pub round: Round,
    pub leader: Pubkey,
    pub leader_name: String
}

impl View {
    pub fn new(round: Round) -> Self {
        Self {
            current: 1,
            round,
            leader: Pubkey::default(),
            leader_name: "".to_string()
        }
    }
}

impl Display for View {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "view(r={}) = {}", self.round, self.current)
    }
}
