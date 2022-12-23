use std::borrow::Cow;
use std::fmt;
use serde::{Serialize, Deserialize};
use crate::certificate::{DagError, DagResult, Header};
use crate::committee::Committee;
use crate::hash::{Hash, Hashable, Hasher};
use crate::keypair::Keypair;
use crate::pubkey::Pubkey;
use crate::Round;
use crate::signature::{Signable, Signature, Signer};

#[derive(Clone, Serialize, Deserialize)]
pub struct Vote {
    pub id: Hash,
    pub round: Round,
    pub origin: Pubkey,
    pub author: Pubkey,
    pub signature: Signature,
}

impl Vote {
    pub async fn new(
        header: &Header,
        author: &Keypair,
    ) -> Self {
        let mut vote = Self {
            id: header.id.clone(),
            round: header.round,
            origin: header.author,
            author: author.pubkey(),
            signature: Signature::default(),
        };
        vote.sign(author);
        vote
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the authority has voting rights.
        if committee.stake(&self.author) <= 0 {
            return Err(DagError::UnknownAuthority(self.author));
        }

        // Check the signature.
        if self.signature.verify(self.author.as_ref(), self.hash().as_ref()){
            Ok(())
        } else {
            Err(DagError::InvalidSignature)
        }
    }
}

impl Hashable for Vote {
    fn hash(&self) -> Hash {
        let mut hasher = Hasher::default();
        hasher.hashr(self.id);
        hasher.hashr(self.round.to_le_bytes());
        hasher.hashr(self.origin);
        hasher.result()
    }
}

impl Signable for Vote {
    fn pubkey(&self) -> Pubkey {
        self.author
    }

    fn signable_data(&self) -> Cow<[u8]> {
        Cow::Owned(self.id.as_ref().into())
    }

    fn get_signature(&self) -> Signature {
        self.signature
    }

    fn set_signature(&mut self, signature: Signature) {
        self.signature = signature;
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: V{}({}, {})",
            self.hash(),
            self.round,
            self.author,
            self.id
        )
    }
}