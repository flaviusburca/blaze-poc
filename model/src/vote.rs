use {
    crate::{
        certificate::{DagError, DagResult, Header},
        committee::Committee,
        hash::{Hash, Hashable, Hasher},
        keypair::Keypair,
        pubkey::Pubkey,
        signature::{Signature, Signer},
        Round,
    },
    serde::{Deserialize, Serialize},
    std::fmt,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Vote {
    pub id: Hash,
    pub round: Round,
    pub origin: Pubkey,
    pub author: Pubkey,
    pub signature: Signature,
}

impl Vote {
    pub async fn new(header: &Header, author: &Keypair) -> Self {
        let vote = Self {
            id: header.id.clone(),
            round: header.round,
            origin: header.author,
            author: author.pubkey(),
            signature: Signature::default(),
        };

        let id = vote.hash();
        let signature = author.sign_message(id.as_ref());

        Vote {
            signature,
            ..vote
        }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the authority has voting rights.
        if committee.stake(&self.author) <= 0 {
            return Err(DagError::UnknownAuthority(self.author));
        }

        // Check the signature.
        if self
            .signature
            .verify(self.author.as_ref(), self.hash().as_ref())
        {
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
