// Copyright(C) Mundis.
use {
    crate::{
        base_types::Epoch,
        committee::Committee,
        hash::{Hash, Hashable, Hasher},
        keypair::Keypair,
        pubkey::Pubkey,
        Round,
        signature::{Signature, Signer}, WorkerId,
    },
    serde::{Deserialize, Serialize},
    std::{
        collections::{BTreeMap, BTreeSet, HashSet},
        fmt,
        time::SystemTime,
    },
    thiserror::Error,
};

use crate::View;

pub type DagResult<T> = Result<T, DagError>;

#[derive(Debug, Error)]
pub enum DagError {
    #[error("Invalid signature")]
    InvalidSignature,

    #[error("Storage failure: {0}")]
    StoreError(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Invalid header id")]
    InvalidHeaderId,

    #[error("Malformed header {0}")]
    MalformedHeader(Hash),

    #[error("Received message from unknown authority {0}")]
    UnknownAuthority(Pubkey),

    #[error("Authority {0} appears in quorum more than once")]
    AuthorityReuse(Pubkey),

    #[error("Received unexpected vote for header {0}")]
    UnexpectedVote(Hash),

    #[error("Received certificate without a quorum")]
    CertificateRequiresQuorum,

    #[error("Parents of header {0} are not a quorum")]
    HeaderRequiresQuorum(Hash),

    #[error("Message {0} (round {1}) too old")]
    TooOld(Hash, Round),

    #[error("Invalid epoch (expected {expected}, received {received})")]
    InvalidEpoch { expected: Epoch, received: Epoch },
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Certificate {
    pub header: Header,
    pub votes: Vec<(Pubkey, Signature)>,
}

impl Certificate {
    pub fn genesis(committee: &Committee) -> Vec<Self> {
        committee
            .authorities
            .keys()
            .map(|pubkey| Self {
                header: Header {
                    author: *pubkey,
                    ..Header::default()
                },
                ..Self::default()
            })
            .collect()
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Genesis certificates are always valid.
        if Self::genesis(committee).contains(self) {
            return Ok(());
        }

        // Check the embedded header.
        self.header.verify(committee)?;

        // Ensure the certificate has a quorum.
        let mut weight = 0;
        let mut used = HashSet::new();
        for (pubkey, _) in self.votes.iter() {
            if used.contains(pubkey) {
                return Err(DagError::AuthorityReuse(pubkey.clone()));
            }

            let voting_rights = committee.stake(pubkey);
            if voting_rights <= 0 {
                return Err(DagError::UnknownAuthority(pubkey.clone()));
            }

            used.insert(*pubkey);
            weight += voting_rights;
        }

        if weight < committee.quorum_threshold() {
            return Err(DagError::CertificateRequiresQuorum);
        }

        // Check the signatures.
        Signature::verify_batch(&self.hash(), &self.votes).map_err(|_| DagError::InvalidSignature)
    }

    pub fn round(&self) -> Round {
        self.header.round
    }

    pub fn view(&self) -> View {
        self.header.meta.abs() as View
    }

    pub fn origin(&self) -> Pubkey {
        self.header.author
    }
}

impl Hashable for Certificate {
    fn hash(&self) -> Hash {
        let mut hasher = Hasher::default();
        hasher.hashr(self.header.id);
        hasher.hashr(self.round().to_le_bytes());
        hasher.hashr(self.origin());
        hasher.result()
    }
}

impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "C(id={}, view={}, round={}, origin={}, header={})",
            self.hash(),
            self.view(),
            self.round(),
            self.origin(),
            self.header.id
        )
    }
}

impl PartialEq for Certificate {
    fn eq(&self, other: &Self) -> bool {
        let mut ret = self.header.id == other.header.id;
        ret &= self.round() == other.round();
        ret &= self.origin() == other.origin();
        ret
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Header {
    pub author: Pubkey,
    pub round: Round,
    pub meta: View,
    pub epoch: Epoch,
    pub created_at: u64,
    pub payload: BTreeMap<Hash, WorkerId>,
    // hashes of certificates from the previous round
    pub parents: BTreeSet<Hash>,
    pub id: Hash,
    pub signature: Signature,
}

impl Header {
    pub fn new(
        author: Keypair,
        round: Round,
        payload: BTreeMap<Hash, WorkerId>,
        parents: BTreeSet<Hash>,
    ) -> Self {
        let created_at = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(n) => n.as_millis() as u64,
            Err(_) => panic!("SystemTime before UNIX EPOCH!"),
        };

        let header = Self {
            author: author.pubkey(),
            round,
            meta: 0,
            epoch: 0,
            created_at,
            payload,
            parents,
            id: Hash::default(),
            signature: Signature::default(),
        };

        let id = header.hash();
        let signature = author.sign_message(id.as_ref());

        Header {
            id,
            signature,
            ..header
        }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the header is from the correct epoch.
        if self.epoch != committee.epoch() {
            return Err(DagError::InvalidEpoch {
                expected: committee.epoch(),
                received: self.epoch,
            });
        }

        // Ensure the header id is well formed.
        if self.hash() != self.id {
            return Err(DagError::InvalidHeaderId);
        }

        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        if voting_rights <= 0 {
            return Err(DagError::UnknownAuthority(self.author));
        }

        // Ensure all worker ids are correct.
        for worker_id in self.payload.values() {
            committee
                .worker(&self.author, worker_id)
                .map_err(|_| DagError::MalformedHeader(self.id.clone()))?;
        }

        // Check the signature.
        if !self
            .signature
            .verify(&self.author.as_ref(), &self.id.as_ref())
        {
            return Err(DagError::InvalidSignature);
        }

        Ok(())
    }
}

impl Hashable for Header {
    fn hash(&self) -> Hash {
        let mut hasher = Hasher::default();
        hasher.hash(self.author.as_ref());
        hasher.hash(&self.round.to_le_bytes());
        for (x, y) in &self.payload {
            hasher.hash(&x.to_bytes());
            hasher.hash(&y.to_le_bytes());
        }
        for x in &self.parents {
            hasher.hash(&x.to_bytes());
        }
        hasher.result()
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "H(id={}, round={}, author={}, payload={})",
            self.id,
            self.round,
            self.author,
            self.payload.keys().map(|x| x.size()).sum::<usize>(),
        )
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "B{}({})", self.round, self.author)
    }
}
