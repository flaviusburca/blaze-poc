use log::error;
// Copyright(C) Facebook, Inc. and its affiliates.
use {
    mundis_model::{
        certificate::{Certificate, DagError, DagResult, Header},
        committee::Committee,
        pubkey::Pubkey,
        signature::Signature,
        vote::Vote,
        Stake,
    },
    std::collections::HashSet,
};
use mundis_model::hash::Hash;
use mundis_model::View;


pub struct ConsensusComplaintsAggregator {
    weight: Stake,
    votes: Vec<Hash>,
    used: HashSet<Pubkey>,
}

impl ConsensusComplaintsAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    pub fn append(
        &mut self,
        committee: &Committee,
        header: &Header,
    ) -> DagResult<Option<bool>> {
        let author = header.author;

        // Ensure it is the first time this authority votes.
        if !self.used.insert(author) {
            return Err(DagError::AuthorityReuse(author));
        }

        self.votes.push(header.id);
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures quorum is only reached once.
            return Ok(Some(true));
        }
        Ok(None)
    }
}

pub struct ConsensusVotesAggregator {
    weight: Stake,
    votes: Vec<Hash>,
    used: HashSet<Pubkey>,
}

impl ConsensusVotesAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    pub fn append(
        &mut self,
        committee: &Committee,
        header: &Header,
    ) -> DagResult<Option<bool>> {
        let author = header.author;

        // Ensure it is the first time this authority votes.
        if !self.used.insert(author) {
            return Err(DagError::AuthorityReuse(author));
        }

        self.votes.push(header.id);
        self.weight += committee.stake(&author);
        if self.weight >= committee.validity_threshold() {
            self.weight = 0; // Ensures quorum is only reached once.
            return Ok(Some(true));
        }
        Ok(None)
    }
}


/// Aggregates votes for a particular header into a certificate.
pub struct VotesAggregator {
    weight: Stake,
    votes: Vec<(Pubkey, Signature)>,
    used: HashSet<Pubkey>,
}

impl VotesAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    pub fn append(
        &mut self,
        vote: Vote,
        committee: &Committee,
        header: &Header,
    ) -> DagResult<Option<Certificate>> {
        let author = vote.author;

        // Ensure it is the first time this authority votes.
        if !self.used.insert(author) {
            return Err(DagError::AuthorityReuse(author));
        }

        self.votes.push((author, vote.signature));
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures quorum is only reached once.
            return Ok(Some(Certificate {
                header: header.clone(),
                votes: self.votes.clone(),
                meta: 0
            }));
        }
        Ok(None)
    }
}

/// Aggregate certificates and check if we reach a quorum.
pub struct CertificatesAggregator {
    weight: Stake,
    certificates: Vec<Certificate>,
    used: HashSet<Pubkey>,
}

impl CertificatesAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            certificates: Vec::new(),
            used: HashSet::new(),
        }
    }

    pub fn append(
        &mut self,
        certificate: Certificate,
        committee: &Committee,
    ) -> DagResult<Option<Vec<Certificate>>> {
        let origin = certificate.origin();

        // Ensure it is the first time this authority votes.
        if !self.used.insert(origin) {
            return Ok(None);
        }

        self.certificates.push(certificate);
        self.weight += committee.stake(&origin);
        if self.weight >= committee.quorum_threshold() {
            //self.weight = 0; // Ensures quorum is only reached once.
            return Ok(Some(self.certificates.drain(..).collect()));
        }
        Ok(None)
    }
}

