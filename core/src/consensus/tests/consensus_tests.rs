// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use {
    mundis_model::{
        certificate::Header,
        committee::{Authority, ExecutorAddresses, PrimaryAddresses},
        keypair::Keypair,
        signature::Signer,
    },
    rand::{rngs::StdRng, SeedableRng},
    std::collections::{BTreeSet, VecDeque},
    tokio::sync::mpsc::channel,
};

pub fn keys() -> Vec<Keypair> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| Keypair::generate(&mut rng)).collect()
}

pub fn mock_committee() -> Committee {
    Committee {
        epoch: 0,
        authorities: keys()
            .iter()
            .map(|keypair| {
                (
                    keypair.pubkey(),
                    Authority {
                        stake: 1,
                        primary: PrimaryAddresses {
                            primary_to_primary: "0.0.0.0:0".parse().unwrap(),
                            worker_to_primary: "0.0.0.0:0".parse().unwrap(),
                        },
                        workers: HashMap::default(),
                        executor: ExecutorAddresses {
                            worker_to_executor: "0.0.0.0:0".parse().unwrap(),
                        },
                    },
                )
            })
            .collect(),
    }
}

fn mock_certificate(origin: Pubkey, round: Round, parents: BTreeSet<Hash>) -> (Hash, Certificate) {
    let certificate = Certificate {
        header: Header {
            author: origin,
            round,
            parents,
            ..Header::default()
        },
        ..Certificate::default()
    };
    (certificate.hash(), certificate)
}

// Creates one certificate per authority starting and finishing at the specified rounds (inclusive).
// Outputs a VecDeque of certificates (the certificate with higher round is on the front) and a set
// of digests to be used as parents for the certificates of the next round.
fn make_certificates(
    start: Round,
    stop: Round,
    initial_parents: &BTreeSet<Hash>,
    keys: &[Pubkey],
) -> (VecDeque<Certificate>, BTreeSet<Hash>) {
    let mut certificates = VecDeque::new();
    let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
    let mut next_parents = BTreeSet::new();

    for round in start..=stop {
        next_parents.clear();
        for pubkey in keys {
            let (hash, certificate) = mock_certificate(*pubkey, round, parents.clone());
            certificates.push_back(certificate);
            next_parents.insert(hash);
        }
        parents = next_parents.clone();
    }
    (certificates, next_parents)
}

