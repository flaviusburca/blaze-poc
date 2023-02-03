use {itertools::rev, tokio::signal::ctrl_c};
// Copyright(C) Mundis
use {
    itertools::Itertools,
    log::{debug, error},
    mundis_model::{
        certificate::Certificate,
        committee::Committee,
        hash::{Hash, Hashable},
        pubkey::Pubkey,
        Round, View,
    },
    std::{
        cmp::max,
        collections::{HashMap, HashSet},
    },
    tokio::sync::mpsc::{Receiver, Sender},
};
/// The representation of the DAG in memory.
type Dag = HashMap<Round, HashMap<Pubkey, (Hash, Certificate)>>;

/// The state that needs to be persisted for crash-recovery.
struct State {
    /// The last committed view.
    last_committed_round: Round,
    // Keeps the last committed round for each authority. This map is used to clean up the dag and
    // ensure we don't commit twice the same certificate.
    last_committed: HashMap<Pubkey, Round>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    dag: Dag,
    /// Keeps a mapping between views and rounds
    rounds: HashMap<Round, View>,
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.hash(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_round: 0,
            last_committed: genesis.iter().map(|(x, (_, y))| (*x, y.round())).collect(),
            dag: [(0, genesis)].iter().cloned().collect(),
            rounds: HashMap::new(),
        }
    }

    /// Update and clean up internal state base on committed certificates.
    fn update(&mut self, certificate: &Certificate, gc_depth: Round) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        let last_committed_round = *self.last_committed.values().max().unwrap();
        self.last_committed_round = last_committed_round;

        // TODO: This cleanup is dangerous: we need to ensure consensus can receive idempotent replies
        // from the primary. Here we risk cleaning up a certificate and receiving it again later.
        for (name, round) in &self.last_committed {
            self.dag.retain(|r, authorities| {
                authorities.retain(|n, _| n != name || r >= round);
                !authorities.is_empty() && r + gc_depth >= last_committed_round
            });
        }
    }

    #[allow(unused)]
    pub fn dump(&self, prefix: Option<String>) {
        let mut msg = format!(
            "{}DAG: \n\
            ------------------",
            prefix.unwrap_or("".to_string())
        );
        for round in self.dag.keys().sorted() {
            msg += &*format!("\n\tRound: {}", round);
            let entry = self.dag.get(round);
            if entry.is_some() {
                for pubkey in entry.unwrap().keys().into_iter() {
                    let pair = entry.unwrap().get(pubkey);
                    if pair.is_some() {
                        let (digest, _certificate) = pair.unwrap();
                        msg += &*format!("\n\t Author={}, Certificate_Digest={}", pubkey, digest);
                    } else {
                        msg += &*format!("\n\t Empty");
                    }
                }
            } else {
                msg += "\n\tEmpty";
            }
        }

        debug!("{}", msg);
    }
}

pub struct Consensus {
    /// The committee information.
    committee: Committee,
    /// The depth of the garbage collector.
    gc_depth: Round,

    /// Receives new certificates from the primary. The primary should send us new certificates only if it already sent us its whole history.
    rx_primary: Receiver<Certificate>,
    /// Receives the leader's certificate for commit
    rx_commit_view: Receiver<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    tx_primary: Sender<Certificate>,
    /// Outputs the sequence of ordered certificates to the application layer.
    tx_output: Sender<Certificate>,

    /// The genesis certificates.
    genesis: Vec<Certificate>,
}

impl Consensus {
    pub fn spawn(
        committee: Committee,
        gc_depth: Round,
        rx_primary: Receiver<Certificate>,
        rx_commit_view: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<Certificate>,
    ) {
        tokio::spawn(async move {
            Self {
                committee: committee.clone(),
                gc_depth,
                rx_primary,
                rx_commit_view,
                tx_primary,
                tx_output,
                genesis: Certificate::genesis(&committee),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // The consensus state (everything else is immutable).
        let mut state = State::new(self.genesis.clone());
        // state.dump(Some("\nGENESIS ".to_string()));

        loop {
            tokio::select! {
                Some(certificate) = self.rx_primary.recv() => {
                    let round = certificate.round();

                    // Add the new certificate to the local storage.
                    state.dag
                        .entry(round)
                        .or_insert_with(HashMap::new)
                        .insert(certificate.origin(), (certificate.hash(), certificate));
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;
