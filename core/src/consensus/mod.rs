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

                Some(certificate) = self.rx_commit_view.recv() => {
                    // commit the leader's certificate and it's causal history
                    // at this point, the current round has 2f+1 certificates
                    let round = certificate.round();
                    let view = certificate.view() - 1;
                    let leader_key = certificate.origin();
                    error!("(v={}, r={}) Commit certificate for leader {}", view, round, certificate.origin());

                    match state.dag.get(&round).map(|x| x.get(&leader_key)).flatten() {
                        Some(x) => {
                            // the certificate is in the store, do nothing
                        }
                        None => {
                            state.dag
                                .entry(round)
                                .or_insert_with(HashMap::new)
                                .insert(certificate.origin(), (certificate.hash(), certificate.clone()));
                        }
                    }

                    if state.rounds.contains_key(&round) {
                        panic!("The view is already committed");
                    }

                    state.rounds.insert(round, view);

                    if state.rounds.len() < 2 {
                        // this is the first view, nothing to do
                        debug!("(v={}, r={}) this is the first view, nothing to do", view, round);
                        continue;
                    }

                    // Get an ordered list of past leaders that are linked to the current leader.
                    let mut sequence = Vec::new();
                    for leader in self.order_leaders(&certificate, &state).iter().rev() {
                        // Starting from the oldest leader, flatten the sub-dag referenced by the leader.
                        for x in self.order_dag(leader, &state) {
                            // Update and clean up internal state.
                            state.update(&x, self.gc_depth);

                            // Add the certificate to the sequence.
                            sequence.push(x);
                        }

                        error!("\t (v={}, r={}) committed view {}", view, round, leader.view());
                    }
                }
            }
        }
    }

    fn order_leaders(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        let mut to_commit = vec![leader.clone()];
        let mut leader = leader;

        debug!("\t Ordering leaders: last_committed_round={}", state.last_committed_round);

        for r in (state.last_committed_round + 1..leader.round()).rev() {
            // Does this round have a leader ?.
            let (_, prev_leader) = match self.leader(r, &state) {
                Some(x) => {
                    debug!("\t leader({})={}", r, x.1.origin());
                    x
                }
                None => {
                    debug!("\t no leader for r={}", r);
                    continue;
                }
            };

            // Check whether there is a path between the last two leaders.
            if self.linked(leader, prev_leader, &state.dag) {
                to_commit.push(prev_leader.clone());
                leader = prev_leader;
            }
        }

        to_commit
    }

    fn leader<'a>(&self, round: Round, state: &'a State) -> Option<&'a (Hash, Certificate)> {
        if let Some(view) = state.rounds.get(&round) {
            let (leader_key, _) = self.elect_leader(*view);
            return state.dag.get(&round).map(|x| x.get(&leader_key)).flatten();
        }

        None
    }

    /// Checks if there is a path between two leaders.
    fn linked(&self, leader: &Certificate, prev_leader: &Certificate, dag: &Dag) -> bool {
        let mut parents = vec![leader];
        for r in (prev_leader.round()..leader.round()).rev() {
            parents = dag
                .get(&(r))
                .expect("We should have the whole history by now")
                .values()
                .filter(|(digest, _)| parents.iter().any(|x| x.header.parents.contains(digest)))
                .map(|(_, certificate)| certificate)
                .collect();
        }
        parents.contains(&prev_leader)
    }

    fn elect_leader(&self, view: View) -> (Pubkey, String) {
        self.committee.leader(view as usize)
    }

    /// Flatten the dag referenced by the input certificate. This is a classic depth-first search (pre-order):
    /// https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
    fn order_dag(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();

        let mut buffer = vec![leader];
        while let Some(x) = buffer.pop() {
            ordered.push(x.clone());
            for parent in &x.header.parents {
                let (digest, certificate) = match state
                    .dag
                    .get(&(x.round() - 1))
                    .map(|x| x.values().find(|(x, _)| x == parent))
                    .flatten()
                {
                    Some(x) => x,
                    None => continue, // We already ordered or GC up to here.
                };

                // We skip the certificate if we (1) already processed it or (2) we reached a round that we already
                // committed for this authority.
                let mut skip = already_ordered.contains(&digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or_else(|| false, |r| r == &certificate.round());
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                }
            }
        }

        // Ensure we do not commit garbage collected certificates.
        ordered.retain(|x| x.round() + self.gc_depth >= state.last_committed_round);

        // Ordering the output by round is not really necessary but it makes the commit sequence prettier.
        ordered.sort_by_key(|x| x.round());
        ordered
    }
}

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;
