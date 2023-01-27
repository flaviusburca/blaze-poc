// Copyright(C) Mundis
use log::error;
use {
    itertools::Itertools,
    log::{debug, info, log_enabled, warn},
    mundis_model::{
        certificate::Certificate,
        committee::Committee,
        hash::{Hash, Hashable},
        pubkey::Pubkey,
        Round, Stake,
    },
    std::{
        cmp::max,
        collections::{HashMap, HashSet},
        time::Duration,
    },
    tokio::{
        sync::mpsc::{Receiver, Sender},
        time::sleep,
    },
};
use mundis_model::View;

/// The representation of the DAG in memory.
type Dag = HashMap<View, HashMap<Pubkey, (Hash, Certificate)>>;

/// The state that needs to be persisted for crash-recovery.
struct State {
    /// The last committed view.
    last_committed_view: View,
    // Keeps the last committed view for each authority. This map is used to clean up the dag and
    // ensure we don't commit twice the same certificate.
    last_committed: HashMap<Pubkey, View>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    dag: Dag,
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.hash(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_view: 0,
            last_committed: genesis.iter().map(|(x, (_, y))| (*x, y.view())).collect(),
            dag: [(0, genesis)].iter().cloned().collect(),
        }
    }

    /// Update and clean up internal state base on committed certificates.
    fn update(&mut self, certificate: &Certificate, gc_depth: View) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.view()))
            .or_insert_with(|| certificate.view());

        let last_committed_view = *self.last_committed.values().max().unwrap();
        self.last_committed_view = last_committed_view;

        error!("Consensus: last committed view: {}", self.last_committed_view);

        // TODO: This cleanup is dangerous: we need to ensure consensus can receive idempotent replies
        // from the primary. Here we risk cleaning up a certificate and receiving it again later.
        for (name, view) in &self.last_committed {
            self.dag.retain(|v, authorities| {
                authorities.retain(|n, _| n != name || v >= view);
                !authorities.is_empty() && v + gc_depth >= last_committed_view
            });
        }
    }

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
                        let votes = _certificate.votes.iter().map(|c| c.0).join(", ");
                        msg += &*format!(
                            "\n\t Author={}, Certificate_Digest={}, Votes={}",
                            pubkey, digest, votes
                        );
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
    gc_depth: View,

    /// Receives new certificates from the primary. The primary should send us new certificates only if it already sent us its whole history.
    rx_primary: Receiver<Certificate>,

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
        gc_depth: View,
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
                    let view = certificate.header.view;

                    // Add the new certificate to the local storage.
                    state.dag
                        .entry(view)
                        .or_insert_with(HashMap::new)
                        .insert(certificate.origin(), (certificate.hash(), certificate));
                }

                Some(certificate) = self.rx_commit_view.recv() => {
                    // commit the leader certificate
                    let view = certificate.header.view;
                    // let (_, leader_cert) = state.dag.get(&view)
                    //     .map(|x| x.get(&certificate.origin()))
                    //     .flatten()
                    //     .expect("The leader's certificate should be here")
                    //     .clone();

                    // Get an ordered list of past leaders that are linked to the current leader.
                    // let mut sequence = Vec::new();
                    // for leader in self.order_leaders(&leader_cert, &state).iter().rev() {
                    //     // Starting from the oldest leader, flatten the sub-dag referenced by the leader.
                    //     for x in self.order_dag(&leader_cert, &state) {
                    //         // Update and clean up internal state.
                    //         state.update(&x, self.gc_depth);
                    //
                    //         // Add the certificate to the sequence.
                    //         sequence.push(x);
                    //     }
                    // }

                    // Output the sequence in the right order.
                    // for certificate in sequence {
                    //     #[cfg(not(feature = "benchmark"))]
                    //     error!("Committed certificate {}", certificate.header);
                    //
                    //     #[cfg(feature = "benchmark")]
                    //     for digest in certificate.header.payload.keys() {
                    //         // NOTE: This log entry is used to compute performance.
                    //         info!("Committed {} -> {:?}", certificate.header, digest);
                    //     }
                    //
                    //     self.tx_primary
                    //         .send(certificate.clone())
                    //         .await
                    //         .expect("Failed to send certificate to primary");
                    //
                    //     if let Err(e) = self.tx_output.send(certificate).await {
                    //         warn!("Failed to output certificate: {}", e);
                    //     }
                    // }
                }
            }
        }
    }

    fn leader_certificate<'a>(&self, view: View, dag: &'a Dag) -> Option<&'a (Hash, Certificate)> {
        let (leader_key, _) = self.committee.leader(view as usize);
        // Return its certificate and the certificate's digest.
        dag.get(&view).map(|x| x.get(&leader_key)).flatten()
    }

    /// Order the past leaders that we didn't already commit.
    fn order_leaders(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        let mut to_commit = vec![leader.clone()];
        let mut leader = leader;

        for v in(state.last_committed_view .. leader.view())
            .rev()
        {
            // Get the certificate proposed by the previous leader.
            let (_, prev_leader) = match self.leader_certificate(v, &state.dag) {
                Some(x) => x,
                None => continue,
            };

            // Check whether there is a path between the last two leaders.
            if self.linked(leader, prev_leader, &state.dag) {
                to_commit.push(prev_leader.clone());
                leader = prev_leader;
            }
        }

        to_commit
    }

    /// Checks if there is a path between two leaders.
    fn linked(&self, leader: &Certificate, prev_leader: &Certificate, dag: &Dag) -> bool {
        let mut parents = vec![leader];
        for v in (prev_leader.view()..leader.view()).rev() {
            parents = dag
                .get(&(v))
                .expect("We should have the whole history by now")
                .values()
                .filter(|(digest, _)| parents.iter().any(|x| x.header.parents.contains(digest)))
                .map(|(_, certificate)| certificate)
                .collect();
        }
        parents.contains(&prev_leader)
    }

    /// Flatten the dag referenced by the input certificate. This is a classic depth-first search (pre-order):
    /// https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
    fn order_dag(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        debug!("Processing sub-dag of {:?}", leader);
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();

        let mut buffer = vec![leader];
        while let Some(x) = buffer.pop() {
            debug!("Sequencing {:?}", x);
            ordered.push(x.clone());
            for parent in &x.header.parents {
                let (digest, certificate) = match state
                    .dag
                    .get(&(x.view() - 1))
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
                    .map_or_else(|| false, |r| r == &certificate.view());
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                }
            }
        }

        // Ensure we do not commit garbage collected certificates.
        ordered.retain(|x| x.view() + self.gc_depth >= state.last_committed_view);

        // Ordering the output by round is not really necessary but it makes the commit sequence prettier.
        ordered.sort_by_key(|x| x.view());
        ordered
    }
}

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;
