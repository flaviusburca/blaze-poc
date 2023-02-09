use std::cmp::max;
use std::collections::HashMap;
use std::sync::Arc;
use itertools::Itertools;
use log::debug;
use mundis_model::certificate::Certificate;
use mundis_model::hash::{Hash, Hashable};
use mundis_model::pubkey::Pubkey;
use mundis_model::{Round, View};

/// The representation of the DAG in memory.
type Dag = HashMap<View, HashMap<Pubkey, (Hash, Certificate)>>;

/// The state that needs to be persisted for crash-recovery.
pub struct State {
    /// The last committed view.
    pub last_committed_view: View,
    // Keeps the last committed round for each authority. This map is used to clean up the dag and
    // ensure we don't commit twice the same certificate.
    pub last_committed: HashMap<Pubkey, View>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    pub dag: Dag,
    /// Keeps a mapping between views and rounds
    pub rounds: HashMap<Round, View>,
}

impl State {
    pub fn new(genesis: Vec<Certificate>, gc_depth: Round) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.hash(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_view: 1,
            last_committed: genesis.iter().map(|(x, (_, y))| (*x, y.view())).collect(),
            dag: [(1, genesis)].iter().cloned().collect(),
            rounds: HashMap::with_capacity(2 * gc_depth as usize)
        }
    }

    pub fn add_certificate(&mut self, certificate: Certificate) {
        self.dag
            .entry(certificate.view())
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.hash(), certificate));
    }

    /// Update and clean up internal state base on committed certificates.
    pub fn update(&mut self, certificate: &Certificate, gc_depth: Round) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.view()))
            .or_insert_with(|| certificate.view());

        let last_committed_view = *self.last_committed.values().max().unwrap();
        self.last_committed_view = last_committed_view;

        // TODO: This cleanup is dangerous: we need to ensure consensus can receive idempotent replies
        // from the primary. Here we risk cleaning up a certificate and receiving it again later.
        for (name, view) in &self.last_committed {
            self.dag.retain(|v, authorities| {
                let max_round = Self::max_round_for_view(*v, &self.rounds);
                let last_committed_round = Self::max_round_for_view(self.last_committed_view, &self.rounds);
                authorities.retain(|n, _| n != name || v >= view);
                !authorities.is_empty() && max_round + gc_depth >= last_committed_round
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

    pub fn max_round_for_view(view: View, rounds: &HashMap<Round, View>) -> Round {
        if view == 1 {
            return 1
        }

        match rounds.iter()
            .filter(|&(k, v)| *v == view)
            .map(|(k, v)| k)
            .max()
            .cloned() {
            Some(r) => r,
            None => {
                rounds.iter()
                    .filter(|&(k, v)| *v == view - 1)
                    .map(|(k, v)| k)
                    .max()
                    .cloned()
                    .unwrap()
            }
        }


    }

    pub fn last_committed_round(&self) -> Round {
        Self::max_round_for_view(self.last_committed_view, &self.rounds)
    }
}
