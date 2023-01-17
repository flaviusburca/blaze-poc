use {
    crate::worker::{
        batch_maker::{Batch, Transaction},
        WorkerMessage,
    },
    bytes::Bytes,
    futures::{SinkExt, StreamExt},
    mundis_model::{
        committee::{Authority, Committee, ExecutorAddresses, PrimaryAddresses, WorkerAddresses},
        hash::{Hash, Hasher},
        keypair::Keypair,
        signature::Signer,
    },
    rand::{rngs::StdRng, SeedableRng},
    std::net::SocketAddr,
    tokio::{net::TcpListener, task::JoinHandle},
    tokio_util::codec::{Framed, LengthDelimitedCodec},
};

pub fn keys() -> Vec<Keypair> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| Keypair::generate(&mut rng)).collect()
}

pub fn committee() -> Committee {
    Committee {
        epoch: 0,
        authorities: keys()
            .iter()
            .enumerate()
            .map(|(i, keypair)| {
                let primary = PrimaryAddresses {
                    primary_to_primary: format!("127.0.0.1:{}", 100 + i).parse().unwrap(),
                    worker_to_primary: format!("127.0.0.1:{}", 200 + i).parse().unwrap(),
                };
                let workers = vec![(
                    0,
                    WorkerAddresses {
                        primary_to_worker: format!("127.0.0.1:{}", 300 + i).parse().unwrap(),
                        transactions: format!("127.0.0.1:{}", 400 + i).parse().unwrap(),
                        worker_to_worker: format!("127.0.0.1:{}", 500 + i).parse().unwrap(),
                    },
                )]
                .iter()
                .cloned()
                .collect();
                let executor = ExecutorAddresses {
                    worker_to_executor: format!("127.0.0.1:{}", 600 + i).parse().unwrap(),
                };

                (
                    keypair.pubkey(),
                    Authority {
                        stake: 1,
                        primary,
                        workers,
                        executor,
                    },
                )
            })
            .collect(),
    }
}

pub fn committee_with_base_port(base_port: u16) -> Committee {
    let mut committee = committee();
    for authority in committee.authorities.values_mut() {
        let primary = &mut authority.primary;

        let port = primary.primary_to_primary.port();
        primary.primary_to_primary.set_port(base_port + port);

        let port = primary.worker_to_primary.port();
        primary.worker_to_primary.set_port(base_port + port);

        for worker in authority.workers.values_mut() {
            let port = worker.primary_to_worker.port();
            worker.primary_to_worker.set_port(base_port + port);

            let port = worker.transactions.port();
            worker.transactions.set_port(base_port + port);

            let port = worker.worker_to_worker.port();
            worker.worker_to_worker.set_port(base_port + port);
        }
    }
    committee
}

pub fn transaction() -> Transaction {
    vec![0; 100]
}

pub fn batch() -> Batch {
    vec![transaction(), transaction()]
}

pub fn serialized_batch() -> Vec<u8> {
    let message = WorkerMessage::Batch(batch());
    bincode::serialize(&message).unwrap()
}

pub fn batch_digest() -> Hash {
    let mut hasher = Hasher::default();
    hasher.hash(&serialized_batch().as_slice()[..32]);
    hasher.result()
}

pub fn listener(address: SocketAddr, expected: Option<Bytes>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let transport = Framed::new(socket, LengthDelimitedCodec::new());
        let (mut writer, mut reader) = transport.split();
        match reader.next().await {
            Some(Ok(received)) => {
                writer.send(Bytes::from("Ack")).await.unwrap();
                if let Some(expected) = expected {
                    assert_eq!(received.freeze(), expected);
                }
            }
            _ => panic!("Failed to receive network message"),
        }
    })
}
