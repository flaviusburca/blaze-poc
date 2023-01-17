// Copyright(C) Facebook, Inc. and its affiliates.
use bytes::Bytes;
use {
    futures::{sink::SinkExt as _, stream::StreamExt as _},
    mundis_model::{
        certificate::{Certificate, Header},
        committee::{Authority, Committee, ExecutorAddresses, PrimaryAddresses, WorkerAddresses},
        hash::Hashable,
        keypair::Keypair,
        signature::{Signature, Signer},
        vote::Vote,
    },
    rand::{rngs::StdRng, SeedableRng as _},
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

pub fn header() -> Header {
    let keypair = keys().pop().unwrap();
    let header = Header {
        author: keypair.pubkey(),
        round: 1,
        epoch: 0,
        parents: Certificate::genesis(&committee())
            .iter()
            .map(|x| x.hash())
            .collect(),
        ..Header::default()
    };

    let id = header.hash();
    let signature = keypair.sign_message(id.as_ref());

    Header {
        id,
        signature,
        ..header
    }
}

pub fn headers() -> Vec<Header> {
    keys()
        .into_iter()
        .map(|keypair| {
            let header = Header {
                author: keypair.pubkey(),
                round: 1,
                parents: Certificate::genesis(&committee())
                    .iter()
                    .map(|x| x.hash())
                    .collect(),
                ..Header::default()
            };

            let digest = header.hash();

            Header {
                id: digest,
                signature: keypair.sign_message(digest.as_ref()),
                ..header
            }
        })
        .collect()
}

pub fn votes(header: &Header) -> Vec<Vote> {
    keys()
        .into_iter()
        .map(|keypair| {
            let vote = Vote {
                id: header.id.clone(),
                round: header.round,
                origin: header.author,
                author: keypair.pubkey(),
                signature: Signature::default(),
            };
            let signature = keypair.sign_message(vote.hash().as_ref());
            Vote { signature, ..vote }
        })
        .collect()
}

pub fn certificate(header: &Header) -> Certificate {
    Certificate {
        header: header.clone(),
        votes: votes(&header)
            .into_iter()
            .map(|x| (x.author, x.signature))
            .collect(),
    }
}

pub fn listener(address: SocketAddr) -> JoinHandle<Bytes> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let transport = Framed::new(socket, LengthDelimitedCodec::new());
        let (mut writer, mut reader) = transport.split();
        match reader.next().await {
            Some(Ok(received)) => {
                writer.send(Bytes::from("Ack")).await.unwrap();
                received.freeze()
            }
            _ => panic!("Failed to receive network message"),
        }
    })
}
