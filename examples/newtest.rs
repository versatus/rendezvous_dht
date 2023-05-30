//! Note that the terms "client" and "server" here are purely what we logically associate with them.
//! Technically, they both work the same.
//! Note that in practice you don't want to implement a chat client using UDP.
use laminar::{Config, ErrorKind, Packet, Socket, SocketEvent};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Instant;
use std::{fmt, thread};
use std::{io::stdin, time::Duration};

use crate::Data::Request;
use crate::Data::Response;
use bincode::{deserialize, serialize};
use cuckoofilter::ExportedCuckooFilter;
use serde_derive::{Deserialize, Serialize};

const SERVER: &str = "127.0.0.1:9090";

fn client() -> Result<(), ErrorKind> {
    let addr = "127.0.0.1:6060";
    // let mut socket = Socket::bind(addr)?;

    let mut socket = Socket::bind_with_config(
        addr,
        Config {
            blocking_mode: false,
            idle_connection_timeout: Duration::from_secs(10),
            heartbeat_interval: None,
            max_packet_size: (16 * 1024) as usize,
            max_fragments: 16 as u8,
            fragment_size: 1024,
            fragment_reassembly_buffer_size: 64,
            receive_buffer_max_size: 1452 as usize,
            rtt_smoothing_factor: 0.10,
            rtt_max_value: 250,
            socket_event_buffer_size: 1024,
            socket_polling_timeout: Some(Duration::from_millis(10000)),
            max_packets_in_flight: 512,
            max_unestablished_connections: 50,
        },
    )
        .unwrap();

    println!("Connected on {}", addr);

    let server = SERVER.parse().unwrap();
    let (sender, receiver) = (socket.get_packet_sender(), socket.get_event_receiver());

    thread::spawn(move || {
        socket.start_polling();
    });

    let payload=Request(RendezvousRequest::FetchNameSpace(
        "FARMER".to_string().as_bytes().to_vec(),
    ));


    let s = sender.send(Packet::reliable_unordered(
        server,
        serialize(&payload)
            .unwrap(),
    ));
    println!("Status :{:?}",s);


    loop {
        if let Ok(event) = receiver.recv() {
            println!("Received event :{:?}", event);
            match event {
                SocketEvent::Packet(packet) => {
                    if packet.addr() == server {
                        let payload_response: Data =
                            bincode::deserialize(packet.payload()).unwrap();
                        match payload_response {
                            Request(req) => match req {
                                RendezvousRequest::Ping => {
                                    let response = &Data::Response(RendezvousResponse::Pong);
                                    let _ = sender.send(Packet::reliable_unordered(
                                        packet.addr(),
                                        bincode::serialize(&response).unwrap(),
                                    ));
                                }
                                _ => {}
                            },
                            Response(res) => match res {
                                RendezvousResponse::Peers(quorum_key, peers, filter) => {
                                    println!("quorum_key :{:?}", hex::encode(quorum_key));
                                    for peer in peers.iter() {
                                        println!("Peers in Quorum are {:?}", peer);
                                    }
                                }
                                RendezvousResponse::NamespaceRegistered => {
                                    println!("Namespace Registered");
                                }
                                RendezvousResponse::PeerRegistered => {
                                    println!("Peer Registered");
                                },
                                RendezvousResponse::Namespaces(namespaces) => {
                                    println!("Namespaces:{:?}", namespaces);
                                }

                                _ => {}
                            },
                            _ => {}
                        }
                    } else {
                        println!("Unknown sender.");
                    }
                }
                _ =>
                    {
                        println!("Received unknown.");
                    }
            }
        }
    }
    Ok(())
}

fn main() -> Result<(), ErrorKind> {
    client()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Data {
    Request(RendezvousRequest),
    Response(RendezvousResponse),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RendezvousRequest {
    Ping,
    Peers(Vec<u8>, Vec<u8>),
    Namespace(Vec<u8>, Vec<u8>),
    RegisterPeer(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, PeerData),
    FetchNameSpace(Vec<u8>)
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RendezvousResponse {
    Pong,
    RequestPeers(Vec<u8>),
    Peers(Vec<u8>,Vec<PeerData>, Vec<u8>),
    PeerRegistered,
    NamespaceRegistered,
    Namespaces(Vec<Vec<u8>>)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct PeerData {
    pub address: String,
    pub raptor_udp_port: u16,
    pub quic_port: u16,
    pub node_type: NodeType,
}

impl Default for PeerData {
    fn default() -> Self {
        PeerData {
            address: "127.0.0.1:8080".to_string(),
            raptor_udp_port: 0,
            quic_port: 0,
            node_type: NodeType::Validator,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum NodeType {
    /// A Node that can archive, validate and mine tokens
    Full = 0,
    /// Same as `NodeType::Full` but without archiving capabilities
    Light = 1,
    /// Archives all transactions processed in the blockchain
    Archive = 2,
    /// Mining node
    Miner = 3,
    Bootstrap = 4,
    Validator = 5,
    MasterNode = 6,
    RPCNode = 7,
    Farmer = 8,
    Unknown = 100,
}

impl fmt::Display for NodeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FromStr for NodeType {
    type Err = ();

    fn from_str(input: &str) -> Result<NodeType, Self::Err> {
        match input {
            "Farmer" => Ok(NodeType::Farmer),
            "Harvester" => Ok(NodeType::Validator),
            "Validator" => Ok(NodeType::Validator),
            _ => Err(()),
        }
    }
}
