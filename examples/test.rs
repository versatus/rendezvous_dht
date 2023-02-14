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
use serde_derive::{Deserialize, Serialize};

const SERVER: &str = "127.0.0.1:9090";

fn client() -> Result<(), ErrorKind> {
    let addr = "127.0.0.1:6060";
    // let mut socket = Socket::bind(addr)?;

    let mut socket = Socket::bind_with_config(
        addr,
        Config {
            blocking_mode: false,
            idle_connection_timeout: Duration::from_secs(5),
            heartbeat_interval: None,
            max_packet_size: (16 * 1024) as usize,
            max_fragments: 16 as u8,
            fragment_size: 1024,
            fragment_reassembly_buffer_size: 64,
            receive_buffer_max_size: 1452 as usize,
            rtt_smoothing_factor: 0.10,
            rtt_max_value: 250,
            socket_event_buffer_size: 1024,
            socket_polling_timeout: Some(Duration::from_millis(1000)),
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
    let mut i = 0;
    let quorum_key=hex::decode("aa4a1fc44e1fc58e17c82c3b5851747e3472405d4b95114e0f2bb943f5e48f3730731a0632a5b31f88038d3e2e657419").unwrap();

    let _ = sender.send(Packet::reliable_ordered(
        server,
        bincode::serialize(&Data::Request(RendezvousRequest::Namespace(
            "FARMER".to_string().as_bytes().to_vec(),
            quorum_key.clone(),
        )))
        .unwrap(),
        None,
    ));

    let _ = sender.send(Packet::reliable_ordered(
        server,
        bincode::serialize(&Data::Request(RendezvousRequest::RegisterPeer(
            quorum_key.clone(),
            "FARMER".to_string().as_bytes().to_vec(),
            hex::decode("a8f8d3a389cd62397112f9fdd20e84fa122ac0d1e69d2ebd1cc3325ba3ac34fa2f26db75112128ab2d0a48ba81d63645").unwrap(),
            hex::decode("b7be0d2cbc712860e983ab1d80fe08fa122fd86111de8277a350c336065de658cc17a6d69511ee9a31c09aad3ac10f64123706abe84f00fc850872787dd7412851f83de0fea915548434f669613f92d3af44ca21547a8edcd2731c8ab39668d5").unwrap(),
            "HelloVrrb".to_string().as_bytes().to_vec(),
            PeerData {
                address: "127.0.0.1:6060".to_string(),
                raptor_udp_port: 8082,
                quic_port: 8083,
                node_type: NodeType::Farmer,
            },
        )))
        .unwrap(),None
    ));

    let _ = sender.send(Packet::reliable_ordered(
        server,
        bincode::serialize(&Data::Request(RendezvousRequest::RegisterPeer(
            quorum_key.clone(),
            "FARMER".to_string().as_bytes().to_vec(),
            hex::decode("a8f8d3a389cd62397112f9fdd20e84fa122ac0d1e69d2ebd1cc3325ba3ac34fa2f26db75112128ab2d0a48ba81d63645").unwrap(),
            hex::decode("b7be0d2cbc712860e983ab1d80fe08fa122fd86111de8277a350c336065de658cc17a6d69511ee9a31c09aad3ac10f64123706abe84f00fc850872787dd7412851f83de0fea915548434f669613f92d3af44ca21547a8edcd2731c8ab39668d5").unwrap(),
            "HelloVrrb".to_string().as_bytes().to_vec(),
            PeerData {
                address: "127.0.0.1:6062".to_string(),
                raptor_udp_port: 8082,
                quic_port: 8083,
                node_type: NodeType::Farmer,
            },
        )))
            .unwrap(),None
    ));
 thread::sleep(Duration::from_secs(4));
    loop {
        if let Ok(event) = receiver.recv() {
            match event {
                SocketEvent::Packet(packet) => {
                    if packet.addr() == server {
                        let payload_response: Data =
                            bincode::deserialize(packet.payload()).unwrap();
                        match payload_response {
                            Request(req) => match req {
                                RendezvousRequest::Ping => {
                                    let response = &Data::Response(RendezvousResponse::Pong);
                                    sender.send(Packet::reliable_unordered(
                                        packet.addr(),
                                        bincode::serialize(&response).unwrap(),
                                    ));
                                    if i == 2 {
                                        println!("Requesting new peers");
                                        let peers = Data::Request(RendezvousRequest::Peers(
                                            quorum_key.clone(),
                                        ));
                                        let req_bytes = bincode::serialize(&peers).unwrap();
                                        let _ = sender.send(Packet::reliable_ordered(
                                            server, req_bytes, None,
                                        ));
                                        i = 0;
                                    };
                                    i = i + 1
                                }
                                _ => {}
                            },
                            Response(res) => match res {
                                RendezvousResponse::Peers(peers) => {
                                    for peer in peers.iter() {
                                        println!("Peers in Quorum are {:?}", peer);
                                    }
                                }
                                RendezvousResponse::NamespaceRegistered => {
                                    println!("Namespace Registered");
                                }
                                RendezvousResponse::PeerRegistered => {
                                    println!("Peer Registered");
                                }

                                _ => {}
                            },
                            _ => {}
                        }
                    } else {
                        println!("Unknown sender.");
                    }
                }
                SocketEvent::Timeout(_) => {}
                _ =>
                    //println!("Silence.."),
                    {}
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
    Peers(Vec<u8>),
    Namespace(Vec<u8>, Vec<u8>),
    RegisterPeer(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, PeerData),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RendezvousResponse {
    Pong,
    RequestPeers(Vec<u8>),
    Peers(Vec<PeerData>),
    PeerRegistered,
    NamespaceRegistered,
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
