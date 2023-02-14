use crate::key::Key;
use crate::node::node_data::NodeData;
use crate::protocol::NodeType::{FARMER, HARVESTER};
use crate::MESSAGE_LENGTH;
use log::warn;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::UdpSocket;
use std::str::FromStr;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;
use std::{fmt, str};

/// An enum representing a request RPC.
///
/// Each request RPC also carries a randomly generated key. The response to the RPC must contain
/// the same randomly generated key or else it will be ignored.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Request {
    pub id: Key,
    pub sender: NodeData,
    pub payload: RequestPayload,
}
pub type FailedPingCount = u8;
pub type NameSpaceType = Vec<u8>;
pub type QuorumPublicKey = Vec<u8>;
pub type PKShare = Vec<u8>;
pub type SigShare = Vec<u8>;
pub type Address = String;
pub type Payload = Vec<u8>;

/// An enum representing the payload to a request RPC.
///
/// As stated in the Kademlia paper, the four possible RPCs are `PING`, `STORE`, `FIND_NODE`, and
/// `FIND_VALUE`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RequestPayload {
    Ping,
    FindNode(Key),
    FindPeers(Key),
    FindNamespaces(Key),
    RegisterPeer(Key, QuorumPublicKey, Address, u16, u16, String),
    RegisterNameSpace(Key, QuorumPublicKey),
}

/// An enum representing the response to a request RPC.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub request: Request,
    pub receiver: NodeData,
    pub payload: ResponsePayload,
}

/// An enum representing the payload to a response RPC.
///
/// As stated in the Kademlia paper, a response to a request could be a list of nodes, a value, or
/// a pong.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ResponsePayload {
    Nodes(Vec<NodeData>),
    Value(HashSet<PeerData>),
    Namespaces(HashSet<String>),
    Pong,
    RegistrationSuccessful,
    RegistrationFailed,
    NameSpaceUnRegistered,
    NodeUnRegistered,
    Failed(String),
}

/// An enum that represents a message that is sent between nodes.
#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Request(Request),
    Response(Response),
    Kill,
}

/// `Protocol` facilitates the underlying communication between nodes by sending messages to other
/// nodes, and by passing messages from other nodes to the current node.
#[derive(Clone)]
pub struct Protocol {
    socket: Arc<UdpSocket>,
}

impl Protocol {
    pub fn new(socket: UdpSocket, tx: Sender<Message>) -> Protocol {
        let protocol = Protocol {
            socket: Arc::new(socket),
        };
        let ret = protocol.clone();
        thread::spawn(move || {
            let mut buffer = [0u8; MESSAGE_LENGTH];
            loop {
                let (len, _src_addr) = protocol.socket.recv_from(&mut buffer).unwrap();
                let message = bincode::deserialize(&buffer[..len]).unwrap();

                if tx.send(message).is_err() {
                    warn!("Protocol: Connection closed.");
                    break;
                }
            }
        });
        ret
    }

    pub fn send_message(&self, message: &Message, node_data: &NodeData) {
        let buffer_string = bincode::serialize(&message).unwrap();
        let NodeData { ref addr, .. } = node_data;
        if self.socket.send_to(&buffer_string, addr).is_err() {
            warn!("Protocol: Could not send data.");
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Data {
    Request(RendezvousRequest),
    Response(RendezvousResponse),
}

/// A enum that is used to send messages between nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RendezvousRequest {
    Ping,
    Peers(Vec<u8>),
    Namespace(Vec<u8>, Vec<u8>),
    RegisterPeer(
        QuorumPublicKey,
        NameSpaceType,
        PKShare,
        SigShare,
        Payload,
        PeerData,
    ),
}

/// A enum that is used to send messages between nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RendezvousResponse {
    Pong,
    RequestPeers(Vec<u8>),
    Peers(Vec<PeerData>),
    PeerRegistered,
    NamespaceRegistered,
}

/// `PeerData` is a struct that contains a `String` (`address`), two `u16`s (`raptor_udp_port` and
/// `quic_port`), and a `NodeType` (`node_type`).
///
/// The `#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]` line is a Rust annotation
/// that tells the Rust compiler to automatically generate code for the `Clone`, `Debug`, `Serialize`,
/// `Deserialize`,
///
/// Properties:
///
/// * `address`: The IP address of the peer.
/// * `raptor_udp_port`: The port that the Raptor UDP protocol is running on.
/// * `quic_port`: The port that the peer is listening on for QUIC connections.
/// * `node_type`: This is the type of node that the peer is. It can be either a farmer,harvester etc
/// node.
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
            address: "127.0.0.1:8080".parse().unwrap(),
            raptor_udp_port: 0,
            quic_port: 0,
            node_type: NodeType::HARVESTER,
        }
    }
}

/// This is an enum that is used to specify the type of node that the peer is. It can be either a
/// farmer,harvester etc node.
#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum NodeType {
    HARVESTER,
    FARMER,
    Unknown,
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
            "FARMER" => Ok(FARMER),
            "HARVESTER" => Ok(HARVESTER),
            _ => Err(()),
        }
    }
}
