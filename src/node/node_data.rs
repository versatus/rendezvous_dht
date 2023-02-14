use crate::key::Key;
use serde_derive::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter, Result};

/// A struct that contains the address and id of a node.
#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct NodeData {
    /// The ip of the node in the form of `ip`.
    pub ip: String,
    /// The  port opened for communication .
    pub port: String,
    /// The address of the node in the form of `ip:port`.
    pub addr: String,
    /// The id of the node.
    pub id: Key,
}

impl NodeData {
    /// `new` is a function that takes in a `String`, a `String`, a `String`, and a `Key` and returns a
    /// `NodeData`
    ///
    /// Arguments:
    ///
    /// * `ip`: The IP address of the node.
    /// * `port`: The port that the node is listening on.
    /// * `addr`: The address of the node.
    /// * `id`: The id of the node.
    ///
    /// Returns:
    ///
    /// A new instance of the NodeData struct.
    pub fn new(ip: String, port: String, addr: String, id: Key) -> Self {
        NodeData { ip, port, addr, id }
    }
}
impl Debug for NodeData {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "{:?},{},{} - {:?}",
            self.ip, self.port, self.addr, self.id
        )
    }
}

/// A struct that contains a `NodeData` and a distance.
#[derive(Eq, Clone, Debug)]
pub struct NodeDataDistancePair(pub NodeData, pub Key);

impl PartialEq for NodeDataDistancePair {
    fn eq(&self, other: &NodeDataDistancePair) -> bool {
        self.0.eq(&other.0)
    }
}

impl PartialOrd for NodeDataDistancePair {
    fn partial_cmp(&self, other: &NodeDataDistancePair) -> Option<Ordering> {
        Some(other.1.cmp(&self.1))
    }
}

impl Ord for NodeDataDistancePair {
    fn cmp(&self, other: &NodeDataDistancePair) -> Ordering {
        other.1.cmp(&self.1)
    }
}
