pub mod node_data;

use crate::key::Key;
use crate::node::node_data::{NodeData, NodeDataDistancePair};
use crate::protocol::{
    Address, Data, FailedPingCount, Message, NodeType, PeerData, Protocol, QuorumPublicKey,
    RendezvousRequest, RendezvousResponse, Request, RequestPayload, Response, ResponsePayload,
};
use crate::routing::{RoutingBucket, RoutingTable};
use crate::storage::Storage;
use crate::{
    BUCKET_REFRESH_INTERVAL, CONCURRENCY_PARAM, KEY_LENGTH, NAME_SPACES, NUMBER_PEER_DATA_TO_SEND,
    PING_TIME_INTERVAL, REPLICATION_PARAM, REQUEST_TIMEOUT, UNREACHABLE_THRESDHOLD,
};
use hbbft::crypto::{PublicKeyShare, SignatureShare};
use laminar::{Config, Packet, Socket, SocketEvent};
use log::{debug, info, warn};
use sha3::{Digest, Sha3_256};
use std::cmp;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::net::{SocketAddr, UdpSocket};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};

use cuckoofilter::{CuckooFilter, ExportedCuckooFilter};
use rand::seq::SliceRandom;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use thiserror::Error;

pub type ExportedFilter = Vec<u8>;
/// A node in the Kademlia DHT.
#[derive(Clone)]
pub struct Node {
    node_data: Arc<NodeData>,
    /// Creating a new instance of the RoutingTable struct and storing it in a mutex.
    pub routing_table: Arc<Mutex<RoutingTable>>,
    storage: Arc<Mutex<Storage>>,
    pending_requests: Arc<Mutex<HashMap<Key, Sender<Response>>>>,
    protocol: Arc<Protocol>,
    is_active: Arc<AtomicBool>,
    rendezvous_address: SocketAddr,
}

#[derive(Error, Debug)]
pub enum RendezvousError {
    #[error("NameSpace Registration Failed ,Reason {0}")]
    NSRegistrationFailed(String),

    #[error("Peer Registration Failed ,Reason {0}")]
    PeerRegistrationFailed(String),

    #[error("Unknown Error Details : {0}")]
    Unknown(String),
}

impl Node {
    /// Constructs a new `Node` on a specific ip and port, and bootstraps the node with an existing
    /// node if `bootstrap` is not `None`.
    pub fn new(
        ip: &str,
        port: &str,
        bootstrap: Option<NodeData>,
        rendezvous_address: SocketAddr,
    ) -> Self {
        let addr = format!("{}:{}", ip, port);
        let socket = UdpSocket::bind(addr).expect("Error: could not bind to address.");
        let node_data = Arc::new(NodeData {
            ip: ip.to_string(),
            port: port.to_string(),
            addr: socket.local_addr().unwrap().to_string(),
            id: Key::rand(),
        });
        let mut routing_table = RoutingTable::new(Arc::clone(&node_data));
        let (message_tx, message_rx) = channel();
        let protocol = Protocol::new(socket, message_tx);

        // directly use update_node as update_routing_table is async
        if let Some(bootstrap_data) = bootstrap {
            routing_table.update_node(bootstrap_data);
        }

        let mut ret = Node {
            node_data,
            routing_table: Arc::new(Mutex::new(routing_table)),
            storage: Arc::new(Mutex::new(Storage::new())),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            protocol: Arc::new(protocol),
            is_active: Arc::new(AtomicBool::new(true)),
            rendezvous_address,
        };
        let (ping_status_sender, ping_status_receiver) = channel::<(SocketAddr, bool)>();
        // let mut socket = Socket::bind(rendezvous_address).unwrap();

        let mut socket = Socket::bind_with_config(
            rendezvous_address,
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
                socket_polling_timeout: Some(Duration::from_millis(3000)),
                max_packets_in_flight: 512,
                max_unestablished_connections: 50,
            },
        )
        .unwrap();
        let (sender, receiver) = (socket.get_packet_sender(), socket.get_event_receiver());
        let _thread = thread::spawn(move || socket.start_polling());

        ret.start_message_handler(message_rx);
        ret.start_bucket_refresher();
        ret.bootstrap_routing_table();
        ret.check_nodes_liveness();
        ret.check_registered_peer_liveliness(ping_status_receiver, sender.clone());
        ret.process_received_request(ping_status_sender, sender, receiver);
        ret
    }

    /// It spawns a thread that periodically pings all the peers in the quorum and if a peer is
    /// unreachable for a certain number of times, it is removed from the quorum
    ///
    /// Arguments:
    ///
    /// * `ping_status_receiver`: Receiver<(SocketAddr, bool)>
    /// * `sender`: crossbeam_channel::Sender<Packet>
    fn check_registered_peer_liveliness(
        &self,
        ping_status_receiver: Receiver<(SocketAddr, bool)>,
        sender: crossbeam_channel::Sender<Packet>,
    ) {
        let node = self.clone();
        thread::spawn(move || loop {
            if let Ok(mut storage) = node.storage.lock() {
                let mut items = storage.quorum_peers.clone();
                let keys = NAME_SPACES
                    .iter()
                    .map(|x| Node::get_key(x.as_bytes()))
                    .collect::<Vec<Key>>();
                for (key, peer_data_set) in items.iter() {
                    if !keys.contains(key) {
                        for peer_data in peer_data_set.iter() {
                            if peer_data.quic_port == 0 {
                                continue;
                            }
                            info!("Pinging Address {:?}", peer_data.address);
                            if let Ok(server) = peer_data.address.parse::<SocketAddr>() {
                                let request = Data::Request(RendezvousRequest::Ping);
                                let req_bytes = bincode::serialize(&request).unwrap();
                                let _ =
                                    sender.send(Packet::reliable_ordered(server, req_bytes, None));
                            }
                        }
                    }

                    loop {
                        match ping_status_receiver.try_recv() {
                            Ok((peer, ping_status)) => {
                                let failed_ping: u8 = if ping_status { 0 } else { 1 };
                                info!("Unreachable peer: {:?}", peer);
                                let peer_address = Node::get_key(peer.to_string().as_bytes());
                                match storage.unreachable_peers.get_mut(&peer_address) {
                                    None => {
                                        storage
                                            .unreachable_peers
                                            .insert(peer_address, (key.clone(), peer, failed_ping));
                                        break;
                                    }
                                    Some(value) => {
                                        if ping_status {
                                            value.2 = 0;
                                        } else {
                                            value.2 += failed_ping;
                                        }
                                    }
                                }
                                let mut list: Vec<(Key, Key, String, FailedPingCount)> = vec![];
                                {
                                    for (unreachable_key, value) in storage.unreachable_peers.iter()
                                    {
                                        list.push((
                                            value.0,
                                            unreachable_key.clone(),
                                            value.1.to_string().clone(),
                                            value.2,
                                        ));
                                    }
                                }
                                for value in list {
                                    if value.3 >= UNREACHABLE_THRESDHOLD {
                                        if let Some(peers) = storage.quorum_peers.get_mut(&value.0)
                                        {
                                            let new_peers = peers
                                                .iter()
                                                .filter(|&x| x.address != value.2)
                                                .map(|x| x.clone())
                                                .collect::<HashSet<PeerData>>();
                                            storage.quorum_peers.insert(value.0, new_peers);
                                            storage.unreachable_peers.remove(&value.1);
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
                drop(storage);
                thread::sleep(Duration::from_secs(PING_TIME_INTERVAL));
            }
        });
    }

    /// It spawns a thread that listens for incoming packets and spawns a thread for each packet that it
    /// receives
    ///
    /// Arguments:
    ///
    /// * `ping_status_sender`: This is a channel that sends the status of the ping request to the main
    /// thread.
    /// * `sender`: crossbeam_channel::Sender<Packet>
    /// * `receiver`: This is the receiver that is used to receive packets from the socket.
    fn process_received_request(
        &mut self,
        ping_status_sender: Sender<(SocketAddr, bool)>,
        sender: crossbeam_channel::Sender<Packet>,
        receiver: crossbeam_channel::Receiver<SocketEvent>,
    ) {
        let mut new_node = self.clone();
        let (request_sender, request_receiver) = channel::<(SocketAddr, Data)>();
        thread::spawn(move || loop {
            loop {
                match request_receiver.try_recv() {
                    Ok(request) => {
                        info!("Received Request: {:?}", request);
                        let packet_sender = request.0.clone();
                        match request.1 {
                            Data::Request(rendezvous_request) => match rendezvous_request {
                                RendezvousRequest::Peers(quorum_key, already_recieved) => {
                                    if let Some((peers_data, filter)) =
                                        new_node.get(&Node::get_key(&quorum_key), already_recieved)
                                    {
                                        let new_peers: Vec<_> =
                                            peers_data.into_iter().map(|x| x.clone()).collect();
                                        if let Ok(data) = bincode::serialize(&Data::Response(
                                            RendezvousResponse::Peers(
                                                quorum_key, new_peers, filter,
                                            ),
                                        )) {
                                            let _ = sender.send(Packet::reliable_ordered(
                                                packet_sender,
                                                data,
                                                None,
                                            ));
                                        }
                                    }
                                }

                                RendezvousRequest::RegisterPeer(
                                    quorum_key,
                                    namespace_type,
                                    pk_share,
                                    sig_share,
                                    payload,
                                    peer_data,
                                ) => {
                                    let _ = new_node.register_peer(
                                        Node::get_key(&quorum_key),
                                        quorum_key,
                                        namespace_type,
                                        pk_share,
                                        sig_share,
                                        payload,
                                        peer_data.address,
                                        peer_data.raptor_udp_port,
                                        peer_data.quic_port,
                                        peer_data.node_type.to_string(),
                                    );
                                    if let Ok(new_data) = bincode::serialize(&Data::Response(
                                        RendezvousResponse::PeerRegistered,
                                    )) {
                                        let _ = sender.send(Packet::reliable_ordered(
                                            packet_sender,
                                            new_data,
                                            None,
                                        ));
                                    }
                                }
                                RendezvousRequest::Namespace(namespace_type, quorum_key) => {
                                    let _ = new_node.register_namespace(
                                        Node::get_key(&namespace_type),
                                        quorum_key,
                                    );
                                    if let Ok(new_data) = bincode::serialize(&Data::Response(
                                        RendezvousResponse::NamespaceRegistered,
                                    )) {
                                        let _ = sender.send(Packet::reliable_ordered(
                                            packet_sender,
                                            new_data,
                                            None,
                                        ));
                                    }
                                }
                                RendezvousRequest::FetchNameSpace(quorum_type) => {
                                    info!("Fetch Namespace {:?}", quorum_type);
                                    if let Some(namespaces) =
                                        new_node.get_namespaces(&Node::get_key(&quorum_type))
                                    {
                                        let namespaces: Vec<Vec<u8>> = namespaces
                                            .into_iter()
                                            .map(|s| s.into_bytes())
                                            .collect();
                                        if let Ok(new_data) = bincode::serialize(&Data::Response(
                                            RendezvousResponse::Namespaces(namespaces.clone()),
                                        )) {
                                            let _ = sender.send(Packet::reliable_ordered(
                                                packet_sender,
                                                new_data,
                                                None,
                                            ));
                                        }
                                    }
                                }
                                _ => {}
                            },
                            _ => {}
                        }
                    }
                    Err(e) => break,
                }
            }
        });
        thread::spawn(move || loop {
            if let Ok(event) = receiver.recv() {
                match event {
                    SocketEvent::Packet(packet) => {
                        let msg = packet.payload();
                        let msg: Data = bincode::deserialize(msg).unwrap();
                        let new_msg = msg.clone();
                        match msg {
                            Data::Response(response) => match response {
                                RendezvousResponse::Pong => {
                                    let _ = ping_status_sender.send((packet.addr(), true));
                                }
                                _ => {}
                            },
                            Data::Request(request) => match request {
                                RendezvousRequest::Peers(_peers, _filter) => {
                                    let _ = request_sender.send((packet.addr(), new_msg));
                                }
                                RendezvousRequest::RegisterPeer(
                                    _quorum_key,
                                    _namespace_type,
                                    _pk_share,
                                    _sig_share,
                                    _payload,
                                    _peer_data,
                                ) => {
                                    let _ = request_sender.send((packet.addr(), new_msg));
                                }
                                RendezvousRequest::Namespace(_namespace_type, _quorum_key) => {
                                    let _ = request_sender.send((packet.addr(), new_msg));
                                },
                                RendezvousRequest::FetchNameSpace(namespace) => {
                                    let _ = request_sender.send((packet.addr(), new_msg));
                                }
                                _ => {}
                            },
                        }
                    }
                    SocketEvent::Timeout(address) => {
                        info!("Client timed out: {}", address);
                        let _ = ping_status_sender.send((address, false));
                    }
                    _ => {}
                }
            }
        });
    }

    fn check_nodes_liveness(&self) {
        let mut node = self.clone();
        thread::spawn(move || loop {
            let routing_table = node.routing_table.lock().unwrap().clone();
            let buckets: Vec<RoutingBucket> = routing_table.buckets.clone();
            for bucket in buckets {
                let nodes = bucket.nodes;
                for request in nodes {
                    node.rpc_ping(&request);
                }
            }
            drop(routing_table);
            thread::sleep(Duration::from_secs(PING_TIME_INTERVAL));
        });
    }

    /// Starts a thread that listens to responses.
    fn start_message_handler(&self, rx: Receiver<Message>) {
        let mut node = self.clone();
        thread::spawn(move || {
            for request in rx.iter() {
                match request {
                    Message::Request(request) => node.handle_request(&request),
                    Message::Response(response) => node.handle_response(&response),
                    Message::Kill => {
                        node.is_active.store(false, Ordering::Release);
                        info!("{} - Killed message handler", node.node_data.addr);
                        break;
                    }
                }
            }
        });
    }

    /// Starts a thread that refreshes stale routing buckets.
    fn start_bucket_refresher(&self) {
        let mut node = self.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(BUCKET_REFRESH_INTERVAL));
            while node.is_active.load(Ordering::Acquire) {
                let stale_indexes = {
                    let routing_table = match node.routing_table.lock() {
                        Ok(routing_table) => routing_table,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    routing_table.get_stale_indexes()
                };

                for index in stale_indexes {
                    node.lookup_nodes(&Key::rand_in_range(index), true, false);
                }
                thread::sleep(Duration::from_secs(BUCKET_REFRESH_INTERVAL));
            }
            warn!("{} - Killed bucket refresher", node.node_data.addr);
        });
    }

    /// Bootstraps the routing table using an existing node. The node first looks up its id to
    /// identify the closest nodes to it. Then it refreshes all routing buckets by looking up a
    /// random key in the buckets' range.
    fn bootstrap_routing_table(&mut self) {
        let target_key = self.node_data.id;
        self.lookup_nodes(&target_key, true, false);

        let bucket_size = { self.routing_table.lock().unwrap().size() };

        for i in 0..bucket_size {
            self.lookup_nodes(&Key::rand_in_range(i), true, false);
        }
    }

    /// Upserts the routing table. If the node cannot be inserted into the routing table, it
    /// removes and pings the least recently seen node. If the least recently seen node responds,
    /// it will be readded into the routing table, and the current node will be ignored.
    fn update_routing_table(&mut self, node_data: NodeData) {
        debug!("{} updating {}", self.node_data.addr, node_data.addr);
        let mut node = self.clone();
        thread::spawn(move || {
            let lrs_node_opt = {
                let mut routing_table = match node.routing_table.lock() {
                    Ok(routing_table) => routing_table,
                    Err(poisoned) => poisoned.into_inner(),
                };
                if !routing_table.update_node(node_data.clone()) {
                    routing_table.remove_lrs(&node_data.id)
                } else {
                    None
                }
            };

            // Ping the lrs node and move to front of bucket if active
            if let Some(lrs_node) = lrs_node_opt {
                node.rpc_ping(&lrs_node);
                let mut routing_table = match node.routing_table.lock() {
                    Ok(routing_table) => routing_table,
                    Err(poisoned) => poisoned.into_inner(),
                };
                routing_table.update_node(node_data);
            }
        });
    }

    /// Handles a request RPC.
    fn handle_request(&mut self, request: &Request) {
        info!(
            "{} - Receiving request from {} {:#?}",
            self.node_data.addr, request.sender.addr, request.payload,
        );
        self.clone().update_routing_table(request.sender.clone());
        let receiver = (*self.node_data).clone();
        let payload = match request.payload.clone() {
            RequestPayload::Ping => ResponsePayload::Pong,
            RequestPayload::FindNode(key) => ResponsePayload::Nodes(
                self.routing_table
                    .lock()
                    .unwrap()
                    .get_closest_nodes(&key, REPLICATION_PARAM),
            ),
            RequestPayload::FindPeers(key) => {
                if let Some(value) = self.storage.lock().unwrap().get_peers(&key) {
                    ResponsePayload::Value(value.clone())
                } else {
                    ResponsePayload::Nodes(
                        self.routing_table
                            .lock()
                            .unwrap()
                            .get_closest_nodes(&key, REPLICATION_PARAM),
                    )
                }
            }
            RequestPayload::FindNamespaces(key) => {
                if let Some(value) = self.storage.lock().unwrap().get_namespaces(&key) {
                    ResponsePayload::Namespaces(value.clone())
                } else {
                    ResponsePayload::Nodes(
                        self.routing_table
                            .lock()
                            .unwrap()
                            .get_closest_nodes(&key, REPLICATION_PARAM),
                    )
                }
            }
            RequestPayload::RegisterPeer(
                quorum_public_key,
                quorum_key,
                address,
                raptor_port,
                quic_port,
                node_type,
            ) => {
                if let Ok(mut storage) = self.storage.lock() {
                    match storage.get_peers(&quorum_public_key) {
                        None => ResponsePayload::Failed(format!(
                            "Failed to register Peer as Quorum Public key {:?} is not registered  ",
                            quorum_key
                        )),
                        Some(_) => match SocketAddr::from_str(address.as_str()) {
                            Ok(_) => {
                                let peer_data = PeerData {
                                    address,
                                    raptor_udp_port: raptor_port,
                                    quic_port,
                                    node_type: NodeType::from_str(node_type.as_str()).unwrap(),
                                };
                                storage.register_peer(quorum_public_key, peer_data);
                                ResponsePayload::RegistrationSuccessful
                            }
                            Err(_) => ResponsePayload::RegistrationFailed,
                        },
                    }
                } else {
                    ResponsePayload::Failed(format!("Failed to obtain lock on storage"))
                }
            }
            RequestPayload::RegisterNameSpace(name_space, quorum_key) => {
                let mut storage = self.storage.lock().unwrap();
                let key = Node::get_key(&quorum_key);
                match storage.get_peers(&name_space) {
                    None => {
                        storage.register_peer(key, PeerData::default());
                        storage.register_namespace(name_space, hex::encode(quorum_key));
                        ResponsePayload::RegistrationSuccessful
                    }
                    Some(_) => {
                        storage.register_namespace(name_space, hex::encode(quorum_key));
                        ResponsePayload::RegistrationSuccessful
                    }
                }
            }
        };
        self.protocol.send_message(
            &Message::Response(Response {
                request: request.clone(),
                receiver,
                payload,
            }),
            &request.sender,
        )
    }

    /// "Clone a slice into an array of the same size."
    ///
    /// The first line is the function signature. It says that the function takes a slice of type `T`
    /// and returns an array of type `A`. The `where` clause says that `A` must be a type that can be
    /// sized, defaulted, and converted to a mutable slice of `T`. The `T` type must be cloneable
    ///
    /// Arguments:
    ///
    /// * `slice`: &[T]
    ///
    /// Returns:
    ///
    /// A
    fn clone_into_array<A, T>(slice: &[T]) -> A
    where
        A: Sized + Default + AsMut<[T]>,
        T: Clone,
    {
        let mut a = Default::default();
        <A as AsMut<[T]>>::as_mut(&mut a).clone_from_slice(slice);
        a
    }

    /// It takes a string, hashes it, and returns the hash as a Key
    ///
    /// Arguments:
    ///
    /// * `key`: The key to be hashed.
    ///
    /// Returns:
    ///
    /// A Key struct with a 32 byte array.
    pub(crate) fn get_key(key: &[u8]) -> Key {
        let mut hasher = Sha3_256::default();
        hasher.input(key);
        Key(Self::clone_into_array(hasher.result().as_slice()))
    }

    /// Handles a response RPC. If the id in the response does not match any outgoing request, then
    /// the response will be ignored.
    fn handle_response(&mut self, response: &Response) {
        self.clone().update_routing_table(response.receiver.clone());
        let pending_requests = self.pending_requests.lock().unwrap();
        let Response { ref request, .. } = response.clone();
        if let Some(sender) = pending_requests.get(&request.id) {
            info!(
                "{} - Receiving response from {} {:#?}",
                self.node_data.addr, response.receiver.addr, response.payload,
            );
            sender.send(response.clone()).unwrap();
        } else {
            warn!(
                "{} - Original request not found; irrelevant response or expired request.",
                self.node_data.addr
            );
        }
    }

    /// Sends a request RPC.
    fn send_request(&mut self, dest: &NodeData, payload: RequestPayload) -> Option<Response> {
        info!(
            "{} - Sending request to {} {:#?}",
            self.node_data.addr, dest.addr, payload
        );
        let (response_tx, response_rx) = channel();
        let mut pending_requests = self.pending_requests.lock().unwrap();
        let mut token = Key::rand();

        while pending_requests.contains_key(&token) {
            token = Key::rand();
        }
        pending_requests.insert(token, response_tx);
        drop(pending_requests);

        self.protocol.send_message(
            &Message::Request(Request {
                id: token,
                sender: (*self.node_data).clone(),
                payload,
            }),
            dest,
        );

        match response_rx.recv_timeout(Duration::from_millis(REQUEST_TIMEOUT)) {
            Ok(response) => {
                let mut pending_requests = self.pending_requests.lock().unwrap();
                pending_requests.remove(&token);
                Some(response)
            }
            Err(_) => {
                warn!(
                    "{} - Request to {} timed out after waiting for {} milliseconds",
                    self.node_data.addr, dest.addr, REQUEST_TIMEOUT
                );
                let mut pending_requests = self.pending_requests.lock().unwrap();
                pending_requests.remove(&token);
                let mut routing_table = self.routing_table.lock().unwrap();
                routing_table.remove_node(dest);
                None
            }
        }
    }

    /// Sends a `PING` RPC.
    pub fn rpc_ping(&mut self, dest: &NodeData) -> Option<Response> {
        self.send_request(dest, RequestPayload::Ping)
    }

    /// Send a request to a node to register a namespace.
    ///
    /// Arguments:
    ///
    /// * `dest`: The node to send the request to.
    /// * `key`: The key of the namespace to register.
    /// * `value`: QuorumPublicKey
    ///
    /// Returns:
    ///
    /// A response from the node that was sent the request.
    fn rpc_register_namespace(
        &mut self,
        dest: &NodeData,
        key: Key,
        value: QuorumPublicKey,
    ) -> Option<Response> {
        self.send_request(dest, RequestPayload::RegisterNameSpace(key, value))
    }

    /// `rpc_register_peer` sends a `RequestPayload::RegisterPeer` message to the `dest` node
    ///
    /// Arguments:
    ///
    /// * `dest`: The node to which the request is being sent.
    /// * `key`: The key of the peer to be registered.
    /// * `quorum_key`: The public key of the quorum that the peer is trying to join.
    /// * `namespace_type`: Valid Namespaces [`NAME_SPACES`]
    /// * `pk_share`: The public key share of the peer.
    /// * `sig_share`: Signature share of the peer
    /// * `payload`: This is the data that the peer has sent signature share for .
    /// * `address`: The address of the peer that is being registered.
    ///
    /// Returns:
    ///
    /// The response from the node that was sent the request.
    fn rpc_register_peer(
        &mut self,
        dest: &NodeData,
        key: Key,
        quorum_key: QuorumPublicKey,
        address: Address,
        raptor_port: u16,
        quic_port: u16,
        node_type: String,
    ) -> Option<Response> {
        self.send_request(
            dest,
            RequestPayload::RegisterPeer(
                key,
                quorum_key,
                address,
                raptor_port,
                quic_port,
                node_type,
            ),
        )
    }

    /// Sends a `FIND_NODE` RPC.
    fn rpc_find_node(&mut self, dest: &NodeData, key: &Key) -> Option<Response> {
        self.send_request(dest, RequestPayload::FindNode(*key))
    }

    /// Sends a `FIND_VALUE` RPC.
    fn rpc_find_value(&mut self, dest: &NodeData, key: &Key) -> Option<Response> {
        self.send_request(dest, RequestPayload::FindPeers(*key))
    }

    /// Sends a `FIND_VALUE` RPC.
    fn rpc_find_namespace(&mut self, dest: &NodeData, key: &Key) -> Option<Response> {
        self.send_request(dest, RequestPayload::FindNamespaces(*key))
    }

    /// Spawns a thread that sends either a `FIND_NODE` or a `FIND_VALUE` RPC.
    fn spawn_find_rpc(
        mut self,
        dest: NodeData,
        key: Key,
        sender: Sender<Option<Response>>,
        find_node: bool,
        is_name_space: bool,
    ) {
        thread::spawn(move || {
            let find_err = {
                if find_node {
                    sender.send(self.rpc_find_node(&dest, &key)).is_err()
                } else if is_name_space {
                    sender.send(self.rpc_find_namespace(&dest, &key)).is_err()
                } else {
                    sender.send(self.rpc_find_value(&dest, &key)).is_err()
                }
            };

            if find_err {
                warn!("Receiver closed channel before rpc returned.");
            }
        });
    }

    /// Iteratively looks up nodes to determine the closest nodes to `key`. The search begins by
    /// selecting `CONCURRENCY_PARAM` nodes in the routing table and adding it to a shortlist. It
    /// then sends out either `FIND_NODE` or `FIND_VALUE` RPCs to `CONCURRENCY_PARAM` nodes not yet
    /// queried in the shortlist. The node will continue to fill its shortlist until it did not find
    /// a closer node for a round of RPCs or if runs out of nodes to query. Finally, it will query
    /// the remaining nodes in its shortlist until there are no remaining nodes or if it has found
    /// `REPLICATION_PARAM` active nodes.
    fn lookup_nodes(&mut self, key: &Key, find_node: bool, is_namespace: bool) -> ResponsePayload {
        let routing_table = self.routing_table.lock().unwrap();
        let closest_nodes = routing_table.get_closest_nodes(key, CONCURRENCY_PARAM);
        drop(routing_table);

        let mut closest_distance = Key::new([255u8; KEY_LENGTH]);
        for node_data in &closest_nodes {
            closest_distance = cmp::min(closest_distance, key.xor(&node_data.id))
        }

        // initialize found nodes, queried nodes, and priority queue
        let mut found_nodes: HashSet<NodeData> = closest_nodes.clone().into_iter().collect();
        found_nodes.insert((*self.node_data).clone());
        let mut queried_nodes = HashSet::new();
        queried_nodes.insert((*self.node_data).clone());

        let mut queue: BinaryHeap<NodeDataDistancePair> = BinaryHeap::from(
            closest_nodes
                .into_iter()
                .map(|node_data| NodeDataDistancePair(node_data.clone(), node_data.id.xor(key)))
                .collect::<Vec<NodeDataDistancePair>>(),
        );

        let (tx, rx) = channel();

        let mut concurrent_thread_count = 0;

        // spawn initial find requests
        for _ in 0..CONCURRENCY_PARAM {
            if !queue.is_empty() {
                self.clone().spawn_find_rpc(
                    queue.pop().unwrap().0,
                    key.clone(),
                    tx.clone(),
                    find_node,
                    is_namespace,
                );
                concurrent_thread_count += 1;
            }
        }

        // loop until we could not find a closer node for a round or if no threads are running
        while concurrent_thread_count > 0 {
            while concurrent_thread_count < CONCURRENCY_PARAM && !queue.is_empty() {
                self.clone().spawn_find_rpc(
                    queue.pop().unwrap().0,
                    key.clone(),
                    tx.clone(),
                    find_node,
                    is_namespace,
                );
                concurrent_thread_count += 1;
            }

            let mut is_terminated = true;
            let response_opt = rx.recv().unwrap();
            concurrent_thread_count -= 1;
            match response_opt {
                Some(Response {
                    payload: ResponsePayload::Nodes(nodes),
                    receiver,
                    ..
                }) => {
                    queried_nodes.insert(receiver);
                    for node_data in nodes {
                        let curr_distance = node_data.id.xor(key);

                        if !found_nodes.contains(&node_data) {
                            if curr_distance < closest_distance {
                                closest_distance = curr_distance;
                                is_terminated = false;
                            }

                            found_nodes.insert(node_data.clone());
                            let dist = node_data.id.xor(key);
                            let next = NodeDataDistancePair(node_data.clone(), dist);
                            queue.push(next.clone());
                        }
                    }
                }
                Some(Response {
                    payload: ResponsePayload::Namespaces(value),
                    ..
                }) => return ResponsePayload::Namespaces(value),
                Some(Response {
                    payload: ResponsePayload::Value(value),
                    ..
                }) => return ResponsePayload::Value(value),
                _ => is_terminated = false,
            }

            if is_terminated {
                break;
            }
            debug!("CURRENT CLOSEST DISTANCE IS {:?}", closest_distance);
        }

        debug!(
            "{} TERMINATED LOOKUP BECAUSE NOT CLOSER OR NO THREADS WITH DISTANCE {:?}",
            self.node_data.addr, closest_distance,
        );

        // loop until no threads are running or if we found REPLICATION_PARAM active nodes
        while queried_nodes.len() < REPLICATION_PARAM {
            while concurrent_thread_count < CONCURRENCY_PARAM && !queue.is_empty() {
                self.clone().spawn_find_rpc(
                    queue.pop().unwrap().0,
                    key.clone(),
                    tx.clone(),
                    find_node,
                    is_namespace,
                );
                concurrent_thread_count += 1;
            }
            if concurrent_thread_count == 0 {
                break;
            }

            let response_opt = rx.recv().unwrap();
            concurrent_thread_count -= 1;

            match response_opt {
                Some(Response {
                    payload: ResponsePayload::Nodes(nodes),
                    receiver,
                    ..
                }) => {
                    queried_nodes.insert(receiver);
                    for node_data in nodes {
                        if !found_nodes.contains(&node_data) {
                            found_nodes.insert(node_data.clone());
                            let dist = node_data.id.xor(key);
                            let next = NodeDataDistancePair(node_data.clone(), dist);
                            queue.push(next.clone());
                        }
                    }
                }
                Some(Response {
                    payload: ResponsePayload::Value(value),
                    ..
                }) => return ResponsePayload::Value(value),
                _ => {}
            }
        }

        let mut ret: Vec<NodeData> = queried_nodes.into_iter().collect();
        ret.sort_by_key(|node_data| node_data.id.xor(key));
        ret.truncate(REPLICATION_PARAM);
        debug!("{} -  CLOSEST NODES ARE {:#?}", self.node_data.addr, ret);
        ResponsePayload::Nodes(ret)
    }

    /// > This function is used to register a namespace with the rendezvous server
    ///
    /// Arguments:
    ///
    /// * `key`: The key for the namespace.
    /// * `quorum_public_key`: The public key of the quorum that will be used to store the data.
    ///
    /// Returns:
    ///
    /// A Result<(), RendezvousError>
    pub fn register_namespace(
        &mut self,
        key: Key,
        quorum_public_key: Vec<u8>,
    ) -> Result<(), RendezvousError> {
        let keys = NAME_SPACES
            .iter()
            .map(|x| Node::get_key(x.as_bytes()))
            .collect::<Vec<Key>>();
        if keys.contains(&key) {
            if let ResponsePayload::Nodes(nodes) = self.lookup_nodes(&key, true, true) {
                for dest in nodes {
                    let mut node = self.clone();
                    let key_clone = key.clone();
                    let value_clone = quorum_public_key.clone();
                    thread::spawn(move || {
                        node.rpc_register_namespace(&dest, key_clone, value_clone.clone());
                    });
                }
            }
            return Ok(());
        } else {
            Err(RendezvousError::NSRegistrationFailed(format!(
                "Only Following Namespaces are allowed :{}",
                NAME_SPACES.to_vec().join(",")
            )))
        }
    }

    /// This function is used to register a peer with the rendezvous server
    ///
    /// Arguments:
    ///
    /// * `key`: The key for which the peer is being registered.
    /// * `quorum_public_key`: The public key of the quorum that the peer belongs to.
    /// * `name_space_type`: The namespace type of the peer.
    /// * `public_key_share`: This is the public key share of the peer.
    /// * `sig_share`: Signature share of the payload
    /// * `payload`: The payload that is signed by the quorum.
    /// * `address`: The IP address of the peer
    /// * `raptor_port`: The port on which the RaptorQ protocol is running.
    /// * `quic_port`: The port on which the peer is listening for quic connections.
    /// * `node_type`: String,
    ///
    /// Returns:
    ///
    /// A Result<(), RendezvousError>
    pub fn register_peer(
        &mut self,
        key: Key,
        quorum_public_key: Vec<u8>,
        name_space_type: Vec<u8>,
        public_key_share: Vec<u8>,
        sig_share: Vec<u8>,
        payload: Vec<u8>,
        address: String,
        raptor_port: u16,
        quic_port: u16,
        node_type: String,
    ) -> Result<(), RendezvousError> {
        let namespace_key = Node::get_key(&name_space_type);
        let keys = NAME_SPACES
            .iter()
            .map(|x| Node::get_key(x.as_bytes()))
            .collect::<Vec<Key>>();
        if keys.contains(&namespace_key) {
            if let Ok(mut storage) = self.storage.lock() {
                if let Some(namespace) = storage.namespaces.get(&namespace_key) {
                    if !namespace.contains(&hex::encode(quorum_public_key.clone())) {
                        return Err(RendezvousError::PeerRegistrationFailed(format!(
                            "Quorum Public Key Is not Registered"
                        )));
                    }
                } else {
                    return Err(RendezvousError::PeerRegistrationFailed(format!(
                        "Quorum Public Key Is not Registered"
                    )));
                }
                drop(storage);
            } else {
                return Err(RendezvousError::Unknown(format!(
                    "Failed to obtain lock on storage"
                )));
            }
            let pk_share_arr = TryInto::<[u8; 48]>::try_into(public_key_share).unwrap();
            if let Ok(pk_share) = PublicKeyShare::from_bytes::<[u8; 48]>(pk_share_arr) {
                let sig_share = TryInto::<[u8; 96]>::try_into(sig_share).unwrap();
                if let Ok(sig_share) = SignatureShare::from_bytes::<[u8; 96]>(sig_share) {
                    if pk_share.verify(&sig_share, &payload) {
                        if let ResponsePayload::Nodes(nodes) = self.lookup_nodes(&key, true, false)
                        {
                            for dest in nodes {
                                let mut node = self.clone();
                                let key_clone = key.clone();
                                let quorum_key_clone = quorum_public_key.clone();
                                let address_clone = address.clone();
                                let node_type_clone = node_type.clone();
                                thread::spawn(move || {
                                    node.rpc_register_peer(
                                        &dest,
                                        key_clone,
                                        quorum_key_clone,
                                        address_clone,
                                        raptor_port,
                                        quic_port,
                                        node_type_clone,
                                    );
                                });
                            }
                        }
                    } else {
                        return Err(RendezvousError::PeerRegistrationFailed(format!(
                            "Signature verification failed"
                        )));
                    }
                    return Ok(());
                } else {
                    return Err(RendezvousError::PeerRegistrationFailed(format!(
                        "Sig Share decoding into 48Sized byte array failed "
                    )));
                }
            } else {
                return Err(RendezvousError::PeerRegistrationFailed(format!(
                    "PK share decoding into 48Sized byte array failed "
                )));
            };
        } else {
            Err(RendezvousError::NSRegistrationFailed(format!(
                "Only Following Namespaces are allowed :{}",
                NAME_SPACES.to_vec().join(",")
            )))
        }
    }

    pub fn create_hash(peer_data: &PeerData) -> u64 {
        let mut hasher = DefaultHasher::new();
        peer_data.address.hash(&mut hasher);
        //  peer_data.raptor_udp_port.hash(&mut hasher);
        //  peer_data.quic_port.hash(&mut hasher);
        hasher.finish()
    }
    /// Gets the value associated with a particular key in the DHT. Returns `None` if the key was
    /// not found.
    pub fn get(
        &mut self,
        key: &Key,
        values_received: ExportedFilter,
    ) -> Option<(HashSet<PeerData>, ExportedFilter)> {
        if let ResponsePayload::Value(value) = self.lookup_nodes(key, false, false) {
            let mut filter: CuckooFilter<DefaultHasher> = CuckooFilter::with_capacity(500);
            for peer_data in value.iter() {
                let hash = Node::create_hash(peer_data);
                let _ = filter.add(&hash);
            }
            let exported_filter = filter.export();
            let saved = serde_json::to_string(&exported_filter).unwrap();
            let mut new_values = HashSet::new();
            if values_received.len() > 0 {
                let restore_string = String::from_utf8_lossy(&values_received).to_string();
                if let Ok(restore_json) =
                    serde_json::from_str::<ExportedCuckooFilter>(&restore_string)
                {
                    let recovered_filter = CuckooFilter::<DefaultHasher>::from(restore_json);
                    for peer_data in value.iter() {
                        let hash = Node::create_hash(peer_data);
                        if !recovered_filter.contains(&hash) {
                            new_values.insert(peer_data.clone());
                        }
                    }
                }
            } else {
                let mut rng = rand::thread_rng();
                let values: Vec<PeerData> = value.iter().cloned().collect();
                let random_peers = values.choose_multiple(&mut rng, NUMBER_PEER_DATA_TO_SEND);
                let random_peers: HashSet<_> = random_peers.collect();
                new_values = random_peers.iter().map(|&data| data.clone()).collect();
            }
            Some((new_values, saved.as_bytes().to_vec()))
        } else {
            None
        }
    }

    /// Gets the value associated with a particular key in the DHT. Returns `None` if the key was
    /// not found.
    pub fn get_namespaces(&mut self, key: &Key) -> Option<HashSet<String>> {
        if let ResponsePayload::Namespaces(value) = self.lookup_nodes(key, false, true) {
            Some(value)
        } else {
            None
        }
    }

    /// Returns the `NodeData` associated with the node.
    pub fn node_data(&self) -> NodeData {
        (*self.node_data).clone()
    }

    /// Kills the current node and all active threads.
    pub fn kill(&self) {
        self.protocol.send_message(&Message::Kill, &self.node_data);
    }
}
