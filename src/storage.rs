use crate::key::Key;
use crate::protocol::{FailedPingCount, PeerData};
use crate::{Node, KEY_EXPIRATION};
use lru_time_cache::LruCache;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

/// A simple storage container that removes stale items.
///
/// `Storage` will remove a item if it is older than `KEY_EXPIRATION` seconds.
#[derive(Clone)]
pub struct Storage {
    pub quorum_peers: LruCache<Key, HashSet<PeerData>>,
    pub namespaces: LruCache<Key, HashSet<String>>,
    pub unreachable_peers: HashMap<Key, (Key, SocketAddr, FailedPingCount)>,
}

impl Storage {
    /// Constructs a new, empty `Storage`.
    pub fn new() -> Self {
        let time_to_live = std::time::Duration::from_secs(KEY_EXPIRATION);
        Storage {
            quorum_peers: LruCache::<Key, HashSet<PeerData>>::with_expiry_duration(time_to_live),
            namespaces: LruCache::<Key, HashSet<String>>::with_expiry_duration(time_to_live),
            unreachable_peers: HashMap::new(),
        }
    }

    /// Inserts an item into `Storage`.
    pub fn register_peer(&mut self, key: Key, value: PeerData) {
        let (peer, expired_entries) = self.quorum_peers.notify_get(&key);
        if let Some(peer_info) = peer {
            let mut new_peer_info = peer_info.clone();
            new_peer_info.insert(value);
            self.quorum_peers.insert(key, new_peer_info);
        } else {
            let mut peers = HashSet::new();
            peers.insert(value);
            self.quorum_peers.insert(key, peers);
        }
        for (key, value) in expired_entries.iter() {
            let lst_expired_entries: Vec<&PeerData> = value.iter().collect();
            if lst_expired_entries.len() > 0 {
                let namespace = lst_expired_entries[0].node_type.to_string();

                if let Some(quorum_peers) =
                    self.namespaces.get(&Node::get_key(namespace.as_bytes()))
                {
                    let mut keys = quorum_peers
                        .iter()
                        .map(|x| (x.clone(), Node::get_key(x.as_bytes())))
                        .collect::<Vec<(String, Key)>>();
                    keys.retain(|x| x.1 != *key);
                    let keys: Vec<String> = keys.iter().map(|x| x.0.clone()).collect();
                    self.namespaces.insert(
                        Node::get_key(namespace.as_bytes()),
                        HashSet::from_iter(keys),
                    );
                }
            }
        }
    }

    pub fn register_namespace(&mut self, key: Key, value: String) {
        if let Some(quorum_ids) = self.namespaces.get(&key) {
            let mut new_quorum_ids = quorum_ids.clone();
            new_quorum_ids.insert(value);
            self.namespaces.insert(key, new_quorum_ids);
        } else {
            let mut quorum_ids = HashSet::new();
            quorum_ids.insert(value);
            self.namespaces.insert(key, quorum_ids);
        }
    }

    /// Returns the value associated with `key`. Returns `None` if such a key does not exist in
    /// `Storage`.
    pub fn get_peers(&mut self, key: &Key) -> Option<&HashSet<PeerData>> {
        let peers = self.quorum_peers.get(key);
        peers
    }

    /// > This function returns a reference to a HashSet of Strings, if it exists
    ///
    /// Arguments:
    ///
    /// * `key`: The key to get the namespaces for.
    ///
    /// Returns:
    ///
    /// A reference to a HashSet of Strings
    pub fn get_namespaces(&mut self, key: &Key) -> Option<&HashSet<String>> {
        self.namespaces.get(key)
    }
}
