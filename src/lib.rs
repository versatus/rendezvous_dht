mod key;
mod node;
mod protocol;
mod routing;
mod storage;

pub use self::key::Key;
pub use self::node::node_data::NodeData;
pub use self::node::Node;

/// The number of bytes in a key.
const KEY_LENGTH: usize = 32;

/// The maximum length of the message in bytes.
const MESSAGE_LENGTH: usize = 8196;

/// The maximum number of k-buckets in the routing table.
const ROUTING_TABLE_SIZE: usize = KEY_LENGTH * 8;

/// The maximum number of entries in a k-bucket.
const REPLICATION_PARAM: usize = 20;

/// The maximum number of active RPCs during `lookup_nodes`.
const CONCURRENCY_PARAM: usize = 3;

/// Request timeout time in milliseconds
const REQUEST_TIMEOUT: u64 = 5000;

/// Key-value pair expiration time in seconds
const KEY_EXPIRATION: u64 = 86400;

/// Bucket refresh interval in seconds
const BUCKET_REFRESH_INTERVAL: u64 = 3600;

/// Ping Nodes after every N seconds
const PING_TIME_INTERVAL: u64 = 5;

const NAME_SPACES: &'static [&'static str] =
    &["FARMER", "HARVESTER", "MESSAGE_CONTROL", "CREDIT_MODEL"];

/// The number of times node is not reachable.
const UNREACHABLE_THRESDHOLD: u8 = 20;

/// `NUMBER_PEER_DATA_TO_SEND`  the number of peer data to send during request peers operation.
const NUMBER_PEER_DATA_TO_SEND: usize = 3;

/// `RETRY_ATTEMPTS` is a constant that specifies the maximum number of retry attempts for any operation
const RETRY_ATTEMPTS: i32 = 3;
