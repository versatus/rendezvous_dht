use crate::KEY_LENGTH;
use serde_derive::{Deserialize, Serialize};
use std::convert::{TryFrom, TryInto};
use std::fmt::{Debug, Formatter, Result};

/// A key that represents nodes and data.
///
/// The keys in this implementation of Kademlia have 256 bits as opposed to 160 bits so that SHA-3
/// can be used instead of SHA-1.
#[derive(Ord, PartialOrd, PartialEq, Eq, Clone, Hash, Serialize, Deserialize, Default, Copy)]
pub struct Key(pub [u8; KEY_LENGTH]);

impl Debug for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let hex_vec: Vec<String> = self.0.iter().map(|b| format!("{:02X}", b)).collect();
        write!(f, "{}", hex_vec.join(""))
    }
}

impl Key {
    /// Constructs a new `Key` from a byte array.
    pub fn new(data: [u8; KEY_LENGTH]) -> Self {
        Key(data)
    }

    /// Constructs a new, random `Key`.
    pub(super) fn rand() -> Self {
        let mut ret = Key([0; KEY_LENGTH]);
        for byte in &mut ret.0 {
            *byte = rand::random::<u8>();
        }
        ret
    }

    /// Constructs a new, random `Key` from `[2^(KEY_LENGTH - index - 1), 2^(KEY_LENGTH - index))`.
    pub(super) fn rand_in_range(index: usize) -> Self {
        let mut ret = Key::rand();
        let bytes = index / 8;
        let bit = index % 8;
        for i in 0..bytes {
            ret.0[i] = 0;
        }
        ret.0[bytes] &= 0xFF >> (bit);
        ret.0[bytes] |= 1 << (8 - bit - 1);
        ret
    }

    /// Returns the XOR result between `self` and `key`.
    pub(super) fn xor(&self, key: &Key) -> Key {
        let mut ret = [0; KEY_LENGTH];
        for (i, byte) in ret.iter_mut().enumerate() {
            *byte = self.0[i] ^ key.0[i];
        }
        Key(ret)
    }

    /// Returns the number of leading zeros in `self`. This is used to calculate the distance
    /// between keys.
    pub(super) fn leading_zeros(&self) -> usize {
        let mut ret = 0;
        for i in 0..KEY_LENGTH {
            if self.0[i] == 0 {
                ret += 8
            } else {
                return ret + self.0[i].leading_zeros() as usize;
            }
        }
        ret
    }

    pub fn to_key(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

/// This is a trait implementation for the TryFrom trait. This is used to convert a vector of bytes to a
/// Key.
impl TryFrom<Vec<u8>> for Key {
    type Error = String;
    fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        let key_data = value.try_into();
        match key_data {
            Ok(key) => Ok(Key(key)),
            Err(_) => Err(String::from(
                "Error occurred while trying to construct Key ",
            )),
        }
    }
}
