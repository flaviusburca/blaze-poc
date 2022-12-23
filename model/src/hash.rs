//! Hashing with the [SHA-256] hash function, and a general [`Hash`] type.
//!
//! [SHA-256]: https://en.wikipedia.org/wiki/SHA-2
//! [`Hash`]: struct@Hash

use std::{fmt, mem};
use std::str::FromStr;
use digest::Digest;
use sha2::Sha256;
use thiserror::Error;
use serde::{Serialize, Deserialize};
use crate::sanitize::Sanitize;

/// Size of a hash in bytes.
pub const HASH_BYTES: usize = 32;
/// Maximum string length of a base58 encoded hash.
const MAX_BASE58_LEN: usize = 44;

/// A hash; the 32-byte output of a hashing algorithm.
#[derive(
Serialize,
Deserialize,
Clone,
Copy,
Default,
Eq,
PartialEq,
Ord,
PartialOrd,
Hash,
)]
pub struct Hash(pub(crate) [u8; HASH_BYTES]);

#[derive(Clone, Default)]
pub struct Hasher {
    hasher: Sha256,
}

impl Hasher {
    pub fn hashr(&mut self, val: impl AsRef<[u8]>) {
        self.hasher.update(val);
    }

    pub fn hash(&mut self, val: &[u8]) {
        self.hasher.update(val);
    }
    pub fn hashv(&mut self, vals: &[&[u8]]) {
        for val in vals {
            self.hash(val);
        }
    }
    pub fn result(self) -> Hash {
        // At the time of this writing, the sha2 library is stuck on an old version
        // of generic_array (0.9.0). Decouple ourselves with a clone to our version.
        Hash(<[u8; HASH_BYTES]>::try_from(self.hasher.finalize().as_slice()).unwrap())
    }
}

impl Sanitize for Hash {}

impl From<[u8; HASH_BYTES]> for Hash {
    fn from(from: [u8; 32]) -> Self {
        Self(from)
    }
}

impl AsRef<[u8]> for Hash {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", bs58::encode(self.0).into_string())
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", bs58::encode(self.0).into_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ParseHashError {
    #[error("string decoded to wrong size for hash")]
    WrongSize,
    #[error("failed to decoded string to hash")]
    Invalid,
}

impl FromStr for Hash {
    type Err = ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() > MAX_BASE58_LEN {
            return Err(ParseHashError::WrongSize);
        }
        let bytes = bs58::decode(s)
            .into_vec()
            .map_err(|_| ParseHashError::Invalid)?;
        if bytes.len() != mem::size_of::<Hash>() {
            Err(ParseHashError::WrongSize)
        } else {
            Ok(Hash::new(&bytes))
        }
    }
}

impl Hash {
    pub fn new(hash_slice: &[u8]) -> Self {
        Hash(<[u8; HASH_BYTES]>::try_from(hash_slice).unwrap())
    }

    pub const fn new_from_array(hash_array: [u8; HASH_BYTES]) -> Self {
        Self(hash_array)
    }

    /// unique Hash for tests and benchmarks.
    pub fn new_unique() -> Self {
        use crate::atomic_u64::AtomicU64;
        static I: AtomicU64 = AtomicU64::new(1);

        let mut b = [0u8; HASH_BYTES];
        let i = I.fetch_add(1);
        b[0..8].copy_from_slice(&i.to_le_bytes());
        Self::new(&b)
    }

    pub fn to_bytes(self) -> [u8; HASH_BYTES] {
        self.0
    }

    pub fn size(self) -> usize {
        self.0.len()
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

/// Return a Sha256 hash for the given data.
pub fn hash(val: &[u8]) -> Hash {
    hashv(&[val])
}

/// Return a Sha256 hash for the given data.
pub fn hashv(vals: &[&[u8]]) -> Hash {
    // Perform the calculation inline, calling this from within a program is not supported
    let mut hasher = Hasher::default();
    hasher.hashv(vals);
    hasher.result()
}

pub trait Hashable {
    fn hash(&self) -> Hash;
}

impl Hashable for &[u8] {
    fn hash(&self) -> Hash {
        let mut hasher = Hasher::default();
        hasher.hash(self);
        hasher.result()
    }
}