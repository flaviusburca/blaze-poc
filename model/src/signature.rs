use std::{fmt, mem};
use std::borrow::{Borrow, Cow};
use std::str::FromStr;
use ed25519_dalek::SignatureError;
use generic_array::GenericArray;
use generic_array::typenum::U64;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use crate::hash::Hash;
use crate::keypair::Keypair;
use crate::pubkey::Pubkey;

/// Number of bytes in a signature
pub const SIGNATURE_BYTES: usize = 64;

/// Maximum string length of a base58 encoded signature
const MAX_BASE58_SIGNATURE_LEN: usize = 88;

#[derive(Serialize, Deserialize, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Signature(GenericArray<u8, U64>);

impl crate::sanitize::Sanitize for Signature {}

impl Signature {
    pub fn new(signature_slice: &[u8]) -> Self {
        Self(GenericArray::clone_from_slice(signature_slice))
    }

    pub fn new_unique() -> Self {
        let random_bytes: Vec<u8> = (0..64).map(|_| rand::random::<u8>()).collect();
        Self::new(&random_bytes)
    }

    pub(self) fn verify_verbose(
        &self,
        pubkey_bytes: &[u8],
        message_bytes: &[u8],
    ) -> Result<(), SignatureError> {
        let publickey = ed25519_dalek::PublicKey::from_bytes(pubkey_bytes)?;
        let signature = self.0.as_slice().try_into()?;
        publickey.verify_strict(message_bytes, &signature)
    }

    pub fn verify(&self, pubkey_bytes: &[u8], message_bytes: &[u8]) -> bool {
        self.verify_verbose(pubkey_bytes, message_bytes).is_ok()
    }

    pub fn verify_batch<'a, I>(hash: &Hash, pairs: I) -> Result<(), SignatureError>
        where I: IntoIterator<Item = &'a (Pubkey, Signature)>
    {
        let mut messages: Vec<&[u8]> = Vec::new();
        let mut signatures: Vec<ed25519_dalek::Signature> = Vec::new();
        let mut keys: Vec<ed25519_dalek::PublicKey> = Vec::new();
        for (key, sig) in pairs.into_iter() {
            messages.push( hash.as_ref());
            signatures.push(sig.0.as_slice().try_into()?);
            keys.push(ed25519_dalek::PublicKey::from_bytes(key.as_ref())?);
        }

        ed25519_dalek::verify_batch(&messages[..], &signatures[..], &keys[..])
    }
}

impl AsRef<[u8]> for Signature {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", bs58::encode(self.0).into_string())
    }
}

impl fmt::Display for Signature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", bs58::encode(self.0).into_string())
    }
}

impl From<Signature> for [u8; 64] {
    fn from(signature: Signature) -> Self {
        signature.0.into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ParseSignatureError {
    #[error("string decoded to wrong size for signature")]
    WrongSize,
    #[error("failed to decode string to signature")]
    Invalid,
}

impl FromStr for Signature {
    type Err = ParseSignatureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() > MAX_BASE58_SIGNATURE_LEN {
            return Err(ParseSignatureError::WrongSize);
        }
        let bytes = bs58::decode(s)
            .into_vec()
            .map_err(|_| ParseSignatureError::Invalid)?;
        if bytes.len() != mem::size_of::<Signature>() {
            Err(ParseSignatureError::WrongSize)
        } else {
            Ok(Signature::new(&bytes))
        }
    }
}


pub trait Signable {
    fn sign(&mut self, keypair: &Keypair) {
        let signature = keypair.sign_message(self.signable_data().borrow());
        self.set_signature(signature);
    }
    fn verify(&self) -> bool {
        self.get_signature()
            .verify(self.pubkey().as_ref(), self.signable_data().borrow())
    }

    fn pubkey(&self) -> Pubkey;
    fn signable_data(&self) -> Cow<[u8]>;
    fn get_signature(&self) -> Signature;
    fn set_signature(&mut self, signature: Signature);
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SignerError {
    #[error("keypair-pubkey mismatch")]
    KeypairPubkeyMismatch,

    #[error("not enough signers")]
    NotEnoughSigners,

    #[error("custom error: {0}")]
    Custom(String),

    // Remote Keypair-specific Errors
    #[error("connection error: {0}")]
    Connection(String),

    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("{0}")]
    Protocol(String),

    #[error("{0}")]
    UserCancel(String),

    #[error("too many signers")]
    TooManySigners,
}

/// The `Signer` trait declares operations that all digital signature providers
/// must support. It is the primary interface by which signers are specified in
/// `Transaction` signing interfaces
pub trait Signer {
    /// Infallibly gets the implementor's public key. Returns the all-zeros
    /// `Pubkey` if the implementor has none.
    fn pubkey(&self) -> Pubkey {
        self.try_pubkey().unwrap_or_default()
    }
    /// Fallibly gets the implementor's public key
    fn try_pubkey(&self) -> Result<Pubkey, SignerError>;
    /// Infallibly produces an Ed25519 signature over the provided `message`
    /// bytes. Returns the all-zeros `Signature` if signing is not possible.
    fn sign_message(&self, message: &[u8]) -> Signature {
        self.try_sign_message(message).unwrap_or_default()
    }
    /// Fallibly produces an Ed25519 signature over the provided `message` bytes.
    fn try_sign_message(&self, message: &[u8]) -> Result<Signature, SignerError>;
    /// Whether the impelmentation requires user interaction to sign
    fn is_interactive(&self) -> bool;
}

impl<T> From<T> for Box<dyn Signer>
    where
        T: Signer + 'static,
{
    fn from(signer: T) -> Self {
        Box::new(signer)
    }
}

impl PartialEq for dyn Signer {
    fn eq(&self, other: &dyn Signer) -> bool {
        self.pubkey() == other.pubkey()
    }
}

impl std::fmt::Debug for dyn Signer {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "Signer: {:?}", self.pubkey())
    }
}

/// Removes duplicate signers while preserving order. O(n²)
pub fn unique_signers(signers: Vec<&dyn Signer>) -> Vec<&dyn Signer> {
    signers.into_iter().unique_by(|s| s.pubkey()).collect()
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use crate::hash::Hashable;
    use crate::keypair::Keypair;
    use crate::signature::{Signature, Signer};

    pub fn keys() -> Vec<Keypair> {
        let mut rng = StdRng::from_seed([0; 32]);
        (0..4).map(|_| Keypair::generate(&mut rng)).collect()
    }

    #[test]
    fn verify_valid_signature() {
        let keypair = keys().pop().unwrap();
        let message: &[u8] = b"Hello, world!";
        let digest = message.hash();

        let signature = keypair.sign_message(message);
        assert!(signature.verify(keypair.pubkey().as_ref(), message));
    }

    #[test]
    fn verify_valid_batch() {
        let message: &[u8] = b"Hello, world!";
        let digest = message.hash();
        let mut keys = keys();
        let signatures: Vec<_> = (0..3)
            .map(|_| {
                let keypair = keys.pop().unwrap();
                (keypair.pubkey(), keypair.sign_message(digest.as_ref()))
            })
            .collect();

        assert!(Signature::verify_batch(&digest, &signatures).is_ok())
    }
}