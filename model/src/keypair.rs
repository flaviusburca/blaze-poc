// Copyright(C) Mundis.
use {
    crate::{
        pubkey::Pubkey,
        signature::{Signature, Signer, SignerError},
    },
    anyhow::anyhow,
    ed25519_dalek::Signer as DalekSigner,
    hmac::Hmac,
    rand::{rngs::OsRng, CryptoRng, RngCore},
    std::{
        fs,
        fs::{File, OpenOptions},
        io::{Read, Write},
        path::Path,
    },
};

/// A plain, vanilla Ed25519 key pair
pub struct Keypair(ed25519_dalek::Keypair);

impl Keypair {
    /// Constructs a new, random `Keypair` using a caller-proveded RNG
    pub fn generate<R>(csprng: &mut R) -> Self
    where
        R: CryptoRng + RngCore,
    {
        Self(ed25519_dalek::Keypair::generate(csprng))
    }

    /// Constructs a new, random `Keypair` using `OsRng`
    pub fn new() -> Self {
        let mut rng = OsRng::default();
        Self::generate(&mut rng)
    }

    /// Recovers a `Keypair` from a byte array
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ed25519_dalek::SignatureError> {
        ed25519_dalek::Keypair::from_bytes(bytes).map(Self)
    }

    /// Returns this `Keypair` as a byte array
    pub fn to_bytes(&self) -> [u8; 64] {
        self.0.to_bytes()
    }

    /// Recovers a `Keypair` from a base58-encoded string
    pub fn from_base58_string(s: &str) -> Self {
        Self::from_bytes(&bs58::decode(s).into_vec().unwrap()).unwrap()
    }

    /// Returns this `Keypair` as a base58-encoded string
    pub fn to_base58_string(&self) -> String {
        bs58::encode(&self.0.to_bytes()).into_string()
    }

    /// Gets this `Keypair`'s SecretKey
    pub fn secret(&self) -> &ed25519_dalek::SecretKey {
        &self.0.secret
    }
}

impl Clone for Keypair {
    fn clone(&self) -> Self {
        let bytes = self.to_bytes().to_vec();
        Keypair::from_bytes(bytes.as_slice()).unwrap()
    }
}

/// Writes a `Keypair` to a file with JSON-encoding
pub fn write_keypair_file<F: AsRef<Path>>(keypair: &Keypair, outfile: F) -> anyhow::Result<String> {
    let outfile = outfile.as_ref();

    if let Some(outdir) = outfile.parent() {
        fs::create_dir_all(outdir)?;
    }

    let mut f = {
        #[cfg(not(unix))]
        {
            OpenOptions::new()
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            OpenOptions::new().mode(0o600)
        }
    }
    .write(true)
    .truncate(true)
    .create(true)
    .open(outfile)?;

    write_keypair(keypair, &mut f)
}

/// Writes a `Keypair` to a `Write` implementor with JSON-encoding
pub fn write_keypair<W: Write>(keypair: &Keypair, writer: &mut W) -> anyhow::Result<String> {
    let keypair_bytes = keypair.0.to_bytes();
    let serialized = serde_json::to_string(&keypair_bytes.to_vec())?;
    writer.write_all(&serialized.clone().into_bytes())?;
    Ok(serialized)
}

/// Reads a JSON-encoded `Keypair` from a `Reader` implementor
pub fn read_keypair<R: Read>(reader: &mut R) -> anyhow::Result<Keypair> {
    let bytes: Vec<u8> = serde_json::from_reader(reader)?;
    let dalek_keypair =
        ed25519_dalek::Keypair::from_bytes(&bytes).map_err(|e| anyhow!(e.to_string()))?;
    Ok(Keypair(dalek_keypair))
}

/// Reads a `Keypair` from a file
pub fn read_keypair_file<F: AsRef<Path>>(path: F) -> anyhow::Result<Keypair> {
    let mut file = File::open(path.as_ref())?;
    read_keypair(&mut file)
}

/// Constructs a `Keypair` from caller-provided seed entropy
pub fn keypair_from_seed(seed: &[u8]) -> anyhow::Result<Keypair> {
    if seed.len() < ed25519_dalek::SECRET_KEY_LENGTH {
        return Err(anyhow!("Seed is too short"));
    }
    let secret = ed25519_dalek::SecretKey::from_bytes(&seed[..ed25519_dalek::SECRET_KEY_LENGTH])
        .map_err(|e| anyhow!(e.to_string()))?;
    let public = ed25519_dalek::PublicKey::from(&secret);
    let dalek_keypair = ed25519_dalek::Keypair { secret, public };
    Ok(Keypair(dalek_keypair))
}

pub fn generate_seed_from_seed_phrase_and_passphrase(
    seed_phrase: &str,
    passphrase: &str,
) -> Vec<u8> {
    const PBKDF2_ROUNDS: u32 = 2048;
    const PBKDF2_BYTES: usize = 64;

    let salt = format!("mnemonic{}", passphrase);

    let mut seed = vec![0u8; PBKDF2_BYTES];
    pbkdf2::pbkdf2::<Hmac<sha2::Sha512>>(
        seed_phrase.as_bytes(),
        salt.as_bytes(),
        PBKDF2_ROUNDS,
        &mut seed,
    );
    seed
}

pub fn keypair_from_seed_phrase_and_passphrase(
    seed_phrase: &str,
    passphrase: &str,
) -> anyhow::Result<Keypair> {
    keypair_from_seed(&generate_seed_from_seed_phrase_and_passphrase(
        seed_phrase,
        passphrase,
    ))
}

impl Signer for Keypair {
    fn pubkey(&self) -> Pubkey {
        Pubkey::new(self.0.public.as_ref())
    }

    fn try_pubkey(&self) -> Result<Pubkey, SignerError> {
        Ok(self.pubkey())
    }

    fn sign_message(&self, message: &[u8]) -> Signature {
        Signature::new(&self.0.sign(message).to_bytes())
    }

    fn try_sign_message(&self, message: &[u8]) -> Result<Signature, SignerError> {
        Ok(self.sign_message(message))
    }

    fn is_interactive(&self) -> bool {
        false
    }
}

impl<T> PartialEq<T> for Keypair
where
    T: Signer,
{
    fn eq(&self, other: &T) -> bool {
        self.pubkey() == other.pubkey()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clone() {
        let keypair1 = Keypair::new();
        let keypair2 = keypair1.clone();
        assert_eq!(keypair1.to_bytes(), keypair2.to_bytes());
    }
}
