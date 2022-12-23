
pub type UnixTimestamp = u64;
pub type ChainId = u8;
pub type Epoch = u64;

pub const CHAIN_LOCAL: ChainId = 0xFF;
pub const CHAIN_DEVNET: ChainId = 0x10;
pub const CHAIN_TESTNET: ChainId = 0x20;
pub const CHAIN_MAINNET: ChainId = 0x30;
