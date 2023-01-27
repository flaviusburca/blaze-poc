// Copyright(C) Mundis.
use {clap::*, mundis_model::pubkey::Pubkey, std::path::PathBuf};

#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum GenesisCommand {
    Generate {
        #[clap(
            long,
            help = "Bootstrap validator's identity",
            multiple = true,
            required = true
        )]
        bootstrap_validator: Vec<Pubkey>,
        #[clap(long, help = "Number of lamports to assign to bootstrap validators", default_value_t = 500 * 1_000_000_000)]
        bootstrap_validator_lamports: u64,
    },
}

impl GenesisCommand {
    pub async fn execute(self, ledger_path: PathBuf) -> anyhow::Result<()> {
        Ok(())
    }
}
