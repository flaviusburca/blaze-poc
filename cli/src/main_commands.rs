use crate::genesis_commands::GenesisCommand;
use crate::keytool_commands::KeytoolCommand;
use clap::*;
use mundis_model::pubkey::Pubkey;
use std::path::PathBuf;

#[allow(clippy::large_enum_variant)]
#[derive(Parser)]
#[clap(
    name = "mundis-cli",
    about = "Mundis command-line tool",
    version,
    rename_all = "kebab-case"
)]
pub enum MundisCommand {
    #[clap(name = "genesis", about = "Genesis commands")]
    Genesis {
        #[clap(
            long,
            help = "Build a genesis config, write it to the specified path, and exit"
        )]
        ledger_path: PathBuf,

        #[clap(subcommand)]
        cmd: GenesisCommand,
    },

    #[clap(name = "keytool")]
    Keytool {
        #[clap(subcommand)]
        cmd: KeytoolCommand,
    },
}

impl MundisCommand {
    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            MundisCommand::Genesis { ledger_path, cmd } => cmd.execute(ledger_path).await,
            MundisCommand::Keytool { cmd } => cmd.execute().await,
        }
    }
}
