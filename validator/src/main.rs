use std::path::PathBuf;
use std::process::id;
use std::ptr::null;
use std::thread::sleep;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::*;
use colored::Colorize;
use log::info;
use multiaddr::Multiaddr;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

use mundis_ledger::Store;
use mundis_model::committee::Committee;
use mundis_model::config::ValidatorConfig;
use mundis_model::keypair::{Keypair, read_keypair, read_keypair_file};
use mundis_model::signature::Signer;
use mundis_validator::validator::Validator;

#[allow(clippy::large_enum_variant)]
#[derive(Parser)]
#[clap(
name = "mundis-validator",
about = "Mundis validator node",
version,
rename_all = "kebab-case",
)]
pub enum ValidatorCommand {
    #[clap(name = "run", about = "Start the validator node")]
    Run {
        #[clap(long, help = "Validator identity keypair", required = true)]
        keypair: PathBuf,
        #[clap(long, help = "Ledger path", required = true)]
        ledger_path: PathBuf,
        #[clap(long, help = "Number of workers to start", default_value_t = 1)]
        num_workers: u8
    }
}

impl ValidatorCommand {
    pub fn execute(self) -> Result<()> {
        match self {
            ValidatorCommand::Run { keypair , ledger_path, num_workers } => {
                let identity = read_keypair_file(keypair.as_path())
                    .context(format!("Could not read identity keypair {}", keypair.display()))?;
                if ledger_path.is_file() {
                    return Err(anyhow!(format!("The provided ledger path {} is not a directory.", ledger_path.display())));
                }
                if !ledger_path.exists() {
                    std::fs::create_dir_all(ledger_path.clone())
                        .map_err(|e| anyhow!("Could not create ledger directory {}: {}", ledger_path.display(), e.to_string()))?;
                }

                let ledger_path = ledger_path.display().to_string();
                let pubkey = identity.pubkey().clone();
                let validator_config = ValidatorConfig {
                    identity,
                    ledger_path,
                    initial_committee: Committee::for_testing(pubkey),
                    num_workers
                };

                Runtime::new()?
                    .block_on(Validator::start_validator(&validator_config))
                    .unwrap();

                Ok(())
            }
        }
    }
}

fn main() {
    mundis_logger::setup();

    let command = ValidatorCommand::parse();
    match command.execute() {
        Ok(_) => {
            println!("{}", "Finished normally".to_string().green())
        },
        Err(e) => {
            println!("{}", e.to_string().bold().red());
            std::process::exit(1);
        }
    }
}
