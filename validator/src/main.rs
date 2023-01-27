// Copyright(C) Mundis.
use {
    anyhow::{anyhow, Context, Result},
    clap::*,
    colored::Colorize,
    mundis_model::{
        committee::Committee, config::ValidatorConfig, keypair::read_keypair_file,
    },
    mundis_validator::validator::Validator,
    std::path::PathBuf,
    tokio::runtime::Runtime,
};

#[allow(clippy::large_enum_variant)]
#[derive(Parser)]
#[clap(
    name = "mundis-validator",
    about = "Mundis validator node",
    version,
    rename_all = "kebab-case"
)]
pub enum ValidatorCommand {
    #[clap(name = "run", about = "Start the validator node")]
    Run {
        #[clap(long, help = "Validator identity keypair", required = true)]
        keypair: PathBuf,
        #[clap(long, help = "Ledger path", required = true)]
        ledger_path: PathBuf,
        #[clap(long, help = "Committee file", required = true)]
        committee: PathBuf
    },
}

impl ValidatorCommand {
    pub fn execute(self) -> Result<()> {
        match self {
            ValidatorCommand::Run {
                keypair,
                ledger_path,
                committee,
            } => {
                let identity = read_keypair_file(keypair.as_path()).context(format!(
                    "Could not read identity keypair {}",
                    keypair.display()
                ))?;
                if ledger_path.is_file() {
                    return Err(anyhow!(format!(
                        "The provided ledger path {} is not a directory.",
                        ledger_path.display()
                    )));
                }
                if !ledger_path.exists() {
                    std::fs::create_dir_all(ledger_path.clone()).map_err(|e| {
                        anyhow!(
                            "Could not create ledger directory {}: {}",
                            ledger_path.display(),
                            e.to_string()
                        )
                    })?;
                }

                let ledger_path = ledger_path.display().to_string();
                let validator_config = ValidatorConfig {
                    identity,
                    ledger_path,
                    initial_committee: Committee::load(committee),
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
        }
        Err(e) => {
            println!("{}", e.to_string().bold().red());
            std::process::exit(1);
        }
    }
}
