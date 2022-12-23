use std::io::{stdin, stdout, Write};
use std::path::PathBuf;
use std::process::exit;
use anyhow::anyhow;
use bip39::{Language, Mnemonic, MnemonicType, Seed};

use clap::*;
use colored::{ColoredString, Colorize};
use log::info;
use rpassword::prompt_password;
use mundis_model::keypair::{generate_seed_from_seed_phrase_and_passphrase, Keypair, keypair_from_seed, keypair_from_seed_phrase_and_passphrase, read_keypair_file, Signer, write_keypair, write_keypair_file};

use mundis_model::pubkey::Pubkey;

#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum KeytoolCommand {
    #[clap(name = "new", about = "Generate new keypair file from a random seed phrase and optional BIP39 passphrase")]
    New {
        #[clap(help = "Path to generated file", required = true)]
        outfile: PathBuf,
        #[clap(long, help = "Overwrite the output file if it exists")]
        force: bool,
        #[clap(help = "Do not prompt for a BIP39 passphrase")]
        no_passphrase: bool,
        #[clap(long, help = "Do not display seed phrase. Useful when piping output to other programs that prompt for user input, like gpg")]
        silent: bool,
    },

    #[clap(name = "pubkey", about = "Display the pubkey from a keypair file")]
    Pubkey {
        #[clap(help = "Filepath or URL to a keypair", required = true)]
        keypair_file: PathBuf,
    },

    #[clap(name = "recover", about = "Recover keypair from a seed phrase and an optional passphrase")]
    Recover {
        #[clap(help = "Path to generated file", required = true)]
        outfile: PathBuf,
        #[clap(long, help = "Overwrite the output file if it exists")]
        force: bool,
    },
}

impl KeytoolCommand {
    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            KeytoolCommand::New { outfile, force, no_passphrase, silent } => {
                if !force && outfile.exists() {
                    return Err(anyhow!("Refusing to overwrite {} without --force flag", outfile.display()));
                }

                let mnemonic = Mnemonic::new(MnemonicType::Words12, Language::English);
                let (passphrase, passphrase_message) = Self::acquire_passphrase_and_message(no_passphrase)?;
                let seed = Seed::new(&mnemonic, &passphrase);
                let keypair = keypair_from_seed(seed.as_bytes())?;

                Self::output_keypair(&keypair, outfile.to_str().unwrap(), "new")
                    .map_err(|err| anyhow!("Unable to write {}: {}", outfile.display(), err))?;

                if !silent {
                    let phrase: &str = mnemonic.phrase();
                    let divider = String::from_utf8(vec![b'='; phrase.len()]).unwrap();
                    println!("\n{}\n\
                        pubkey: {}\n\
                        {}\n\
                        {}:\n\
                        {}\n\
                        {}",
                             &divider,
                             keypair.pubkey().to_string().bold(),
                             &divider,
                             format!("Save this seed phrase{} to recover your new keypair:", passphrase_message).to_string().red(),
                             phrase.bold(),
                             &divider
                    );
                }
            }
            KeytoolCommand::Pubkey { keypair_file } => {
                if !keypair_file.exists() {
                    return Err(anyhow!("The provided keypair {} does not exist", keypair_file.display()));
                }

                let keypair = read_keypair_file(keypair_file.as_path())?;
                println!("{}", keypair.pubkey().to_string());
            }
            KeytoolCommand::Recover { outfile, force } => {
                if !force && outfile.exists() {
                    return Err(anyhow!("Refusing to overwrite {} without --force flag", outfile.display()));
                }


                let seed_phrase = prompt_password("Enter seed phrase: ")?;
                let seed_phrase = seed_phrase.trim();
                let passphrase = Self::prompt_passphrase(
                    "If this seed phrase has an associated passphrase, enter it now. Otherwise, press ENTER to continue:"
                )?;

                let keypair = keypair_from_seed_phrase_and_passphrase(&seed_phrase, passphrase.as_str())?;
                let pubkey = keypair.pubkey();
                print!("Recovered pubkey `{pubkey:?}`. Continue? (y/n): ");
                let _ignored = stdout().flush();
                let mut input = String::new();
                stdin().read_line(&mut input).expect("Unexpected input");
                if input.to_lowercase().trim() != "y" {
                    println!("Exiting");
                    exit(1);
                }

                Self::output_keypair(&keypair, outfile.to_str().unwrap(), "recovered")
                    .map_err(|err| anyhow!("Unable to write {}: {}", outfile.display(), err))?;
            }
        }

        Ok(())
    }

    fn acquire_passphrase_and_message(no_passphrase: bool) -> anyhow::Result<(String, String)> {
        if no_passphrase {
            Ok(Self::no_passphrase_and_message())
        } else {
            match Self::prompt_passphrase("\nFor added security, enter a passphrase\n\
                 \nNOTE: \n\
                 - This passphrase improves security of the recovery seed phrase. \n\
                 - It does NOT improve the security of the keypair file itself, which is stored as insecure plain text\n\
                \nPassphrase (empty for none): ",
            ) {
                Ok(passphrase) => {
                    println!();
                    Ok((passphrase, " and your passphrase".to_string()))
                }
                Err(e) => Err(e),
            }
        }
    }

    fn no_passphrase_and_message() -> (String, String) {
        ("".to_string(), "".to_string())
    }

    /// Prompts user for a passphrase and then asks for confirmirmation to check for mistakes
    fn prompt_passphrase(prompt: &str) -> anyhow::Result<String> {
        let passphrase = prompt_password(prompt)?;
        if !passphrase.is_empty() {
            let confirmed = prompt_password("Enter same passphrase again: ")?;
            if confirmed != passphrase {
                return Err(anyhow!("Passphrases did not match"));
            }
        }
        Ok(passphrase)
    }

    fn output_keypair(
        keypair: &Keypair,
        outfile: &str,
        source: &str,
    ) -> anyhow::Result<()> {
        if outfile == "-" {
            let mut stdout = std::io::stdout();
            write_keypair(keypair, &mut stdout)?;
        } else {
            write_keypair_file(keypair, outfile)?;
            println!("Wrote {source} keypair to {outfile}");
        }
        Ok(())
    }
}