// Copyright(C) Mundis.
use {clap::Parser, colored::Colorize, mundis_cli::main_commands::MundisCommand};

#[tokio::main]
async fn main() {
    mundis_logger::setup();

    let command = MundisCommand::parse();
    match command.execute().await {
        Ok(_) => (),
        Err(err) => {
            println!("{}", err.to_string().bold().red());
            std::process::exit(1);
        }
    }
}
