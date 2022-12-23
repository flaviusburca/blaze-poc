use std::fs;
use std::path::PathBuf;

use anyhow::anyhow;
use crate::command::MundisCommand;

pub(crate) async fn create_genesis(dest_path: PathBuf) -> anyhow::Result<()> {
    if dest_path.exists() {
        let files = dest_path.read_dir()?.count();
        if files > 0 {
            return Err(anyhow!("The destination genesis folder '{}' must be empty! Aborting...", dest_path.to_str().unwrap()));
        }
    } else {
        fs::create_dir_all(dest_path)?;
    }


    Ok(())
}