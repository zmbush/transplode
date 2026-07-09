use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Paths {
    pub organized: PathBuf,
    pub encoded: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Ssh {
    pub username: String,
    pub address: String,
    pub path: PathBuf,
    #[serde(default)]
    pub other_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Handbrake {
    pub cfg: PathBuf,
    pub preset: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub paths: Paths,
    pub ssh: Option<Ssh>,
    pub handbrake: Handbrake,
}

static CFG_NAME: &str = "transplode.toml";

pub fn load() -> eyre::Result<Config> {
    let mut current_dir = std::env::current_dir()?;

    loop {
        let target_file = current_dir.join(CFG_NAME);
        if target_file.is_file() {
            return Ok(toml::from_str(&std::fs::read_to_string(&target_file)?)?);
        }

        if !current_dir.pop() {
            break;
        }
    }

    eyre::bail!("Could not find config")
}
