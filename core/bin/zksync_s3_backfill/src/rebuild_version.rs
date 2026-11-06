use std::{fs::File, io::Read};

use sha2::{Digest, Sha256};

pub fn executable_sha256() -> anyhow::Result<String> {
    let mut executable = File::open(std::env::current_exe()?)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let count = executable.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hex::encode(hasher.finalize()))
}
