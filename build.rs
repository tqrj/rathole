use anyhow::Result;
use vergen::{vergen, Config};

fn main() -> Result<()> {
    #[cfg(feature = "git-version")]
    {
        let mut config = Config::default();
        // Change the SEMVER output to the lightweight variant
        *config.git_mut().semver_kind_mut() = vergen::SemverKind::Lightweight;
        // Add a `-dirty` flag to the SEMVER output
        *config.git_mut().semver_dirty_mut() = Some("-dirty");
        match vergen(config) {
            Ok(()) => return Ok(()),
            Err(e) => eprintln!("error occurred while generating instructions: {:?}", e),
        }
    }
    // Without git: only build and cargo instructions
    let mut config = Config::default();
    #[cfg(feature = "git-version")]
    {
        *config.git_mut().enabled_mut() = false;
    }
    vergen(config)
}
