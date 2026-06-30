//! The bundled agent skills and their installer.

use std::{
    env::var_os,
    fs::{create_dir_all, write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::Subcommand;

/// The bundled skill files, as `(relative path under the skills dir, contents)`.
/// Embedded at compile time so the binary is self-contained.
const SKILL_FILES: &[(&str, &str)] = &[
    ("infino/SKILL.md", include_str!("../skills/infino/SKILL.md")),
    (
        "infino/references/WORKFLOWS.md",
        include_str!("../skills/infino/references/WORKFLOWS.md"),
    ),
    (
        "infino-search/SKILL.md",
        include_str!("../skills/infino-search/SKILL.md"),
    ),
    (
        "infino-search/references/SEARCH.md",
        include_str!("../skills/infino-search/references/SEARCH.md"),
    ),
    (
        "infino-data/SKILL.md",
        include_str!("../skills/infino-data/SKILL.md"),
    ),
    (
        "infino-data/references/SCHEMA.md",
        include_str!("../skills/infino-data/references/SCHEMA.md"),
    ),
];

#[derive(Subcommand)]
pub enum SkillsCommand {
    /// Write the bundled skills into an agent's skills directory.
    Install {
        /// Skills directory (default: ~/.claude/skills).
        #[arg(long)]
        dir: Option<PathBuf>,
    },
    /// Report which bundled skills are installed.
    Status {
        /// Skills directory (default: ~/.claude/skills).
        #[arg(long)]
        dir: Option<PathBuf>,
    },
}

pub fn run(command: &SkillsCommand) -> Result<()> {
    match command {
        SkillsCommand::Install { dir } => {
            let base = resolve_dir(dir.as_deref())?;
            for (rel, body) in SKILL_FILES {
                let path = base.join(rel);
                let parent = path.parent().expect("skill path has a parent");
                create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
                write(&path, body).with_context(|| format!("writing {}", path.display()))?;
            }
            println!(
                "installed {} skill files -> {}",
                SKILL_FILES.len(),
                base.display()
            );
        }
        SkillsCommand::Status { dir } => {
            let base = resolve_dir(dir.as_deref())?;
            for (rel, _) in SKILL_FILES {
                let path = base.join(rel);
                let mark = if path.exists() { "ok" } else { "missing" };
                println!("[{mark}] {}", path.display());
            }
        }
    }
    Ok(())
}

/// The explicit `--dir`, else `~/.claude/skills`.
fn resolve_dir(dir: Option<&Path>) -> Result<PathBuf> {
    if let Some(dir) = dir {
        return Ok(dir.to_path_buf());
    }
    let home = var_os("HOME").context("HOME is not set; pass --dir")?;
    Ok(Path::new(&home).join(".claude").join("skills"))
}
