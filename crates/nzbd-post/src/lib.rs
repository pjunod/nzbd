//! Post-processing: trait boundaries (phase 0), implementations in phase 2.
//!
//! The pipeline orchestrator (ARCHITECTURE.md §9) drives these traits through
//! the NZBGet-equivalent stage graph: par-rename → par-verify/repair →
//! rar-rename → unpack (⇆ repair retry) → cleanup → move → scripts.
//!
//! Design intent baked into the boundaries:
//! - `ParEngine::quick_verify` stays **native** (we hold per-segment CRC32s
//!   from download; par2 packet parsing is cheap) while `repair` is
//!   subprocess `par2cmdline-turbo` first, swappable for a native engine.
//! - `Extractor` is subprocess `unrar`/`7z` (licensing + crash isolation),
//!   hardened: argv-only, scrubbed env, timeout, bounded output, staging dir.
//! - `ScriptRunner` reproduces the NZBGet extension protocol byte-for-byte
//!   (`NZBPP_*` env, `[NZB] KEY=value` stdout commands, exit codes 92–95).

use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum PostError {
    #[error("tool not found: {0}")]
    ToolMissing(String),
    #[error("subprocess failed: {0}")]
    Subprocess(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyResult {
    /// All files present and matching — repair not needed.
    Intact,
    /// Repair is possible; `blocks_needed` recovery blocks required.
    Repairable {
        blocks_available: u32,
        blocks_needed: u32,
    },
    /// More recovery blocks required than available.
    NeedMoreBlocks {
        blocks_needed: u32,
    },
    Unrepairable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairResult {
    Repaired,
    Failed,
}

/// Known per-file evidence gathered during download, used for quick verify.
#[derive(Debug, Clone)]
pub struct DownloadEvidence {
    pub path: PathBuf,
    /// Combined CRC32 of the fully-downloaded file, if all segments succeeded.
    pub crc32: Option<u32>,
    /// (offset, len, crc) of each successfully downloaded segment, for
    /// partial files.
    pub segment_crcs: Vec<(u64, u32, u32)>,
}

pub trait ParEngine {
    /// CRC-based verification against the par2 set — no data re-read for
    /// fully downloaded files. Falls back to `verify_full` on any mismatch.
    fn quick_verify(
        &self,
        par2_file: &Path,
        evidence: &[DownloadEvidence],
    ) -> Result<VerifyResult, PostError>;

    fn verify_full(&self, par2_file: &Path) -> Result<VerifyResult, PostError>;

    fn repair(&self, par2_file: &Path) -> Result<RepairResult, PostError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveKind {
    Rar,
    SevenZip,
    Zip,
    Split, // .001/.002 joinable fragments
}

#[derive(Debug, Clone)]
pub struct ExtractOutcome {
    pub success: bool,
    /// Password was wrong / required (drives the password-file retry loop).
    pub password_error: bool,
    /// Out of disk space (unrar exit code 5).
    pub disk_space_error: bool,
}

pub trait Extractor {
    fn kind(&self) -> ArchiveKind;
    fn extract(
        &self,
        archive: &Path,
        dest: &Path,
        password: Option<&str>,
    ) -> Result<ExtractOutcome, PostError>;
}

/// NZBGet extension-script exit codes (post-processing scripts).
pub mod script_exit {
    pub const PAR_CHECK: i32 = 92;
    pub const SUCCESS: i32 = 93;
    pub const ERROR: i32 = 94;
    pub const NONE: i32 = 95;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptKind {
    PostProcessing, // NZBPP_*
    Scan,           // NZBNP_*
    Queue,          // NZBNA_*
    Scheduler,      // NZBSP_*
    Feed,           // NZBFP_*
    Command,        // NZBCP_*
}

#[derive(Debug, Clone)]
pub struct ScriptOutcome {
    pub exit_code: i32,
    /// `[NZB] KEY=value` commands emitted by the script.
    pub commands: Vec<(String, String)>,
}

pub trait ScriptRunner {
    fn run(
        &self,
        kind: ScriptKind,
        entry: &Path,
        env: &[(String, String)],
    ) -> Result<ScriptOutcome, PostError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_match_nzbget_protocol() {
        assert_eq!(script_exit::PAR_CHECK, 92);
        assert_eq!(script_exit::SUCCESS, 93);
        assert_eq!(script_exit::ERROR, 94);
        assert_eq!(script_exit::NONE, 95);
    }
}
