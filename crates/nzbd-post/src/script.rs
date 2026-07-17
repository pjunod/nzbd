//! NZBGet extension-script protocol (ARCHITECTURE.md §3.2 "Script
//! protocol"): env-var interface, `[LEVEL]` stdout log lines, the
//! `[NZB] KEY=value` command channel, exit codes 92–95. Existing NZBGet
//! post-processing scripts run unmodified.

use crate::{PostError, ScriptOutcome};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};

pub struct ScriptHost {
    pub timeout: Duration,
}

impl ScriptHost {
    /// Run one script with the given environment (`NZBOP_*`/`NZBPP_*`/…
    /// built by the caller). Stdout is parsed line-by-line.
    pub async fn run(
        &self,
        entry: &Path,
        cwd: &Path,
        env: &[(String, String)],
    ) -> Result<ScriptOutcome, PostError> {
        let mut cmd = tokio::process::Command::new(entry);
        cmd.current_dir(cwd)
            .env_clear()
            .env(
                "PATH",
                std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into()),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        for (k, v) in env {
            cmd.env(k, v);
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| PostError::ToolMissing(format!("{}: {e}", entry.display())))?;

        let stdout = child.stdout.take().unwrap();
        let script = entry
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();

        let parse = async {
            let mut commands = Vec::new();
            let mut lines = BufReader::new(stdout).lines();
            let mut seen = 0usize;
            while let Ok(Some(line)) = lines.next_line().await {
                seen += 1;
                if seen > 100_000 {
                    break; // output cap
                }
                if let Some(rest) = line.strip_prefix("[NZB] ") {
                    if let Some((k, v)) = rest.split_once('=') {
                        commands.push((k.trim().to_string(), v.trim().to_string()));
                    }
                } else if let Some(rest) = line.strip_prefix("[ERROR] ") {
                    tracing::error!(%script, "{rest}");
                } else if let Some(rest) = line.strip_prefix("[WARNING] ") {
                    tracing::warn!(%script, "{rest}");
                } else if let Some(rest) = line.strip_prefix("[INFO] ") {
                    tracing::info!(%script, "{rest}");
                } else if let Some(rest) = line
                    .strip_prefix("[DETAIL] ")
                    .or_else(|| line.strip_prefix("[DEBUG] "))
                {
                    tracing::debug!(%script, "{rest}");
                } else if !line.is_empty() {
                    tracing::debug!(%script, "{line}");
                }
            }
            commands
        };

        let result = tokio::time::timeout(self.timeout, async {
            let (commands, status) = tokio::join!(parse, child.wait());
            (commands, status)
        })
        .await;

        match result {
            Ok((commands, Ok(status))) => Ok(ScriptOutcome {
                exit_code: status.code().unwrap_or(-1),
                commands,
            }),
            Ok((_, Err(e))) => Err(PostError::Subprocess(format!("{script}: {e}"))),
            Err(_) => {
                let _ = child.kill().await;
                Err(PostError::Subprocess(format!("{script}: timed out")))
            }
        }
    }
}

/// Discover post-processing scripts: executables in `dir` carrying the
/// legacy `### NZBGET POST-PROCESSING SCRIPT ###` header, or v2 extension
/// directories with a `manifest.json` naming a `main` entry.
pub fn discover(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    let mut paths: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
    paths.sort();
    for p in paths {
        if p.is_dir() {
            let manifest = p.join("manifest.json");
            if let Ok(bytes) = std::fs::read(&manifest) {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    if let Some(main) = v["main"].as_str() {
                        let entry = p.join(main);
                        if entry.is_file() {
                            out.push(entry);
                        }
                    }
                }
            }
        } else if p.is_file() {
            if let Ok(head) = std::fs::read_to_string(&p) {
                if head.contains("### NZBGET POST-PROCESSING SCRIPT ###") {
                    out.push(p);
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn write_script(path: &Path, body: &str) {
        std::fs::write(path, body).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[tokio::test]
    async fn protocol_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("notify.sh");
        write_script(
            &script,
            "#!/bin/sh\n\
             ### NZBGET POST-PROCESSING SCRIPT ###\n\
             echo \"[INFO] hello from $NZBPP_NZBNAME\"\n\
             echo \"[NZB] MARK=yes\"\n\
             echo \"[NZB] FINALDIR=$NZBPP_DIRECTORY/moved\"\n\
             exit 93\n",
        );
        let host = ScriptHost {
            timeout: Duration::from_secs(10),
        };
        let out = host
            .run(
                &script,
                tmp.path(),
                &[
                    ("NZBPP_NZBNAME".into(), "myjob".into()),
                    ("NZBPP_DIRECTORY".into(), "/dest/myjob".into()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(out.exit_code, crate::script_exit::SUCCESS);
        assert_eq!(
            out.commands,
            vec![
                ("MARK".to_string(), "yes".to_string()),
                ("FINALDIR".to_string(), "/dest/myjob/moved".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn exit_codes_pass_through() {
        let tmp = tempfile::tempdir().unwrap();
        for (code, name) in [(94, "err.sh"), (95, "skip.sh"), (92, "parreq.sh")] {
            let p = tmp.path().join(name);
            write_script(&p, &format!("#!/bin/sh\nexit {code}\n"));
            let host = ScriptHost {
                timeout: Duration::from_secs(5),
            };
            let out = host.run(&p, tmp.path(), &[]).await.unwrap();
            assert_eq!(out.exit_code, code);
        }
    }

    #[test]
    fn discovery_legacy_and_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        write_script(
            &tmp.path().join("legacy.sh"),
            "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\nexit 93\n",
        );
        std::fs::write(tmp.path().join("not-a-script.txt"), "hello").unwrap();
        let ext = tmp.path().join("myext");
        std::fs::create_dir(&ext).unwrap();
        std::fs::write(
            ext.join("manifest.json"),
            r#"{"main": "main.sh", "name": "MyExt"}"#,
        )
        .unwrap();
        write_script(&ext.join("main.sh"), "#!/bin/sh\nexit 93\n");

        let found = discover(tmp.path());
        let names: Vec<String> = found
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["legacy.sh", "main.sh"]);
    }
}
