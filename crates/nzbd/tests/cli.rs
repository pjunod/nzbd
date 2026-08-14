use std::process::Command;

#[test]
fn import_config_writes_the_conversion_and_reports_every_disposition() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("nzbget.conf");
    let output = tmp.path().join("nzbd.toml");
    std::fs::write(
        &source,
        "MainDir=/srv/downloads\n\
         DestDir=${MainDir}/complete\n\
         ControlPassword=secret\n\
         Server1.Name=hostless\n\
         Server1.Connections=4\n\
         FutureOption=review-me\n",
    )
    .unwrap();

    let result = Command::new(env!("CARGO_BIN_EXE_nzbd"))
        .args(["import-config", source.to_str().unwrap(), "--out"])
        .arg(&output)
        .output()
        .unwrap();
    let stdout = String::from_utf8(result.stdout).unwrap();
    let stderr = String::from_utf8(result.stderr).unwrap();
    assert!(result.status.success(), "stdout={stdout}\nstderr={stderr}");
    assert!(
        stdout.contains(&format!("wrote {}", output.display())),
        "{stdout}"
    );
    assert!(
        stdout.contains("mapped ") && stdout.contains("skipped 1"),
        "{stdout}"
    );
    assert!(stdout.contains("warning:"), "{stdout}");
    assert!(stdout.contains("review by hand: FutureOption"), "{stdout}");

    let converted = std::fs::read_to_string(output).unwrap();
    let cfg = nzbd_config::Config::from_toml(&converted).unwrap();
    assert_eq!(
        cfg.paths.main_dir,
        std::path::PathBuf::from("/srv/downloads")
    );
    assert_eq!(
        cfg.paths.dest_dir,
        std::path::PathBuf::from("/srv/downloads/complete")
    );
    assert!(cfg.servers.is_empty(), "hostless servers stay disabled");
}
