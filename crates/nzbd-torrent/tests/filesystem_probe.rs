use std::io::ErrorKind;

#[derive(Debug, PartialEq, Eq)]
enum FilesystemNameBehavior {
    Aliased,
    Distinct,
    Unsupported(ErrorKind),
}

fn probe_filesystem_name_behavior(left: &str, right: &str) -> FilesystemNameBehavior {
    let root = tempfile::tempdir().unwrap();
    if let Err(error) = std::fs::create_dir(root.path().join(left)) {
        return FilesystemNameBehavior::Unsupported(error.kind());
    }
    match std::fs::create_dir(root.path().join(right)) {
        Ok(()) => FilesystemNameBehavior::Distinct,
        Err(error) if error.kind() == ErrorKind::AlreadyExists => FilesystemNameBehavior::Aliased,
        Err(error) => FilesystemNameBehavior::Unsupported(error.kind()),
    }
}

#[test]
fn rejected_portable_aliases_are_observed_without_assuming_host_behavior() {
    for (name, left, right) in [
        ("ascii_case", "CaseProbe", "caseprobe"),
        ("unicode_nfc_nfd", "Caf\u{e9}Probe", "Cafe\u{301}Probe"),
        ("compatibility_ligature", "ﬁleProbe", "fileProbe"),
        ("full_case_fold", "ßProbe", "SSProbe"),
    ] {
        let behavior = probe_filesystem_name_behavior(left, right);
        println!(
            "filesystem rejected-name probe: os={} pair={name} behavior={behavior:?}",
            std::env::consts::OS
        );
    }
}

#[test]
fn admitted_portable_names_must_remain_distinct_on_native_runner_volumes() {
    let behavior = probe_filesystem_name_behavior("ＦProbe", "fProbe");
    println!(
        "filesystem admitted-name probe: os={} pair=compatibility_width behavior={behavior:?}",
        std::env::consts::OS
    );
    assert_eq!(
        behavior,
        FilesystemNameBehavior::Distinct,
        "the adapter admits a compatibility-width pair that is not distinct on this volume"
    );
}
