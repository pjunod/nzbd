//! The example configs shipped in the repo must always parse through the
//! real validator (`deny_unknown_fields` means doc rot fails loudly here
//! instead of silently confusing a user).

use std::path::Path;

#[test]
fn shipped_example_configs_parse() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for rel in [
        "examples/docker-compose/nzbd.toml.example",
        "dev/nzbd.toml.example",
    ] {
        let path = root.join(rel);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        nzbd_config::Config::from_toml(&text)
            .unwrap_or_else(|e| panic!("{rel} does not parse: {e}"));
    }
}

/// The shipped Compose recipes must keep the config mount **writable**.
///
/// Regression (2026-07-25): `examples/docker-compose` delivered the config
/// through a Compose `configs:` entry — which Docker always mounts
/// read-only — so every save in the settings editor failed with
/// `Read-only file system (os error 30)`. The default deployment silently
/// disabled a headline feature. Deliberate read-only shapes (Kubernetes
/// ConfigMaps) are fine; the Docker happy path is not one of them.
#[test]
fn shipped_compose_files_mount_the_config_writable() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for rel in [
        "examples/docker-compose/docker-compose.yml",
        "dev/docker-compose.yml",
    ] {
        let path = root.join(rel);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));

        // Only look at real YAML, not the explanatory comments.
        let code: String = text
            .lines()
            .filter(|l| !l.trim_start().starts_with('#'))
            .collect::<Vec<_>>()
            .join("\n");

        let mount = code
            .lines()
            .find(|l| l.contains(":/etc/nzbd"))
            .unwrap_or_else(|| panic!("{rel}: no /etc/nzbd mount at all"));
        assert!(
            !mount.trim_end().ends_with(":ro"),
            "{rel}: config mounted read-only, the settings editor cannot save:\n  {mount}"
        );
        assert!(
            !mount.contains("nzbd.toml:/etc/nzbd"),
            "{rel}: mount the config DIRECTORY, not the file — Docker turns a \
             missing source file into a directory and the daemon won't boot:\n  {mount}"
        );
        assert!(
            !code.contains("configs:"),
            "{rel}: Compose `configs:` entries are always read-only; use a \
             read-write bind mount of the config directory instead"
        );
    }
}

/// The image must name its data volume, or boot-time config recovery is
/// blind.
///
/// Regression (2026-07-26): a container whose config directory was not a
/// mount lost `nzbd.toml` on every recreate and came back serving the
/// first-run wizard. Recovery reads the copy kept beside the state — but
/// with no config file, the only thing that can point it at the data
/// volume is `NZBD_MAIN_DIR`, set here. If the `VOLUME` and the env ever
/// disagree, recovery silently stops finding anything, so pin them
/// together.
#[test]
fn the_image_points_recovery_at_its_data_volume() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let text = std::fs::read_to_string(root.join("Dockerfile")).expect("read Dockerfile");

    let env_line = text
        .lines()
        .find(|l| {
            l.trim_start().starts_with("ENV ") && l.contains(nzbd_config::durable::MAIN_DIR_ENV)
        })
        .unwrap_or_else(|| {
            panic!(
                "the Dockerfile must set {} so recovery can find the mirror",
                nzbd_config::durable::MAIN_DIR_ENV
            )
        });
    let value = env_line
        .split('=')
        .nth(1)
        .map(|v| v.trim().trim_matches('"').to_string())
        .expect("ENV line has a value");
    assert!(
        text.lines()
            .any(|l| l.starts_with("VOLUME") && l.contains(&value)),
        "{} is {value}, but the Dockerfile declares no VOLUME there — \
         recovery would look on a directory that does not persist",
        nzbd_config::durable::MAIN_DIR_ENV
    );
}

/// A container build must be told which commit it is.
///
/// Regression (field report 2026-07-27: "the version at the bottom of the
/// main page has been the same for a hundred commits"). `.dockerignore`
/// excludes `.git` to keep the build context small, so the build script's
/// `git describe` finds nothing inside the image build and the daemon
/// falls back to the bare crate version — on the one machine whose build
/// anyone ever needs to identify. The commit therefore travels as a build
/// argument, and every link of that chain is pinned here: the Dockerfile
/// declares and forwards it, the Makefile fills it in, and any Compose
/// file that actually builds the image passes it through.
///
/// A build that skips it is not silently wrong — it reports
/// `<version>+unknown` — but the paths we ship should never need that.
#[test]
fn the_image_is_told_which_commit_it_is() {
    const ARG: &str = "NZBD_GIT_DESCRIBE";
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let read = |rel: &str| {
        std::fs::read_to_string(root.join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"))
    };
    // Uncommented lines only — a recipe in a comment builds nothing.
    let code = |text: &str| {
        text.lines()
            .filter(|l| !l.trim_start().starts_with('#'))
            .collect::<Vec<_>>()
            .join("\n")
    };

    let dockerfile = code(&read("Dockerfile"));
    assert!(
        dockerfile
            .lines()
            .any(|l| l.trim_start().starts_with("ARG ") && l.contains(ARG)),
        "the Dockerfile must declare `ARG {ARG}` — without it the image \
         cannot learn its own commit, because the context has no .git"
    );
    assert!(
        dockerfile
            .lines()
            .any(|l| l.trim_start().starts_with("ENV ") && l.contains(ARG)),
        "the Dockerfile declares `ARG {ARG}` but never forwards it as an \
         ENV, so the build script never sees it"
    );

    let makefile = read("Makefile");
    assert!(
        makefile.contains(&format!("--build-arg {ARG}=")),
        "the Makefile's image build must pass --build-arg {ARG}=…"
    );

    for rel in [
        "examples/docker-compose/docker-compose.yml",
        "dev/docker-compose.yml",
    ] {
        let yaml = code(&read(rel));
        if !yaml.lines().any(|l| l.trim_start().starts_with("build:")) {
            continue; // pulls a published image; nothing to stamp
        }
        assert!(
            yaml.contains(&format!("{ARG}:")),
            "{rel} builds the image but does not pass {ARG}, so it would \
             produce a container that cannot say which commit it is"
        );
    }
}

/// Coverage is release evidence, so a partial or failing suite must never
/// produce a green workflow or update the public badges.
///
/// Regression (2026-08-14): the instrumented command was piped through `tee`
/// without `pipefail`, which discarded Cargo's failing exit status. It also
/// stopped at the first failing target, then published partial measurements.
/// Keep the CI and local entry points pinned to the same fail-closed contract.
#[test]
fn coverage_measurement_fails_closed_and_runs_the_whole_workspace() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let workflow = std::fs::read_to_string(root.join(".github/workflows/coverage.yml"))
        .expect("read coverage workflow");
    let makefile = std::fs::read_to_string(root.join("Makefile")).expect("read Makefile");

    let instrumented_step = workflow
        .split_once("- name: run instrumented test suite")
        .map(|(_, rest)| rest)
        .and_then(|rest| rest.split_once("\n      - name:").map(|(step, _)| step))
        .expect("find the instrumented coverage step");
    assert!(
        instrumented_step.contains("set -euo pipefail"),
        "the instrumented step pipes Cargo through tee, so it must enable \
         pipefail or a failing suite reports success"
    );
    assert!(
        instrumented_step
            .lines()
            .any(|line| line.contains("cargo llvm-cov --workspace --no-fail-fast")),
        "the instrumented suite must use --no-fail-fast so every workspace \
         test target contributes evidence before the job fails"
    );

    let coverage_floors: Vec<_> = workflow
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("MINIMUM_LINE_COVERAGE:")
                .map(str::trim)
        })
        .collect();
    assert_eq!(
        coverage_floors.len(),
        1,
        "the minimum line coverage must have one reviewable workflow constant"
    );
    let coverage_floor: f64 = coverage_floors[0]
        .parse()
        .expect("MINIMUM_LINE_COVERAGE must be a numeric percentage");
    assert!(
        coverage_floor > 0.0 && coverage_floor <= 100.0,
        "MINIMUM_LINE_COVERAGE must be a percentage in (0, 100]"
    );

    let floor_step = workflow
        .split_once("- name: enforce minimum line coverage")
        .map(|(_, rest)| rest)
        .and_then(|rest| rest.split_once("\n      - name:").map(|(step, _)| step))
        .expect("find the minimum line-coverage gate");
    assert!(
        floor_step.contains("cargo llvm-cov report")
            && floor_step.contains("--fail-under-lines \"$MINIMUM_LINE_COVERAGE\""),
        "the coverage gate must pass the single workflow floor to cargo llvm-cov"
    );
    assert!(
        workflow.contains("if-no-files-found: error"),
        "the coverage artifact must fail loudly when lcov.info is missing"
    );
    assert!(
        workflow.contains(
            "if: success() && github.event_name == 'push' && github.ref == 'refs/heads/main'"
        ),
        "badge publication must be explicitly gated on the instrumented suite succeeding"
    );

    assert_eq!(
        makefile
            .matches("$(CARGO) llvm-cov --workspace --no-fail-fast")
            .count(),
        2,
        "both make coverage and make coverage-html must run every workspace \
         test target, matching CI"
    );
}
