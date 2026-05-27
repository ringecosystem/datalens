use std::{fs, path::Path};

#[test]
fn test_logging_dependencies_are_declared_in_expected_crates() {
    let workspace = read("Cargo.toml");
    assert!(workspace.contains("log = "));
    assert!(workspace.contains("tracing-log = "));
    assert!(workspace.contains("tracing-subscriber = "));

    assert_manifest_has_dependency("crates/api/Cargo.toml", "log.workspace = true");
    assert_manifest_has_dependency("crates/storage/Cargo.toml", "log.workspace = true");
    assert_manifest_has_dependency("crates/evm/Cargo.toml", "log.workspace = true");
    assert_manifest_has_dependency("crates/cli/Cargo.toml", "log.workspace = true");
    assert_manifest_has_dependency("crates/cli/Cargo.toml", "tracing-log.workspace = true");
    assert_manifest_has_dependency(
        "crates/cli/Cargo.toml",
        "tracing-subscriber.workspace = true",
    );
}

#[test]
fn test_cli_owns_tracing_backed_log_output() {
    let cli = read("crates/cli/src/lib.rs");

    assert!(cli.contains("tracing_log::LogTracer"));
    assert!(cli.contains("tracing_subscriber::EnvFilter"));
    assert!(cli.contains("datalens=info"));
}

#[test]
fn test_library_code_uses_log_facade_without_initializing_tracing() {
    for path in [
        "crates/api/src/lib.rs",
        "crates/storage/src/lib.rs",
        "crates/evm/src/lib.rs",
    ] {
        let source = read(path);
        assert!(
            source.contains("log::"),
            "{path} should use the log facade for runtime log calls"
        );
        assert!(
            !source.contains("tracing::"),
            "{path} should not use tracing macros directly"
        );
        assert!(
            !source.contains("#[tracing::"),
            "{path} should not use tracing instrumentation attributes"
        );
        assert!(
            !source.contains("tracing_subscriber"),
            "{path} should not initialize a tracing subscriber"
        );
        assert!(
            !source.contains("LogTracer"),
            "{path} should not initialize the log-to-tracing bridge"
        );
    }
}

fn assert_manifest_has_dependency(path: &str, dependency: &str) {
    let manifest = read(path);
    assert!(
        manifest.contains(dependency),
        "{path} should declare {dependency}"
    );
}

fn read(path: impl AsRef<Path>) -> String {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let full_path = workspace_root.join(path.as_ref());
    fs::read_to_string(&full_path).unwrap_or_else(|error| {
        panic!("read {}: {error}", full_path.display());
    })
}
