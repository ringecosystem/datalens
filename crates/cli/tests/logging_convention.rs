use std::{fs, path::Path};

#[test]
fn test_logging_dependencies_are_declared_in_expected_crates() {
    let workspace = read("Cargo.toml");
    assert!(workspace.contains("log = "));
    assert!(workspace.contains("tracing-log = "));
    assert!(workspace.contains("tracing-subscriber = "));

    assert_manifest_has_dependency("crates/edge/Cargo.toml", "log.workspace = true");
    assert_manifest_has_dependency("crates/storage/Cargo.toml", "log.workspace = true");
    assert_manifest_has_dependency("crates/adapters/evm/Cargo.toml", "log.workspace = true");
    assert_manifest_has_dependency("crates/cli/Cargo.toml", "log.workspace = true");
    assert_manifest_has_dependency("crates/cli/Cargo.toml", "tracing-log.workspace = true");
    assert_manifest_has_dependency(
        "crates/cli/Cargo.toml",
        "tracing-subscriber.workspace = true",
    );
}

#[test]
fn test_cli_owns_tracing_backed_log_output() {
    let cli = read("crates/cli/src/commands.rs");

    assert!(cli.contains("tracing_log::LogTracer"));
    assert!(cli.contains("tracing_subscriber::EnvFilter"));
    assert!(cli.contains("datalens=info"));
}

#[test]
fn test_library_code_uses_log_facade_without_initializing_tracing() {
    for path in [
        "crates/edge/src",
        "crates/storage/src",
        "crates/adapters/evm/src",
    ] {
        let source = read_rust_sources(path);
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
    fs::read_to_string(workspace_path(path.as_ref())).unwrap_or_else(|error| {
        panic!("read {}: {error}", workspace_path(path.as_ref()).display());
    })
}

fn read_rust_sources(path: impl AsRef<Path>) -> String {
    let mut source = String::new();
    collect_rust_sources(&workspace_path(path.as_ref()), &mut source);
    source
}

fn collect_rust_sources(path: &Path, source: &mut String) {
    if path.is_file() {
        if path.extension().is_some_and(|extension| extension == "rs") {
            source.push_str(&fs::read_to_string(path).unwrap_or_else(|error| {
                panic!("read {}: {error}", path.display());
            }));
        }
        return;
    }

    let entries = fs::read_dir(path).unwrap_or_else(|error| {
        panic!("read dir {}: {error}", path.display());
    });
    for entry in entries {
        let entry = entry.unwrap_or_else(|error| {
            panic!("read dir entry {}: {error}", path.display());
        });
        collect_rust_sources(&entry.path(), source);
    }
}

fn workspace_path(path: &Path) -> std::path::PathBuf {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    workspace_root.join(path)
}
