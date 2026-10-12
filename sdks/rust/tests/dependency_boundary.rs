use std::process::Command;

#[test]
fn the_sdk_does_not_pull_in_server_side_crates() {
    let output = Command::new(env!("CARGO"))
        .args([
            "tree",
            "--offline",
            "--package",
            "datalens-sdk",
            "--edges",
            "normal,build",
            "--prefix",
            "none",
            "--format",
            "{p}",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("run cargo tree");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let tree = String::from_utf8(output.stdout).unwrap();
    let packages: Vec<&str> = tree
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .collect();
    assert!(packages.contains(&"datalens-core"), "{tree}");
    for forbidden in [
        "sqlx",
        "sqlx-core",
        "axum",
        "async-graphql",
        "datalens-store",
        "datalens-engine",
        "datalens-api",
    ] {
        assert!(!packages.contains(&forbidden), "{forbidden} in\n{tree}");
    }
}
