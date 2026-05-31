use std::collections::BTreeSet;

#[test]
fn test_datalens_sdk_does_not_depend_on_internal_workspace_crates() {
    let manifest: toml::Value =
        toml::from_str(include_str!("../Cargo.toml")).expect("sdk Cargo.toml");
    let mut internal = BTreeSet::new();

    for table_name in ["dependencies", "dev-dependencies", "build-dependencies"] {
        let Some(table) = manifest.get(table_name).and_then(toml::Value::as_table) else {
            continue;
        };
        for (name, dependency) in table {
            if name.starts_with("datalens-") && name != "datalens-sdk" {
                internal.insert(name.clone());
            }
            if dependency
                .get("path")
                .and_then(toml::Value::as_str)
                .is_some_and(|path| path.contains("../../crates") || path.contains("../crates"))
            {
                internal.insert(name.clone());
            }
        }
    }

    assert!(
        internal.is_empty(),
        "datalens-sdk must not depend on internal datalens crates: {internal:?}"
    );
}
