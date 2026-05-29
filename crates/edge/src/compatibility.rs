pub trait CompatibilityAdapter {
    fn name(&self) -> &'static str;
}

#[derive(Clone, Debug, Default)]
pub struct NativeCompatibility;

impl CompatibilityAdapter for NativeCompatibility {
    fn name(&self) -> &'static str {
        "native"
    }
}
