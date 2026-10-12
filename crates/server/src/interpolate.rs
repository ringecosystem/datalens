use crate::config::ConfigError;

/// Replaces every `${NAME}` in string values. A `$` not followed by `{` is kept literally.
pub(crate) fn interpolate_value(
    value: &mut toml::Value,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<(), ConfigError> {
    match value {
        toml::Value::String(text) => *text = interpolate(text, env)?,
        toml::Value::Array(items) => {
            for item in items {
                interpolate_value(item, env)?;
            }
        }
        toml::Value::Table(table) => {
            for (_, item) in table.iter_mut() {
                interpolate_value(item, env)?;
            }
        }
        toml::Value::Integer(_)
        | toml::Value::Float(_)
        | toml::Value::Boolean(_)
        | toml::Value::Datetime(_) => {}
    }
    Ok(())
}

fn interpolate(text: &str, env: &dyn Fn(&str) -> Option<String>) -> Result<String, ConfigError> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or(ConfigError::UnterminatedPlaceholder)?;
        let name = &after[..end];
        let valid = !name.is_empty()
            && !name.starts_with(|c: char| c.is_ascii_digit())
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !valid {
            return Err(ConfigError::InvalidPlaceholder(name.to_owned()));
        }
        out.push_str(&env(name).ok_or_else(|| ConfigError::MissingEnv(name.to_owned()))?);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}
