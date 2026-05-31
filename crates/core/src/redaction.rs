pub fn redact_url(value: &str) -> String {
    let Some((scheme, rest)) = value.split_once("://") else {
        return "<redacted>".to_owned();
    };
    let (without_fragment, fragment) = rest
        .split_once('#')
        .map(|(value, fragment)| (value, Some(fragment)))
        .unwrap_or((rest, None));
    let (without_query, query) = without_fragment
        .split_once('?')
        .map(|(value, query)| (value, Some(query)))
        .unwrap_or((without_fragment, None));
    let (authority, path) = without_query
        .split_once('/')
        .map(|(authority, path)| (authority, Some(path)))
        .unwrap_or((without_query, None));
    let authority = authority
        .rsplit_once('@')
        .map(|(_, host)| format!("<redacted>@{host}"))
        .unwrap_or_else(|| authority.to_owned());

    let mut output = format!("{scheme}://{authority}");
    if let Some(path) = path {
        output.push('/');
        output.push_str(path);
    }
    if let Some(query) = query {
        output.push('?');
        output.push_str(&redact_query(query));
    }
    if let Some(fragment) = fragment {
        output.push('#');
        output.push_str(fragment);
    }
    output
}

pub fn redact_urls_in_text(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut index = 0;
    while let Some(offset) = find_url_start(&value[index..]) {
        let start = index + offset;
        output.push_str(&value[index..start]);
        let end = url_end(value, start);
        let (url, suffix) = trim_url_suffix(&value[start..end]);
        output.push_str(&redact_url(url));
        output.push_str(suffix);
        index = end;
    }
    output.push_str(&value[index..]);
    output
}

fn find_url_start(value: &str) -> Option<usize> {
    match (value.find("http://"), value.find("https://")) {
        (Some(http), Some(https)) => Some(http.min(https)),
        (Some(http), None) => Some(http),
        (None, Some(https)) => Some(https),
        (None, None) => None,
    }
}

fn url_end(value: &str, start: usize) -> usize {
    value[start..]
        .char_indices()
        .find_map(|(offset, character)| {
            if character.is_ascii_whitespace()
                || matches!(character, '"' | '\'' | '<' | '>' | '{' | '}')
            {
                Some(start + offset)
            } else {
                None
            }
        })
        .unwrap_or(value.len())
}

fn trim_url_suffix(value: &str) -> (&str, &str) {
    let trim_len = value
        .chars()
        .rev()
        .take_while(|character| matches!(character, ')' | ']' | ',' | ';' | '.'))
        .map(char::len_utf8)
        .sum::<usize>();
    if trim_len == value.len() {
        (value, "")
    } else {
        value.split_at(value.len() - trim_len)
    }
}

fn redact_query(query: &str) -> String {
    query
        .split('&')
        .map(|parameter| {
            let (name, separator, value) = parameter
                .split_once('=')
                .map(|(name, value)| (name, "=", value))
                .unwrap_or((parameter, "", ""));
            if is_sensitive_query_parameter(name) {
                format!("{name}=<redacted>")
            } else {
                format!("{name}{separator}{value}")
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn is_sensitive_query_parameter(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "token"
            | "access_token"
            | "api_key"
            | "apikey"
            | "key"
            | "secret"
            | "password"
            | "signature"
    )
}
