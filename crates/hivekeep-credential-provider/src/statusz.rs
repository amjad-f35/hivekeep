//! The `/statusz` debug page: the configuration the running process holds,
//! notably the authorization policy loaded at startup (which the ConfigMap
//! on disk may since have drifted from). HTML by default, JSON with
//! `?format=json` or `Accept: application/json`. Names and configuration
//! only, never secret values.

use std::collections::BTreeMap;
use std::fmt::Write;

use serde::{Deserialize, Serialize};

use crate::http::{not_found, response, HttpResponse};
use crate::provider::KubeSecretProvider;
use crate::secret_uri::PROVIDER_NAME;

/// The page's data. The JSON form is what tests and tooling read; the HTML
/// form renders the same fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusContext {
    /// Build version.
    pub version: String,
    /// The URI host served.
    pub provider_name: String,
    /// The command line.
    pub args: String,
    /// Every flag with its effective value.
    pub flags: BTreeMap<String, String>,
    /// The SPIFFE ID callers must present.
    pub injector_san: String,
    /// hive → namespaces; `None` when authorization is disabled.
    pub policy_grants: Option<BTreeMap<String, Vec<String>>>,
}

/// Serves `/statusz` over a provider and the process configuration.
#[derive(Clone)]
pub struct StatusPage {
    provider: KubeSecretProvider,
    args: String,
    flags: BTreeMap<String, String>,
    injector_san: String,
}

impl StatusPage {
    /// A page for `provider`, showing the given command line and flags.
    pub fn new(
        provider: KubeSecretProvider,
        args: String,
        flags: BTreeMap<String, String>,
        injector_san: String,
    ) -> Self {
        StatusPage {
            provider,
            args,
            flags,
            injector_san,
        }
    }

    /// The current page data.
    pub fn context(&self) -> StatusContext {
        StatusContext {
            version: env!("CARGO_PKG_VERSION").to_string(),
            provider_name: PROVIDER_NAME.to_string(),
            args: self.args.clone(),
            flags: self.flags.clone(),
            injector_san: self.injector_san.clone(),
            policy_grants: self.provider.grants(),
        }
    }

    /// Handles one request.
    pub fn handle(&self, req: &http::request::Parts) -> HttpResponse {
        if req.uri.path() != "/statusz" {
            return not_found();
        }
        let ctx = self.context();
        let wants_json = req
            .headers
            .get(http::header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains("application/json"))
            || req
                .uri
                .query()
                .is_some_and(|q| q.split('&').any(|kv| kv == "format=json"));
        if wants_json {
            let mut body = serde_json::to_vec(&ctx).unwrap_or_default();
            body.push(b'\n');
            return response(http::StatusCode::OK, "application/json", body);
        }
        response(
            http::StatusCode::OK,
            "text/html; charset=utf-8",
            render_html(&ctx),
        )
    }
}

/// Escapes text for HTML element content and attribute values.
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

const HEAD: &str = r#"<!DOCTYPE html>
<html>
<head><title>hivekeep-credential-provider statusz</title>
<style>
body { font-family: monospace; margin: 2em; }
table { border-collapse: collapse; margin-bottom: 2em; }
th, td { border: 1px solid #999; padding: 4px 8px; text-align: left; }
th { background: #eee; }
</style>
</head>
<body>
<h1>hivekeep-credential-provider</h1>
"#;

/// Renders the HTML page.
pub fn render_html(ctx: &StatusContext) -> String {
    let mut h = String::from(HEAD);
    let _ = write!(
        h,
        "<p>version: {}<br>\nprovider: {}<br>\nrequired caller SAN: {}<br>\nargs: {}</p>\n\n",
        escape(&ctx.version),
        escape(&ctx.provider_name),
        escape(&ctx.injector_san),
        escape(&ctx.args)
    );
    h.push_str("<h2>Namespace policy (as loaded at startup)</h2>\n");
    match &ctx.policy_grants {
        Some(grants) if !grants.is_empty() => {
            h.push_str("<table>\n<tr><th>hive</th><th>allowed namespaces</th></tr>\n");
            for (hive, namespaces) in grants {
                let list: Vec<String> = namespaces.iter().map(|n| escape(n)).collect();
                let _ = writeln!(
                    h,
                    "<tr><td>{}</td><td>{}</td></tr>",
                    escape(hive),
                    list.join(" ")
                );
            }
            h.push_str("</table>\n");
        }
        Some(_) => h.push_str("<p>no hive is granted any namespace.</p>\n"),
        None => h.push_str(
            "<p><b>authorization disabled</b> — every namespace is allowed (dev only).</p>\n",
        ),
    }
    h.push_str("\n<h2>Flags</h2>\n<table>\n<tr><th>flag</th><th>value</th></tr>\n");
    for (name, value) in &ctx.flags {
        let _ = writeln!(
            h,
            "<tr><td>{}</td><td>{}</td></tr>",
            escape(name),
            escape(value)
        );
    }
    h.push_str("</table>\n</body>\n</html>\n");
    h
}

#[cfg(test)]
#[path = "statusz_tests.rs"]
mod tests;
