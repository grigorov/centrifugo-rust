//! Origin allow-listing for the WS and SockJS transports, mirroring Go
//! centrifugo's `allowed_origins` (main.go:1226-1289 + internal/origin).
//!
//! Go semantics replicated here:
//! - `allowed_origins` NOT configured → allow every request (Go's default
//!   `CheckOrigin` returns true).
//! - configured → for each request, an EMPTY `Origin` header is allowed (Go's
//!   `PatternChecker.Check` returns nil on empty Origin); a non-empty Origin is
//!   lowercased and matched against the patterns (gobwas/glob), and rejected
//!   (HTTP 403 on the WS upgrade) when none match.
//! - configured to the single pattern `*` (or any pattern that matches all) →
//!   allow every request.
//!
//! Patterns support `*` (any run of characters) and `?` (any single character),
//! matched against the lowercased Origin — covering the realistic origin patterns
//! (`*`, `https://*.example.com`, exact hosts). Patterns are used as written (Go
//! lowercases only the Origin), so a lowercase config matches case-insensitively.
//! gobwas's brace alternation (`{a,b}`), character classes (`[a-z]`) and backslash
//! escaping are NOT supported: such a pattern is matched literally, so a real Origin
//! never matches it and the request is rejected (403) — fail-closed, never a bypass.
//! Use one explicit `*`/`?` allowlist entry per origin instead of brace/class syntax.

/// Decides whether a request's `Origin` header is allowed.
pub struct OriginChecker {
    /// `allowed_origins` not configured → every request is allowed.
    allow_all: bool,
    /// Configured glob patterns (empty when `allow_all`).
    patterns: Vec<String>,
}

impl OriginChecker {
    /// Build from the configured `allowed_origins`: `None` when the key is absent
    /// (allow all), `Some(list)` when configured (match each non-empty Origin).
    pub fn new(allowed: Option<Vec<String>>) -> Self {
        match allowed {
            None => OriginChecker {
                allow_all: true,
                patterns: Vec::new(),
            },
            Some(list) => OriginChecker {
                allow_all: false,
                patterns: list,
            },
        }
    }

    /// Whether a request bearing `origin` (the `Origin` header, if any) is allowed.
    pub fn check(&self, origin: Option<&str>) -> bool {
        if self.allow_all {
            return true;
        }
        let origin = match origin {
            Some(o) if !o.is_empty() => o,
            // Go: an empty/absent Origin is allowed (not a cross-origin browser request).
            _ => return true,
        };
        let lower = origin.to_ascii_lowercase();
        self.patterns
            .iter()
            .any(|p| glob_match(p.as_bytes(), lower.as_bytes()))
    }
}

/// Glob match supporting `*` (any run) and `?` (single char), comparing bytes
/// literally otherwise. Linear-time with backtracking on `*`.
fn glob_match(pat: &[u8], text: &[u8]) -> bool {
    let (mut p, mut t) = (0usize, 0usize);
    // Remember the last `*` position and where in `text` we last branched there.
    let mut star: Option<usize> = None;
    let mut mark = 0usize;
    while t < text.len() {
        if p < pat.len() && (pat[p] == b'?' || pat[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pat.len() && pat[p] == b'*' {
            star = Some(p);
            mark = t;
            p += 1;
        } else if let Some(sp) = star {
            // Backtrack: let the last `*` swallow one more char of `text`.
            p = sp + 1;
            mark += 1;
            t = mark;
        } else {
            return false;
        }
    }
    // Trailing `*`s match the empty remainder.
    while p < pat.len() && pat[p] == b'*' {
        p += 1;
    }
    p == pat.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checker(list: &[&str]) -> OriginChecker {
        OriginChecker::new(Some(list.iter().map(|s| s.to_string()).collect()))
    }

    #[test]
    fn not_configured_allows_everything() {
        let c = OriginChecker::new(None);
        assert!(c.check(Some("https://evil.example")));
        assert!(c.check(None));
        assert!(c.check(Some("")));
    }

    #[test]
    fn star_pattern_allows_everything() {
        let c = checker(&["*"]);
        assert!(c.check(Some("https://anything.example:8443")));
        assert!(c.check(None));
    }

    #[test]
    fn exact_origin_matches_only_itself() {
        let c = checker(&["https://good.example"]);
        assert!(c.check(Some("https://good.example")));
        assert!(!c.check(Some("https://evil.example")));
        // Empty/absent Origin is always allowed (Go semantics).
        assert!(c.check(None));
        assert!(c.check(Some("")));
    }

    #[test]
    fn matching_is_case_insensitive_on_origin() {
        let c = checker(&["https://good.example"]);
        // The Origin is lowercased before matching (Go: strings.ToLower).
        assert!(c.check(Some("https://GOOD.example")));
        assert!(c.check(Some("HTTPS://Good.Example")));
    }

    #[test]
    fn wildcard_subdomain_pattern() {
        let c = checker(&["https://*.example.com"]);
        assert!(c.check(Some("https://a.example.com")));
        assert!(c.check(Some("https://deep.sub.example.com")));
        assert!(!c.check(Some("https://example.com")));
        assert!(!c.check(Some("https://evil.com")));
    }

    #[test]
    fn empty_list_rejects_non_empty_origins() {
        // Go: `allowed_origins: []` is "configured" → a non-empty Origin matching
        // nothing is rejected, while an empty Origin is still allowed.
        let c = OriginChecker::new(Some(Vec::new()));
        assert!(!c.check(Some("https://anything.example")));
        assert!(c.check(None));
    }
}
