//! NZBGet-style feed filter language (documented subset).
//!
//! One rule per line; first matching Accept/Reject wins, Require rules must
//! ALL pass first. A line is `Accept(option:value, …): expression`,
//! `Reject: expression`, `Require: expression` (short forms `A:`/`R:`/`Q:`),
//! or a bare expression (= Accept). `#` starts a comment.
//!
//! An expression is space-separated terms, ALL of which must match (AND):
//! - `pattern` / `title:pattern` — wildcard (`*`, `?`) match on the title
//! - `category:pattern`, `url:pattern` — same, other fields
//! - `size:500MB-2GB`, `size:>4GB`, `size:<900MB` — decoded size window
//! - `age:>3d`, `age:<30d` — item age in days
//! - a leading `-` negates any term (`-title:*720p*`)
//!
//! Accept options carried onto the queued job: `category`, `priority`,
//! `pause` (yes/no), `dupekey`, `dupescore`.

use crate::parse::FeedItem;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MatchOptions {
    pub category: Option<String>,
    pub priority: Option<i32>,
    pub pause: Option<bool>,
    pub dupekey: Option<String>,
    pub dupescore: Option<i32>,
}

#[derive(Debug, Clone, PartialEq)]
enum Verb {
    Accept(MatchOptions),
    Reject,
    Require,
}

#[derive(Debug, Clone, PartialEq)]
enum Term {
    Text {
        field: Field,
        pattern: String,
        negate: bool,
    },
    Size {
        min: u64,
        max: u64,
        negate: bool,
    },
    Age {
        min: u32,
        max: u32,
        negate: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Field {
    Title,
    Category,
    Url,
}

#[derive(Debug, Clone)]
struct Rule {
    verb: Verb,
    terms: Vec<Term>,
}

#[derive(Debug, Clone, Default)]
pub struct Filter {
    rules: Vec<Rule>,
}

/// Case-insensitive wildcard match (`*` any run, `?` one char).
fn wild_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.to_lowercase().chars().collect();
    let t: Vec<char> = text.to_lowercase().chars().collect();
    // Iterative glob with backtracking.
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut mark) = (usize::MAX, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = pi;
            mark = ti;
            pi += 1;
        } else if star != usize::MAX {
            pi = star + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    let split = s.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(s.len());
    let num: f64 = s[..split].parse().ok()?;
    let mult = match s[split..].to_uppercase().as_str() {
        "" | "B" => 1.0,
        "K" | "KB" | "KIB" => 1024.0,
        "M" | "MB" | "MIB" => 1024.0 * 1024.0,
        "G" | "GB" | "GIB" => 1024.0 * 1024.0 * 1024.0,
        "T" | "TB" | "TIB" => 1024.0f64.powi(4),
        _ => return None,
    };
    Some((num * mult) as u64)
}

fn parse_age(s: &str) -> Option<u32> {
    let s = s.trim().trim_end_matches(['d', 'D']);
    s.parse().ok()
}

fn parse_term(raw: &str) -> Option<Term> {
    let (negate, raw) = match raw.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, raw),
    };
    let (field, value) = match raw.split_once(':') {
        Some((f, v)) => (f.to_lowercase(), v),
        None => ("title".into(), raw),
    };
    match field.as_str() {
        "title" | "filename" => Some(Term::Text {
            field: Field::Title,
            pattern: value.to_string(),
            negate,
        }),
        "category" => Some(Term::Text {
            field: Field::Category,
            pattern: value.to_string(),
            negate,
        }),
        "url" | "link" => Some(Term::Text {
            field: Field::Url,
            pattern: value.to_string(),
            negate,
        }),
        "size" => {
            let (min, max) = if let Some(v) = value.strip_prefix('>') {
                (parse_size(v)?, u64::MAX)
            } else if let Some(v) = value.strip_prefix('<') {
                (0, parse_size(v)?)
            } else if let Some((lo, hi)) = value.split_once('-') {
                (parse_size(lo)?, parse_size(hi)?)
            } else {
                let exact = parse_size(value)?;
                (exact, exact)
            };
            Some(Term::Size { min, max, negate })
        }
        "age" => {
            let (min, max) = if let Some(v) = value.strip_prefix('>') {
                (parse_age(v)?, u32::MAX)
            } else if let Some(v) = value.strip_prefix('<') {
                (0, parse_age(v)?)
            } else if let Some((lo, hi)) = value.split_once('-') {
                (parse_age(lo)?, parse_age(hi)?)
            } else {
                (parse_age(value)?, u32::MAX)
            };
            Some(Term::Age { min, max, negate })
        }
        _ => None, // unknown field: term ignored (logged at parse)
    }
}

fn parse_options(s: &str) -> MatchOptions {
    let mut o = MatchOptions::default();
    for part in s.split(',') {
        let Some((k, v)) = part.split_once(':') else {
            continue;
        };
        let (k, v) = (k.trim().to_lowercase(), v.trim());
        match k.as_str() {
            "category" => o.category = Some(v.to_string()),
            "priority" => o.priority = v.parse().ok(),
            "pause" => o.pause = Some(v.eq_ignore_ascii_case("yes") || v == "1"),
            "dupekey" => o.dupekey = Some(v.to_string()),
            "dupescore" => o.dupescore = v.parse().ok(),
            _ => {}
        }
    }
    o
}

impl Filter {
    /// Parse a filter script. Unknown fields inside expressions are dropped
    /// with a warning; malformed lines are skipped the same way.
    pub fn parse(text: &str) -> Filter {
        let mut rules = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (verb, expr) = if let Some(rest) = strip_verb(line, &["accept", "a"]) {
                let (opts, expr) = rest;
                (Verb::Accept(parse_options(&opts)), expr)
            } else if let Some((_, expr)) = strip_verb(line, &["reject", "r"]) {
                (Verb::Reject, expr)
            } else if let Some((_, expr)) = strip_verb(line, &["require", "q"]) {
                (Verb::Require, expr)
            } else {
                (Verb::Accept(MatchOptions::default()), line.to_string())
            };
            let terms: Vec<Term> = expr
                .split_whitespace()
                .filter_map(|t| {
                    let parsed = parse_term(t);
                    if parsed.is_none() {
                        tracing::warn!(term = t, "feed filter: unknown term ignored");
                    }
                    parsed
                })
                .collect();
            if !terms.is_empty() || matches!(verb, Verb::Accept(_)) {
                rules.push(Rule { verb, terms });
            }
        }
        Filter { rules }
    }

    /// `Some(options)` when the item should be downloaded.
    pub fn evaluate(&self, item: &FeedItem) -> Option<MatchOptions> {
        // All Require rules must pass.
        for r in &self.rules {
            if r.verb == Verb::Require && !terms_match(&r.terms, item) {
                return None;
            }
        }
        // First Accept/Reject whose expression matches wins.
        for r in &self.rules {
            match &r.verb {
                Verb::Accept(opts) if terms_match(&r.terms, item) => return Some(opts.clone()),
                Verb::Reject if terms_match(&r.terms, item) => return None,
                _ => {}
            }
        }
        // No Accept rules at all = everything passing Require is accepted.
        if !self.rules.iter().any(|r| matches!(r.verb, Verb::Accept(_))) {
            return Some(MatchOptions::default());
        }
        None
    }
}

/// `Verb(opts): expr` / `Verb: expr` → `(opts, expr)`.
fn strip_verb(line: &str, names: &[&str]) -> Option<(String, String)> {
    let lower = line.to_lowercase();
    for n in names {
        // With options: name(…):
        if lower.starts_with(&format!("{n}(")) {
            let close = line.find(')')?;
            let rest = line[close + 1..].trim_start();
            let rest = rest.strip_prefix(':')?;
            return Some((
                line[n.len() + 1..close].to_string(),
                rest.trim().to_string(),
            ));
        }
        // Plain: name:
        if let Some(rest) = lower.strip_prefix(*n) {
            if rest.starts_with(':') {
                return Some((String::new(), line[n.len() + 1..].trim().to_string()));
            }
        }
    }
    None
}

fn terms_match(terms: &[Term], item: &FeedItem) -> bool {
    terms.iter().all(|t| match t {
        Term::Text {
            field,
            pattern,
            negate,
        } => {
            let text = match field {
                Field::Title => &item.title,
                Field::Category => &item.category,
                Field::Url => &item.url,
            };
            wild_match(pattern, text) ^ negate
        }
        Term::Size { min, max, negate } => ((*min..=*max).contains(&item.size)) ^ negate,
        Term::Age { min, max, negate } => ((*min..=*max).contains(&item.age_days)) ^ negate,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(title: &str, category: &str, size: u64, age: u32) -> FeedItem {
        FeedItem {
            title: title.into(),
            url: "https://x/get.nzb".into(),
            guid: "g".into(),
            category: category.into(),
            size,
            age_days: age,
        }
    }

    #[test]
    fn wildcards() {
        assert!(wild_match("*1080p*", "Show.S01E01.1080p.WEB"));
        assert!(wild_match("show.*.web", "SHOW.S01E01.WEB"));
        assert!(!wild_match("*2160p*", "Show.1080p"));
        assert!(wild_match("s??e??", "S01E02"));
        assert!(wild_match("*", "anything"));
    }

    #[test]
    fn accept_reject_order_and_require() {
        let f = Filter::parse(
            "# only HD tv, never x265, sane sizes\n\
             Require: size:100MB-20GB\n\
             Reject: title:*x265*\n\
             Accept(category:tv, priority:50): title:*1080p* category:TV*\n",
        );
        let opts = f
            .evaluate(&item("Show.S01E01.1080p", "TV > HD", 2 << 30, 1))
            .expect("accepted");
        assert_eq!(opts.category.as_deref(), Some("tv"));
        assert_eq!(opts.priority, Some(50));

        // Reject wins on x265 even though the accept would match.
        assert!(f
            .evaluate(&item("Show.S01E01.1080p.x265", "TV", 2 << 30, 1))
            .is_none());
        // Require gate: too small.
        assert!(f
            .evaluate(&item("Show.S01E01.1080p", "TV", 10 << 20, 1))
            .is_none());
        // No accept match: not TV category.
        assert!(f
            .evaluate(&item("Movie.1080p", "Movies", 2 << 30, 1))
            .is_none());
    }

    #[test]
    fn size_age_and_negation() {
        let f = Filter::parse("Accept: size:>4GB -title:*CAM* age:<30d\n");
        assert!(f
            .evaluate(&item("Big.Movie.2026", "", 5 << 30, 3))
            .is_some());
        assert!(f.evaluate(&item("Big.Movie.CAM", "", 5 << 30, 3)).is_none());
        assert!(f.evaluate(&item("Big.Movie", "", 1 << 30, 3)).is_none());
        assert!(f.evaluate(&item("Old.Movie", "", 5 << 30, 90)).is_none());
    }

    #[test]
    fn short_forms_and_bare_expression() {
        let f = Filter::parse("R: *720p*\nA: *1080p*\n");
        assert!(f.evaluate(&item("x.1080p", "", 0, 0)).is_some());
        assert!(f.evaluate(&item("x.720p", "", 0, 0)).is_none());

        // A bare expression is an Accept.
        let f = Filter::parse("*WEB*\n");
        assert!(f.evaluate(&item("a.WEB.b", "", 0, 0)).is_some());
        assert!(f.evaluate(&item("a.BluRay.b", "", 0, 0)).is_none());

        // Empty filter accepts everything.
        let f = Filter::parse("");
        assert!(f.evaluate(&item("anything", "", 0, 0)).is_some());
    }

    #[test]
    fn size_units() {
        assert_eq!(parse_size("500MB"), Some(500 * 1024 * 1024));
        assert_eq!(parse_size("2GB"), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("1.5G"), Some((1.5 * 1024.0f64.powi(3)) as u64));
        assert_eq!(parse_size("nope"), None);
    }
}
