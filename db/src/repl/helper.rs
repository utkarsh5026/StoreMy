//! Rustyline integration: validation, syntax highlighting, hints, completion.
//!
//! [`ReplHelper`] is the type rustyline calls back into on every keypress and
//! Enter. The [`Helper`] trait is just a marker that the type implements
//! [`Validator`] + [`Highlighter`] + [`Hinter`] + [`Completer`]; we provide
//! real impls for all four.

use std::borrow::Cow;

use owo_colors::OwoColorize;
use rustyline::{
    Context, Helper,
    completion::{Completer, Pair},
    highlight::Highlighter,
    hint::Hinter,
    validate::{ValidationContext, ValidationResult, Validator},
};

/// SQL keywords highlighted in the input line and offered for tab-completion.
///
/// Kept as `&[&str]` so the table is `'static` and we don't allocate per
/// keystroke. The list is intentionally short — it matches what the parser
/// actually accepts today, not the full SQL standard.
const KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "INSERT",
    "INTO",
    "VALUES",
    "UPDATE",
    "SET",
    "DELETE",
    "CREATE",
    "TABLE",
    "DROP",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "OUTER",
    "ON",
    "GROUP",
    "BY",
    "ORDER",
    "ASC",
    "DESC",
    "LIMIT",
    "OFFSET",
    "AND",
    "OR",
    "NOT",
    "NULL",
    "TRUE",
    "FALSE",
    "AS",
    "DISTINCT",
    "UNION",
    "INTERSECT",
    "EXCEPT",
    "PRIMARY",
    "KEY",
    "INT",
    "VARCHAR",
    "STRING",
    "BOOLEAN",
    "BOOL",
];

/// The single helper instance attached to the rustyline editor.
pub struct ReplHelper;

impl ReplHelper {
    pub fn new() -> Self {
        Self
    }
}

impl Helper for ReplHelper {}

impl Validator for ReplHelper {
    fn validate(&self, ctx: &mut ValidationContext<'_>) -> rustyline::Result<ValidationResult> {
        if statement_is_complete(ctx.input()) {
            Ok(ValidationResult::Valid(None))
        } else {
            Ok(ValidationResult::Incomplete)
        }
    }
}

/// Returns `true` when `input` contains an unquoted `;`, signalling end of
/// statement. Tracks single- and double-quoted string literals so a `;` inside
/// `'a;b'` does not end the statement.
pub(crate) fn statement_is_complete(input: &str) -> bool {
    let mut in_single = false;
    let mut in_double = false;
    let mut prev = '\0';

    for ch in input.chars() {
        match ch {
            '\'' if !in_double && prev != '\\' => in_single = !in_single,
            '"' if !in_single && prev != '\\' => in_double = !in_double,
            ';' if !in_single && !in_double => return true,
            _ => {}
        }
        prev = ch;
    }
    false
}

impl Highlighter for ReplHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        Cow::Owned(highlight_sql(line))
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> Cow<'b, str> {
        // The prompt is built in `theme.rs` and already contains ANSI escapes,
        // so we forward it untouched.
        Cow::Borrowed(prompt)
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Cow::Owned(hint.dimmed().to_string())
    }

    /// Tell rustyline to re-render on every keystroke so colors stay in sync
    /// with the buffer. Cheap because `highlight_sql` is a single linear scan.
    fn highlight_char(&self, _line: &str, _pos: usize, _forced: bool) -> bool {
        true
    }
}

/// A tiny tokenizer that recognises identifiers, numbers, single/double-quoted
/// strings, line comments (`-- …`), and punctuation, and wraps each in ANSI
/// color codes. This is *not* the real parser's lexer — it's a deliberately
/// permissive scanner so the highlighter never refuses to render bad input.
fn highlight_sql(line: &str) -> String {
    let mut out = String::with_capacity(line.len() + 32);
    let mut chars = line.chars().peekable();
    let mut buf = String::new();

    let flush_word = |buf: &mut String, out: &mut String| {
        if buf.is_empty() {
            return;
        }
        let upper = buf.to_ascii_uppercase();
        if KEYWORDS.contains(&upper.as_str()) {
            out.push_str(&buf.bright_magenta().bold().to_string());
        } else if buf.chars().all(|c| c.is_ascii_digit() || c == '.') {
            out.push_str(&buf.bright_yellow().to_string());
        } else {
            out.push_str(buf);
        }
        buf.clear();
    };

    while let Some(&ch) = chars.peek() {
        if ch == '-' {
            let mut peek = chars.clone();
            peek.next();
            if peek.peek() == Some(&'-') {
                flush_word(&mut buf, &mut out);
                let rest: String = chars.by_ref().collect();
                out.push_str(&rest.dimmed().italic().to_string());
                continue;
            }
        }

        match ch {
            '\'' | '"' => {
                flush_word(&mut buf, &mut out);
                let quote = ch;
                let mut s = String::new();
                s.push(quote);
                chars.next();
                while let Some(&c) = chars.peek() {
                    s.push(c);
                    chars.next();
                    if c == quote {
                        break;
                    }
                }
                out.push_str(&s.green().to_string());
            }
            c if c.is_alphanumeric() || c == '_' || c == '.' => {
                buf.push(c);
                chars.next();
            }
            other => {
                flush_word(&mut buf, &mut out);
                if matches!(other, ',' | '(' | ')' | ';' | '*') {
                    out.push_str(&other.to_string().bright_blue().to_string());
                } else {
                    out.push(other);
                }
                chars.next();
            }
        }
    }
    flush_word(&mut buf, &mut out);
    out
}

impl Hinter for ReplHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Option<String> {
        if line.is_empty() || pos < line.len() {
            return None;
        }
        let history = ctx.history();
        // Walk history newest-first; rustyline indexes oldest-first so we
        // iterate in reverse.
        for i in (0..history.len()).rev() {
            let Ok(Some(entry)) = history.get(i, rustyline::history::SearchDirection::Forward)
            else {
                continue;
            };
            if entry.entry.starts_with(line) && entry.entry.len() > line.len() {
                return Some(entry.entry[line.len()..].to_string());
            }
        }
        None
    }
}

impl Completer for ReplHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let start = line[..pos]
            .rfind(|c: char| !c.is_alphanumeric() && c != '_')
            .map_or(0, |i| i + 1);
        let prefix = &line[start..pos];
        if prefix.is_empty() {
            return Ok((start, vec![]));
        }
        let upper = prefix.to_ascii_uppercase();
        let lower_input = prefix.chars().next().is_some_and(char::is_lowercase);

        let candidates = KEYWORDS
            .iter()
            .filter(|kw| kw.starts_with(&upper))
            .map(|kw| {
                let display = (*kw).to_string();
                let replacement = if lower_input {
                    kw.to_ascii_lowercase()
                } else {
                    (*kw).to_string()
                };
                Pair {
                    display,
                    replacement,
                }
            })
            .collect();

        Ok((start, candidates))
    }
}

#[cfg(test)]
mod tests {
    use super::statement_is_complete;

    #[test]
    fn complete_when_unquoted_semicolon_present() {
        assert!(statement_is_complete("SELECT 1;"));
        assert!(statement_is_complete("INSERT INTO t VALUES ('a;b');\n"));
    }

    #[test]
    fn incomplete_when_no_semicolon_or_inside_string() {
        assert!(!statement_is_complete("SELECT 1"));
        assert!(!statement_is_complete("SELECT 'a;b"));
        assert!(!statement_is_complete("SELECT \"x;\nstill open"));
    }

    #[test]
    fn escaped_quote_does_not_flip_state() {
        assert!(statement_is_complete(r"SELECT '\'';"));
    }
}
