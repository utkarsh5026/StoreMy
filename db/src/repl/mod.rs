//! Interactive read-eval-print loop for the `StoreMy` CLI.
//!
//! ## Module layout
//!
//! - [`helper`] — rustyline glue: validator, highlighter, hinter, completer.
//! - [`meta`]   — backslash meta-commands (`\help`, `\timing`, …).
//! - [`render`] — pretty-printing of statement results and errors.
//! - [`state`]  — mutable per-session flags shared across the modules above.
//! - [`theme`]  — single source of truth for colors, glyphs, and banner text.
//!
//! Public surface is just [`run`] — call it from `main`.

mod helper;
mod meta;
mod render;
mod state;
mod theme;

use std::path::Path;

use rustyline::{Config, Editor, error::ReadlineError, history::DefaultHistory};

use crate::{
    database::Database,
    repl::{
        helper::ReplHelper,
        meta::{MetaOutcome, handle_meta},
        render::execute_and_print,
        state::ReplState,
    },
};

/// Runs the REPL until EOF, an unrecoverable input error, or `\quit`.
///
/// `buffer_pages` is shown in the banner only — it's a display value, not a
/// runtime knob.
pub fn run(db: &Database, history_path: &Path, data_dir: &Path, buffer_pages: usize) {
    theme::print_banner(buffer_pages, data_dir);

    let config = Config::builder()
        .auto_add_history(false) // we add entries ourselves so meta-commands don't pollute
        .history_ignore_dups(true)
        .unwrap_or_else(|e| panic!("invalid rustyline config: {e}"))
        .build();

    let mut rl: Editor<ReplHelper, DefaultHistory> = match Editor::with_config(config) {
        Ok(rl) => rl,
        Err(e) => {
            theme::fatal(&format!("failed to initialize line editor: {e}"));
            std::process::exit(1);
        }
    };
    rl.set_helper(Some(ReplHelper::new()));
    let _ = rl.load_history(history_path);

    let mut state = ReplState::default();

    loop {
        let prompt = theme::prompt(&state);
        match rl.readline(&prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                if trimmed.starts_with('\\') {
                    let _ = rl.add_history_entry(trimmed);
                    match handle_meta(trimmed, &mut state, db) {
                        MetaOutcome::Continue => continue,
                        MetaOutcome::Quit => break,
                    }
                }

                let _ = rl.add_history_entry(line.as_str());
                execute_and_print(db, &line, &state);
            }
            Err(ReadlineError::Interrupted) => {
                println!("{}", theme::dim("(canceled)"));
            }
            Err(ReadlineError::Eof) => break,
            Err(e) => {
                theme::error(&format!("input error: {e}"));
                break;
            }
        }
    }

    let _ = rl.save_history(history_path);
}

/// Entry point for one-shot execution (`storemy "SELECT 1;"`). Lives here so
/// `main.rs` only deals with arg parsing and database setup.
#[tracing::instrument(name = "one_shot", skip(db), fields(sql = %truncate(sql, 200)))]
pub fn execute_one_shot(db: &Database, sql: &str) {
    let state = ReplState {
        show_timing: false,
        explain: false,
    };
    execute_and_print(db, sql, &state);
}

/// Truncate a string to at most `max` chars for use in span fields.
///
/// The full SQL is logged at the boundary; the span field is just an identifier
/// readers scan visually, so we cap it to keep span output one-line-friendly.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let cut = s
            .char_indices()
            .map(|(i, _)| i)
            .take_while(|&i| i <= max)
            .last()
            .unwrap_or(0);
        format!("{}…", &s[..cut])
    }
}
