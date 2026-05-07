//! Backslash meta-commands: REPL-only directives that never reach the parser.
//!
//! Anything that starts with `\` after trimming is dispatched here. Returning
//! [`MetaOutcome::Quit`] tells the main loop to exit gracefully; otherwise it
//! continues to the next prompt.

use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_FULL};
use owo_colors::OwoColorize;

use crate::{
    database::Database,
    primitives::ColumnId,
    repl::{state::ReplState, theme},
    tuple::TupleSchema,
};

pub enum MetaOutcome {
    Continue,
    Quit,
}

/// Dispatch table for `\foo` commands.
///
/// `input` is the raw line including the leading `\`. Trailing `;` is
/// tolerated so users can type `\help;` out of habit.
pub fn handle_meta(input: &str, state: &mut ReplState, db: &Database) -> MetaOutcome {
    let cmd = input.trim_end_matches(';').trim();
    let mut parts = cmd.split_whitespace();
    let head = parts.next().unwrap_or("");

    match head {
        r"\q" | r"\quit" | r"\exit" => return MetaOutcome::Quit,
        r"\help" | r"\?" => print_help(),
        r"\clear" => {
            // CSI 2J = clear screen, CSI H = move cursor to top-left.
            print!("\x1b[2J\x1b[H");
        }
        r"\timing" => toggle_flag("Timing", parts.next(), &mut state.show_timing),
        r"\explain" => toggle_flag("Explain mode", parts.next(), &mut state.explain),
        r"\status" => print_status(state),
        r"\dt" => print_table_list(db),
        r"\d" => match parts.next() {
            Some(name) => print_table_describe(db, name),
            None => print_table_list(db),
        },
        other => {
            theme::error(&format!("unknown meta-command `{other}`. Try `\\help`."));
        }
    }
    MetaOutcome::Continue
}

fn toggle_flag(name: &str, arg: Option<&str>, flag: &mut bool) {
    match arg {
        Some("on") => {
            *flag = true;
            println!("{}", format!("{name} is on.").dimmed());
        }
        Some("off") => {
            *flag = false;
            println!("{}", format!("{name} is off.").dimmed());
        }
        None => {
            *flag = !*flag;
            println!(
                "{}",
                format!("{name} is {}.", if *flag { "on" } else { "off" }).dimmed()
            );
        }
        Some(other) => {
            theme::notice(
                "warning:",
                &format!("expected `on` or `off`, got `{other}`"),
            );
        }
    }
}

fn print_status(state: &ReplState) {
    println!("{}", "Session state:".bold());
    println!(
        "  {:<14} {}",
        "timing".cyan(),
        if state.show_timing { "on" } else { "off" }.dimmed()
    );
    println!(
        "  {:<14} {}",
        "explain".cyan(),
        if state.explain { "on" } else { "off" }.dimmed()
    );
}

/// `\dt` — list every user table with shape and primary-key info.
fn print_table_list(db: &Database) {
    let tables = match db.list_user_tables() {
        Ok(t) => t,
        Err(e) => {
            theme::error(&format!("failed to list tables: {e}"));
            return;
        }
    };

    if tables.is_empty() {
        println!("{}", "(no user tables)".dimmed());
        return;
    }

    let mut t = Table::new();
    t.load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic);
    t.set_header(
        ["name", "file_id", "columns", "primary key"]
            .into_iter()
            .map(|h| Cell::new(h).fg(comfy_table::Color::Cyan)),
    );

    for info in &tables {
        let pk_text = primary_key_label(&info.schema, info.primary_key.as_deref());
        t.add_row(vec![
            Cell::new(&info.name).fg(comfy_table::Color::Green),
            Cell::new(info.file_id.0.to_string()).fg(comfy_table::Color::Yellow),
            Cell::new(info.schema.logical_num_fields().to_string()),
            match pk_text.as_deref() {
                Some(s) => Cell::new(s).fg(comfy_table::Color::Magenta),
                None => Cell::new("—").fg(comfy_table::Color::DarkGrey),
            },
        ]);
    }

    println!("{t}");
    println!(
        "{}",
        format!(
            "({} {})",
            tables.len(),
            if tables.len() == 1 { "table" } else { "tables" }
        )
        .dimmed()
    );
}

/// `\d <name>` — show columns, types, nullability, and PK marker.
fn print_table_describe(db: &Database, name: &str) {
    let info = match db.describe_table(name) {
        Ok(i) => i,
        Err(e) => {
            theme::error(&format!("{e}"));
            return;
        }
    };

    let pk_indices: &[ColumnId] = info.primary_key.as_deref().unwrap_or(&[]);

    println!(
        "{} {}  {}",
        "Table".bold(),
        info.name.green().bold(),
        format!("(file {}, {})", info.file_id.0, info.file_path.display()).dimmed(),
    );

    let mut t = Table::new();
    t.load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic);
    t.set_header(
        ["#", "column", "type", "nullable", "key"]
            .into_iter()
            .map(|h| Cell::new(h).fg(comfy_table::Color::Cyan)),
    );

    for (i, field) in info.schema.fields().enumerate() {
        let is_pk = pk_indices.iter().any(|col_id| usize::from(*col_id) == i);
        t.add_row(vec![
            Cell::new(i.to_string()).fg(comfy_table::Color::DarkGrey),
            Cell::new(&field.name).fg(comfy_table::Color::Green),
            Cell::new(format!("{:?}", field.field_type)).fg(comfy_table::Color::Yellow),
            if field.nullable {
                Cell::new("YES").fg(comfy_table::Color::DarkGrey)
            } else {
                Cell::new("NO").fg(comfy_table::Color::Red)
            },
            if is_pk {
                Cell::new("PK").fg(comfy_table::Color::Magenta)
            } else {
                Cell::new("")
            },
        ]);
    }

    println!("{t}");
}

/// Resolves the primary key indices into a comma-joined column-name string.
/// Returns `None` if the table has no primary key.
fn primary_key_label(schema: &TupleSchema, pk: Option<&[ColumnId]>) -> Option<String> {
    let indices = pk?;
    if indices.is_empty() {
        return None;
    }
    let names: Vec<String> = indices
        .iter()
        .filter_map(|&col_id| {
            schema
                .field(usize::from(col_id))
                .map(|f| f.name.as_str().to_owned())
        })
        .collect();
    if names.is_empty() {
        None
    } else {
        Some(names.join(", "))
    }
}

fn print_help() {
    let lines = [
        (r"\help, \?", "show this help"),
        (r"\q, \quit, \exit", "leave the REPL"),
        (r"\dt, \d", "list all user tables"),
        (r"\d <table>", "show columns, types, and PK for a table"),
        (r"\timing [on|off]", "toggle per-statement timing"),
        (r"\explain [on|off]", "toggle EXPLAIN-wrapping mode"),
        (r"\status", "show current session flags"),
        (r"\clear", "clear the screen"),
    ];
    println!("{}", "Meta-commands:".bold());
    for (cmd, desc) in lines {
        println!("  {:<22} {}", cmd.cyan(), desc.dimmed());
    }
    println!(
        "{}",
        "Anything else is treated as SQL — terminate with `;`.".dimmed()
    );
}
