//! Output rendering: result tables, status lines, error formatting.

use std::time::Instant;

use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_FULL};
use owo_colors::OwoColorize;

use crate::{
    Value,
    database::Database,
    engine::{EngineError, StatementResult},
    repl::{state::ReplState, theme},
    tuple::Tuple,
};

/// Submits `sql` to the engine, waits for the result, and pretty-prints it.
///
/// Timing is measured around the round-trip (send + execute + receive) so the
/// number reflects what the user actually waited for, not just engine work.
pub fn execute_and_print(db: &Database, sql: &str, state: &ReplState) {
    let start = Instant::now();
    let rx = db.execute(sql.to_string());
    let result = rx.recv();
    let elapsed = start.elapsed();

    match result {
        Ok(Ok(r)) => print_result(&r),
        Ok(Err(e)) => print_engine_error(&e),
        Err(e) => theme::error(&format!("worker disconnected: {e}")),
    }

    if state.show_timing {
        println!(
            "{} {}",
            "⏱".bright_blue(),
            format!("{elapsed:.2?}").dimmed()
        );
    }
    println!();
}

fn print_result(r: &StatementResult) {
    match r {
        StatementResult::Selected { table, rows } => print_select(table, rows),
        StatementResult::Inserted { table, rows } => {
            theme::ok(
                "INSERT",
                &format!("{rows} {} into {}", row_word(*rows), table.cyan()),
            );
        }
        StatementResult::Deleted { table, rows } => {
            theme::ok(
                "DELETE",
                &format!("{rows} {} from {}", row_word(*rows), table.cyan()),
            );
        }
        StatementResult::Updated { table, rows } => {
            theme::ok(
                "UPDATE",
                &format!("{rows} {} in {}", row_word(*rows), table.cyan()),
            );
        }
        StatementResult::TableCreated {
            name,
            file_id,
            already_exists,
        } => {
            if *already_exists {
                theme::notice(
                    "NOTICE",
                    &format!("table {} already exists (file {file_id})", name.cyan()),
                );
            } else {
                theme::ok("CREATE TABLE", &format!("{} (file {file_id})", name.cyan()));
            }
        }
        StatementResult::TableDropped { name } => {
            theme::ok("DROP TABLE", &name.cyan().to_string());
        }
        StatementResult::IndexCreated {
            name,
            table,
            already_exists,
        } => {
            if *already_exists {
                theme::notice("NOTICE", &format!("index {} already exists", name.cyan()));
            } else {
                theme::ok(
                    "CREATE INDEX",
                    &format!("{} on {}", name.cyan(), table.cyan()),
                );
            }
        }
        StatementResult::IndexDropped { name } => {
            theme::ok("DROP INDEX", &name.cyan().to_string());
        }
        StatementResult::IndexesShown { scope, rows } => {
            print_indexes(scope.as_deref(), rows);
        }
    }
}

fn print_indexes(scope: Option<&str>, rows: &[crate::engine::ShownIndex]) {
    if rows.is_empty() {
        let label = scope.map_or_else(
            || "(no indexes)".to_string(),
            |t| format!("(no indexes on {t})"),
        );
        println!("{}", label.dimmed());
        return;
    }

    let mut t = Table::new();
    t.load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic);
    t.set_header(
        ["name", "table", "columns", "kind"]
            .iter()
            .map(|h| Cell::new(h).fg(comfy_table::Color::Cyan)),
    );

    for r in rows {
        let kind_label: &'static str = r.kind.into();
        t.add_row([
            Cell::new(&r.name).fg(comfy_table::Color::Green),
            Cell::new(&r.table).fg(comfy_table::Color::Green),
            Cell::new(r.columns.join(", ")),
            Cell::new(kind_label).fg(comfy_table::Color::Magenta),
        ]);
    }

    println!("{t}");
    let suffix = scope.map_or_else(
        || format!("({} indexes)", rows.len()),
        |s| format!("({} indexes on {s})", rows.len()),
    );
    println!("{}", suffix.dimmed());
}

fn print_engine_error(e: &EngineError) {
    eprintln!("{} {e}", "ERROR:".red().bold());
}

fn print_select(table_name: &str, rows: &[Tuple]) {
    if rows.is_empty() {
        println!("{}", format!("(0 rows from {table_name})").dimmed());
        return;
    }

    let width = rows.iter().map(Tuple::len).max().unwrap_or(0);

    let mut t = Table::new();
    t.load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic);
    t.set_header((0..width).map(|i| Cell::new(format!("col{i}")).fg(comfy_table::Color::Cyan)));

    for tup in rows {
        let cells = (0..width).map(|i| match tup.get(i) {
            Some(Value::Null) | None => Cell::new("NULL").fg(comfy_table::Color::DarkGrey),
            Some(v) => format_value_cell(v),
        });
        t.add_row(cells);
    }

    println!("{t}");
    println!(
        "{}",
        format!(
            "({} {} from {table_name})",
            rows.len(),
            row_word(rows.len())
        )
        .dimmed()
    );
}

/// Format a single value with type-aware coloring inside a table cell.
fn format_value_cell(v: &Value) -> Cell {
    match v {
        Value::Null => Cell::new("NULL").fg(comfy_table::Color::DarkGrey),
        Value::Int32(_)
        | Value::Int64(_)
        | Value::Uint32(_)
        | Value::Uint64(_)
        | Value::Float64(_) => Cell::new(v.to_string()).fg(comfy_table::Color::Yellow),
        Value::String(_) => Cell::new(v.to_string()).fg(comfy_table::Color::Green),
        Value::Bool(_) => Cell::new(v.to_string()).fg(comfy_table::Color::Magenta),
    }
}

fn row_word(n: usize) -> &'static str {
    if n == 1 { "row" } else { "rows" }
}
