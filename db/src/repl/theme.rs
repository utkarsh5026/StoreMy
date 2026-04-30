use owo_colors::{OwoColorize, Style};

use crate::repl::state::ReplState;

const ACCENT: &str = "▍";

/// ANSI 256-color gradient used to fade the logo from cyan → blue → magenta.
/// Picked so it reads on both light and dark terminals.
const GRADIENT: [u8; 6] = [51, 45, 39, 33, 27, 93];

/// Big block-letter logo. Each row is colored with one step of `GRADIENT`,
/// producing a vertical fade. Kept under 64 cols so it fits an 80-col terminal
/// with room for a border.
const LOGO: [&str; 6] = [
    r"  ____  _                __  __        ",
    r" / ___|| |_ ___  _ __ ___|  \/  |_   _  ",
    r" \___ \| __/ _ \| '__/ _ \ |\/| | | | | ",
    r"  ___) | || (_) | | |  __/ |  | | |_| | ",
    r" |____/ \__\___/|_|  \___|_|  |_|\__, | ",
    r"                                 |___/  ",
];

const TAGLINE: &str = "a teaching database, written in Rust 🦀";

pub fn print_banner(buffer_pages: usize, data_dir: &std::path::Path) {
    println!(
        "{}",
        "╔══════════════════════════════════════════════════════════════╗".bright_blue()
    );

    for (row, line) in LOGO.iter().enumerate() {
        let color = GRADIENT[row.min(GRADIENT.len() - 1)];
        let styled = line.style(
            Style::new()
                .color(owo_colors::XtermColors::from(color))
                .bold(),
        );
        println!("{}  {}", "║".bright_blue(), styled);
    }

    println!("{}  {}", "║".bright_blue(), TAGLINE.italic().bright_white());
    println!(
        "{}",
        "╚══════════════════════════════════════════════════════════════╝".bright_blue()
    );
    println!();

    let sep = "│".bright_black();
    println!(
        "  {} {} {sep} {} {} pages {sep} {} {}",
        "engine".bright_black(),
        format!("v{}", env!("CARGO_PKG_VERSION"))
            .bright_white()
            .bold(),
        "buffer".bright_black(),
        buffer_pages.to_string().bright_cyan().bold(),
        "data".bright_black(),
        data_dir.display().to_string().bright_white(),
    );

    println!(
        "  {} {} {sep} {} {} {sep} {} {}",
        "rustc".bright_black(),
        option_env!("RUSTC_VERSION")
            .unwrap_or("stable")
            .bright_white(),
        "profile".bright_black(),
        if cfg!(debug_assertions) {
            "debug".yellow().to_string()
        } else {
            "release".bright_green().to_string()
        },
        "target".bright_black(),
        std::env::consts::OS.bright_white(),
    );
    println!();
    print_hint();
}

fn print_hint() {
    let bullet = "•".bright_blue();
    println!(
        "  {}  Type SQL ending in {}   {bullet}  {} for meta-commands   {bullet}  {} to quit",
        "›".bright_green().bold(),
        "`;`".bright_yellow(),
        "\\help".bright_yellow(),
        "Ctrl-D".bright_yellow(),
    );
    println!(
        "  {}  Try {} or {} to get started",
        "›".bright_green().bold(),
        "`\\tables`".bright_yellow(),
        "`\\explain on`".bright_yellow(),
    );
    println!();
}

/// Prompt rendered by rustyline before each input line. Colored cyan in normal
/// mode, magenta when `\explain on` is active so the user can tell at a glance
/// which session flag is set.
pub fn prompt(state: &ReplState) -> String {
    let marker = ACCENT;
    if state.explain {
        format!(
            "{} {} ",
            marker.bright_magenta().bold(),
            "storemy(explain)>".bright_magenta()
        )
    } else {
        format!(
            "{} {} ",
            marker.bright_cyan().bold(),
            "storemy>".bright_cyan()
        )
    }
}

/// Wrap `s` in ANSI dim styling. Used for non-essential annotations like
/// "(canceled)" or row counts.
pub fn dim(s: &str) -> String {
    s.dimmed().to_string()
}

/// Success line with a green check + bold label and a trailing message.
///
/// Used by [`render`](super::render) for outcomes like `INSERT 5 rows into t`.
pub fn ok(label: &str, msg: &str) {
    println!("{} {} {msg}", "✓".bright_green().bold(), label.bold());
}

/// Yellow advisory line — non-fatal, but worth the user's attention.
pub fn notice(label: &str, msg: &str) {
    println!("{} {} {msg}", "!".bright_yellow().bold(), label.bold());
}

/// Red error line on stderr. The process keeps running; for unrecoverable
/// startup failures use [`fatal`].
pub fn error(msg: &str) {
    eprintln!("{} {msg}", "ERROR:".red().bold());
}

/// Red fatal line on stderr. Caller is expected to exit immediately after.
pub fn fatal(msg: &str) {
    eprintln!("{} {msg}", "FATAL:".red().bold());
}
