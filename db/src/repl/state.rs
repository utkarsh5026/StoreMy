//! Mutable per-session state that meta-commands read and toggle.
//!
//! Kept in its own module so both `repl::meta` and `repl::render` can mutate
//! and observe it without circular dependencies on `repl::mod`.

#[derive(Debug)]
pub struct ReplState {
    /// Print elapsed time after every executed statement.
    pub show_timing: bool,
    /// Reserved for a future `\explain on` mode that wraps every query in
    /// `EXPLAIN`. Currently only changes the prompt color so the surface
    /// is in place.
    pub explain: bool,
}

impl Default for ReplState {
    fn default() -> Self {
        Self {
            show_timing: true,
            explain: false,
        }
    }
}
