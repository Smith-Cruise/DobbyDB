use std::borrow::Cow;

use datafusion_cli::highlighter::SyntaxHighlighter;
use datafusion::common::config::Dialect;
use rustyline::completion::Completer;
use rustyline::highlight::{CmdKind, Highlighter};
use rustyline::hint::{Hinter, HistoryHinter};
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Context, Helper, Result};

pub struct DobbyDbCliHelper {
    hinter: HistoryHinter,
    highlighter: SyntaxHighlighter,
}

impl DobbyDbCliHelper {
    pub fn new(dialect: &Dialect) -> Self {
        Self {
            hinter: HistoryHinter::new(),
            highlighter: SyntaxHighlighter::new(dialect),
        }
    }
}

impl Hinter for DobbyDbCliHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Option<String> {
        self.hinter.hint(line, pos, ctx)
    }
}

impl Highlighter for DobbyDbCliHelper {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        self.highlighter.highlight(line, pos)
    }

    fn highlight_char(&self, line: &str, pos: usize, kind: CmdKind) -> bool {
        self.highlighter.highlight_char(line, pos, kind)
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Cow::Owned(format!("\x1b[90m{hint}\x1b[0m"))
    }
}

impl Completer for DobbyDbCliHelper {
    type Candidate = String;

    fn complete(
        &self,
        _line: &str,
        _pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<String>)> {
        Ok((0, vec![]))
    }
}

impl Validator for DobbyDbCliHelper {
    fn validate(&self, _ctx: &mut ValidationContext<'_>) -> Result<ValidationResult> {
        Ok(ValidationResult::Valid(None))
    }
}

impl Helper for DobbyDbCliHelper {}
