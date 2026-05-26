//! Transaction-control (TCL) statement parsers.
//!
//! Covers SQL that opens, commits, aborts, or partially rolls back an explicit
//! transaction, plus savepoint create/release. Each `parse_*` method consumes
//! its keyword sequence and returns the matching AST node from
//! [`crate::parser::statements`].
//!
//! # SQL coverage
//!
//! - `BEGIN [TRANSACTION]`
//! - `COMMIT [TRANSACTION]`
//! - `ROLLBACK [TRANSACTION]` — abort the whole transaction
//! - `ROLLBACK TO [SAVEPOINT] <name>` — undo work after a savepoint only
//! - `SAVEPOINT <name>`
//! - `RELEASE [SAVEPOINT] <name>`
//!
//! # Shape
//!
//! - [`Parser::parse_begin`] → [`BeginStatement`]
//! - [`Parser::parse_commit`] → [`CommitStatement`]
//! - [`Parser::parse_rollback`] → [`RollbackStatement`] (`Transaction` or `Savepoint`)
//! - [`Parser::parse_savepoint`] → [`SavepointStatement`]
//! - [`Parser::parse_release_savepoint`] → [`ReleaseSavepointStatement`]
//!
//! [`super::Parser::parse_one`] dispatches on the leading keyword and wraps
//! each result in [`Statement::Begin`], [`Statement::Commit`],
//! [`Statement::Rollback`], [`Statement::Savepoint`], or
//! [`Statement::ReleaseSavepoint`].
//!
//! # Fixed schema for examples
//!
//! ```sql
//! CREATE TABLE orders (order_id INT PRIMARY KEY, total INT);
//! ```
//!
//! Typical multi-statement flow:
//!
//! ```sql
//! BEGIN;
//! INSERT INTO orders VALUES (1, 100);
//! SAVEPOINT s1;
//! INSERT INTO orders VALUES (2, -50);
//! ROLLBACK TO SAVEPOINT s1;
//! INSERT INTO orders VALUES (2, 50);
//! COMMIT;
//! ```

use tracing::{debug, instrument};

use crate::parser::{
    Parser, ParserError,
    statements::{
        BeginStatement, CommitStatement, ReleaseSavepointStatement, RollbackStatement,
        SavepointStatement,
    },
    token::TokenType,
};

impl Parser {
    /// Parses `BEGIN [TRANSACTION]`.
    ///
    /// Opens an explicit transaction. The optional `TRANSACTION` keyword is
    /// stored on [`BeginStatement::transaction`] so [`Display`] can round-trip
    /// the surface syntax.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- BEGIN;
    /// --   BeginStatement { transaction: false }
    ///
    /// -- BEGIN TRANSACTION;
    /// --   BeginStatement { transaction: true }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::UnexpectedToken`] if the stream does not start
    /// with `BEGIN`, or [`ParserError::WantedToken`] if the input ends after
    /// `BEGIN`.
    #[instrument(
        skip(self),
        fields(component = "parser", statement = "begin"),
        err(Debug)
    )]
    pub(in crate::parser) fn parse_begin(&mut self) -> Result<BeginStatement, ParserError> {
        self.expect(TokenType::Begin)?;
        let transaction = self.if_peek_then_consume(TokenType::Transaction)?;
        debug!(transaction, "parsed BEGIN statement");
        Ok(BeginStatement { transaction })
    }

    /// Parses `COMMIT [TRANSACTION]`.
    ///
    /// Ends the current explicit transaction and makes its changes durable.
    /// The optional `TRANSACTION` keyword is stored on
    /// [`CommitStatement::transaction`].
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- COMMIT;
    /// --   CommitStatement { transaction: false }
    ///
    /// -- COMMIT TRANSACTION;
    /// --   CommitStatement { transaction: true }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::UnexpectedToken`] if the stream does not start
    /// with `COMMIT`, or [`ParserError::WantedToken`] if the input ends after
    /// `COMMIT`.
    #[instrument(
        skip(self),
        fields(component = "parser", statement = "commit"),
        err(Debug)
    )]
    pub(in crate::parser) fn parse_commit(&mut self) -> Result<CommitStatement, ParserError> {
        self.expect(TokenType::Commit)?;
        let transaction = self.if_peek_then_consume(TokenType::Transaction)?;
        debug!(transaction, "parsed COMMIT statement");
        Ok(CommitStatement { transaction })
    }

    /// Parses `ROLLBACK [TRANSACTION]` or `ROLLBACK TO [SAVEPOINT] <name>`.
    ///
    /// After consuming `ROLLBACK`, peeks for `TO` to choose the AST shape:
    ///
    /// - `TO` present → [`RollbackStatement::Savepoint`] (partial undo)
    /// - `TO` absent  → [`RollbackStatement::Transaction`] (full abort)
    ///
    /// The `SAVEPOINT` keyword after `TO` is optional in SQL and is accepted
    /// but not stored — both `ROLLBACK TO s1` and `ROLLBACK TO SAVEPOINT s1`
    /// produce the same [`RollbackStatement::Savepoint`] variant.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- ROLLBACK;
    /// --   RollbackStatement::Transaction { transaction: false }
    ///
    /// -- ROLLBACK TRANSACTION;
    /// --   RollbackStatement::Transaction { transaction: true }
    ///
    /// -- ROLLBACK TO SAVEPOINT s1;
    /// --   RollbackStatement::Savepoint { name: "s1" }
    ///
    /// -- ROLLBACK TO s1;
    /// --   RollbackStatement::Savepoint { name: "s1" }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::UnexpectedToken`] if `ROLLBACK` is missing or
    /// the savepoint name is not an identifier. Returns
    /// [`ParserError::WantedToken`] if the stream ends after `ROLLBACK` or
    /// `ROLLBACK TO` without a name.
    #[instrument(
        skip(self),
        fields(component = "parser", statement = "rollback"),
        err(Debug)
    )]
    pub(in crate::parser) fn parse_rollback(&mut self) -> Result<RollbackStatement, ParserError> {
        self.expect(TokenType::Rollback)?;

        if self.if_peek_then_consume(TokenType::To)? {
            self.if_peek_then_consume(TokenType::Savepoint)?;
            let name = self.expect_ident()?;
            debug!(
                savepoint = %name,
                rollback_kind = "savepoint",
                "parsed ROLLBACK statement"
            );
            Ok(RollbackStatement::Savepoint { name })
        } else {
            let transaction = self.if_peek_then_consume(TokenType::Transaction)?;
            debug!(
                transaction,
                rollback_kind = "transaction",
                "parsed ROLLBACK statement"
            );
            Ok(RollbackStatement::Transaction { transaction })
        }
    }

    /// Parses `SAVEPOINT <name>`.
    ///
    /// Records a rollback point inside the current transaction. Later
    /// `ROLLBACK TO [SAVEPOINT] <name>` undoes only work written after this
    /// statement.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SAVEPOINT s1;
    /// --   SavepointStatement { name: "s1" }
    ///
    /// -- SAVEPOINT before_bad_insert;
    /// --   SavepointStatement { name: "before_bad_insert" }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::UnexpectedToken`] if `SAVEPOINT` is missing or
    /// the name is not an identifier. Returns [`ParserError::WantedToken`] if
    /// the stream ends after `SAVEPOINT` without a name.
    #[instrument(
        skip(self),
        fields(component = "parser", statement = "savepoint"),
        err(Debug)
    )]
    pub(in crate::parser) fn parse_savepoint(&mut self) -> Result<SavepointStatement, ParserError> {
        self.expect(TokenType::Savepoint)?;
        let name = self.expect_ident()?;
        debug!(savepoint = %name, "parsed SAVEPOINT statement");
        Ok(SavepointStatement { name })
    }

    /// Parses `RELEASE [SAVEPOINT] <name>`.
    ///
    /// Discards a savepoint mark without rolling back any work. The optional
    /// `SAVEPOINT` keyword is accepted but not stored — both `RELEASE s1` and
    /// `RELEASE SAVEPOINT s1` produce the same AST.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- RELEASE SAVEPOINT s1;
    /// --   ReleaseSavepointStatement { name: "s1" }
    ///
    /// -- RELEASE s1;
    /// --   ReleaseSavepointStatement { name: "s1" }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::UnexpectedToken`] if `RELEASE` is missing or
    /// the name is not an identifier. Returns [`ParserError::WantedToken`] if
    /// the stream ends after `RELEASE` without a name.
    #[instrument(
        skip(self),
        fields(component = "parser", statement = "release_savepoint"),
        err(Debug)
    )]
    pub(in crate::parser) fn parse_release_savepoint(
        &mut self,
    ) -> Result<ReleaseSavepointStatement, ParserError> {
        self.expect(TokenType::Release)?;
        self.if_peek_then_consume(TokenType::Savepoint)?;
        let name = self.expect_ident()?;
        debug!(savepoint = %name, "parsed RELEASE SAVEPOINT statement");
        Ok(ReleaseSavepointStatement { name })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        parser::{
            Parser,
            parsers::ParserError,
            statements::{RollbackStatement, Statement},
            token::TokenType,
        },
        primitives::NonEmptyString,
    };

    fn parse(sql: &str) -> Result<Statement, ParserError> {
        Parser::new(sql).parse()
    }

    fn parse_all(sql: &str) -> Result<Vec<Statement>, ParserError> {
        Parser::new(sql).parse_all()
    }

    #[test]
    fn parse_begin_without_transaction_keyword() {
        let mut p = Parser::new("BEGIN");
        let begin = p.parse_begin().unwrap();
        assert!(!begin.transaction);
    }

    #[test]
    fn parse_begin_with_transaction_keyword() {
        let mut p = Parser::new("BEGIN TRANSACTION");
        let begin = p.parse_begin().unwrap();
        assert!(begin.transaction);
    }

    #[test]
    fn parse_commit_without_transaction_keyword() {
        let mut p = Parser::new("COMMIT");
        let commit = p.parse_commit().unwrap();
        assert!(!commit.transaction);
    }

    #[test]
    fn parse_commit_with_transaction_keyword() {
        let mut p = Parser::new("COMMIT TRANSACTION");
        let commit = p.parse_commit().unwrap();
        assert!(commit.transaction);
    }

    #[test]
    fn parse_rollback_full_abort() {
        let mut p = Parser::new("ROLLBACK");
        let rollback = p.parse_rollback().unwrap();
        assert_eq!(rollback, RollbackStatement::Transaction {
            transaction: false
        });
    }

    #[test]
    fn parse_rollback_full_abort_with_transaction_keyword() {
        let mut p = Parser::new("ROLLBACK TRANSACTION");
        let rollback = p.parse_rollback().unwrap();
        assert_eq!(rollback, RollbackStatement::Transaction {
            transaction: true
        });
    }

    #[test]
    fn parse_rollback_to_savepoint_with_keyword() {
        let mut p = Parser::new("ROLLBACK TO SAVEPOINT s1");
        let rollback = p.parse_rollback().unwrap();
        assert_eq!(rollback, RollbackStatement::Savepoint {
            name: NonEmptyString::new("s1").unwrap(),
        });
    }

    #[test]
    fn parse_rollback_to_savepoint_without_keyword() {
        let mut p = Parser::new("ROLLBACK TO before_bad_insert");
        let rollback = p.parse_rollback().unwrap();
        assert_eq!(rollback, RollbackStatement::Savepoint {
            name: NonEmptyString::new("before_bad_insert").unwrap(),
        });
    }

    #[test]
    fn parse_savepoint() {
        let mut p = Parser::new("SAVEPOINT s1");
        let savepoint = p.parse_savepoint().unwrap();
        assert_eq!(savepoint.name, "s1");
    }

    #[test]
    fn parse_release_savepoint_with_keyword() {
        let mut p = Parser::new("RELEASE SAVEPOINT s1");
        let release = p.parse_release_savepoint().unwrap();
        assert_eq!(release.name, "s1");
    }

    #[test]
    fn parse_release_savepoint_without_keyword() {
        let mut p = Parser::new("RELEASE s1");
        let release = p.parse_release_savepoint().unwrap();
        assert_eq!(release.name, "s1");
    }

    #[test]
    fn parse_dispatches_begin_through_public_entry() {
        let stmt = parse("BEGIN").unwrap();
        let Statement::Begin(begin) = stmt else {
            panic!("expected Begin, got {stmt}");
        };
        assert!(!begin.transaction);
    }

    #[test]
    fn parse_dispatches_commit_through_public_entry() {
        let stmt = parse("COMMIT TRANSACTION").unwrap();
        let Statement::Commit(commit) = stmt else {
            panic!("expected Commit, got {stmt}");
        };
        assert!(commit.transaction);
    }

    #[test]
    fn parse_dispatches_rollback_transaction_through_public_entry() {
        let stmt = parse("ROLLBACK").unwrap();
        let Statement::Rollback(RollbackStatement::Transaction { transaction }) = stmt else {
            panic!("expected Rollback(Transaction), got {stmt}");
        };
        assert!(!transaction);
    }

    #[test]
    fn parse_dispatches_rollback_to_savepoint_through_public_entry() {
        let stmt = parse("ROLLBACK TO s1").unwrap();
        let Statement::Rollback(RollbackStatement::Savepoint { name }) = stmt else {
            panic!("expected Rollback(Savepoint), got {stmt}");
        };
        assert_eq!(name, "s1");
    }

    #[test]
    fn parse_dispatches_savepoint_through_public_entry() {
        let stmt = parse("SAVEPOINT s1").unwrap();
        let Statement::Savepoint(savepoint) = stmt else {
            panic!("expected Savepoint, got {stmt}");
        };
        assert_eq!(savepoint.name, "s1");
    }

    #[test]
    fn parse_dispatches_release_savepoint_through_public_entry() {
        let stmt = parse("RELEASE SAVEPOINT s1").unwrap();
        let Statement::ReleaseSavepoint(release) = stmt else {
            panic!("expected ReleaseSavepoint, got {stmt}");
        };
        assert_eq!(release.name, "s1");
    }

    #[test]
    fn parse_all_transaction_control_sequence() {
        let stmts = parse_all(concat!(
            "BEGIN;",
            "SAVEPOINT s1;",
            "ROLLBACK TO SAVEPOINT s1;",
            "RELEASE SAVEPOINT s1;",
            "COMMIT;",
        ))
        .unwrap();
        assert_eq!(stmts.len(), 5);
        assert!(matches!(stmts[0], Statement::Begin(_)));
        assert!(matches!(stmts[1], Statement::Savepoint(_)));
        assert!(matches!(
            stmts[2],
            Statement::Rollback(RollbackStatement::Savepoint { .. })
        ));
        assert!(matches!(stmts[3], Statement::ReleaseSavepoint(_)));
        assert!(matches!(stmts[4], Statement::Commit(_)));
    }

    #[test]
    fn parse_savepoint_missing_name_is_error() {
        let mut p = Parser::new("SAVEPOINT");
        assert!(matches!(p.parse_savepoint(), Err(ParserError::WantedToken)));
    }

    #[test]
    fn parse_rollback_to_missing_name_is_error() {
        let mut p = Parser::new("ROLLBACK TO");
        assert!(matches!(p.parse_rollback(), Err(ParserError::WantedToken)));
    }

    #[test]
    fn parse_release_missing_name_is_error() {
        let mut p = Parser::new("RELEASE");
        assert!(matches!(
            p.parse_release_savepoint(),
            Err(ParserError::WantedToken)
        ));
    }

    #[test]
    fn parse_savepoint_display_round_trip() {
        let stmt = parse("SAVEPOINT s1").unwrap();
        assert_eq!(stmt.to_string(), "SAVEPOINT s1");
    }

    #[test]
    fn parse_rollback_to_display_uses_canonical_form() {
        let stmt = parse("ROLLBACK TO s1").unwrap();
        assert_eq!(stmt.to_string(), "ROLLBACK TO SAVEPOINT s1");
    }

    #[test]
    fn parse_begin_rejects_wrong_leading_keyword() {
        let mut p = Parser::new("COMMIT");
        assert!(matches!(
            p.parse_begin(),
            Err(ParserError::UnexpectedToken {
                expected: TokenType::Begin,
                ..
            })
        ));
    }
}
