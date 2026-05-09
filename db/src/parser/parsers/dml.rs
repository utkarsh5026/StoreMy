use tracing::{debug, instrument, trace, warn};

use super::ParserError;
use crate::{
    Value,
    parser::{
        Parser,
        parsers::expr::Precedence,
        statements::{
            Assignment, DeleteStatement, InsertSource, InsertStatement, Statement, UpdateStatement,
        },
        token::TokenType,
    },
};

impl Parser {
    /// Parses `DELETE FROM <table> [<alias>] [WHERE <condition>]`.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if `DELETE FROM` or the table name is missing,
    /// or if the `WHERE` clause is malformed.
    #[instrument(
        skip(self),
        fields(component = "parser", statement = "delete"),
        err(Debug)
    )]
    pub(super) fn parse_delete(&mut self) -> Result<DeleteStatement, ParserError> {
        self.expect_seq(&[TokenType::Delete, TokenType::From])?;
        let (table_name, alias) = self.parse_table_with_alias()?;

        if !self.peek_is(TokenType::Where)? {
            debug!(
                table = %table_name,
                has_alias = alias.is_some(),
                has_where = false,
                "parsed DELETE statement"
            );
            return Ok(Statement::delete(table_name, alias, None));
        }

        let where_clause = self.on_peek_token(TokenType::Where, Parser::parse_where)?;
        debug!(
            table = %table_name,
            has_alias = alias.is_some(),
            has_where = where_clause.is_some(),
            "parsed DELETE statement"
        );
        Ok(Statement::delete(table_name, alias, where_clause))
    }

    /// Parses one of:
    ///   `INSERT INTO <table> [(<cols>)] VALUES (<row>)[, (<row>)]*`
    ///   `INSERT INTO <table> [(<cols>)] SELECT ...`
    ///   `INSERT INTO <table> DEFAULT VALUES`
    ///
    /// The column list is optional for `VALUES` and `SELECT`. For
    /// `DEFAULT VALUES` the column list is forbidden — if a `(` is present
    /// it parses as a column list and the following `DEFAULT` then fails
    /// the `expect(VALUES)` check, matching the error a user would expect.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the syntax is malformed or a value token
    /// can't be converted to a [`Value`].
    #[instrument(
        skip(self),
        fields(component = "parser", statement = "insert"),
        err(Debug)
    )]
    pub(super) fn parse_insert(&mut self) -> Result<InsertStatement, ParserError> {
        self.expect_seq(&[TokenType::Insert, TokenType::Into])?;
        let (table_name, _) = self.parse_table_with_alias()?;

        // INSERT INTO t DEFAULT VALUES — branches before the optional column
        // list, so `INSERT INTO t (a) DEFAULT VALUES` ends up failing the
        // VALUES expectation below rather than being silently accepted.
        if self.peek_is(TokenType::Default)? {
            self.expect_seq(&[TokenType::Default, TokenType::Values])?;
            debug!(
                table = %table_name,
                column_count = 0,
                source = "default_values",
                "parsed INSERT statement"
            );
            return Ok(Statement::insert(
                table_name,
                None,
                InsertSource::DefaultValues,
            ));
        }

        let columns = self.on_peek_token(TokenType::Lparen, |p| {
            p.parse_delimited_list(TokenType::Comma, TokenType::Rparen, Parser::expect_ident)
        })?;

        let source = self.parse_insert_source()?;
        let row_count = match &source {
            InsertSource::Values(rows) => rows.len(),
            InsertSource::Select(_) | InsertSource::DefaultValues => 0,
        };
        let source_kind = match &source {
            InsertSource::Values(_) => "values",
            InsertSource::Select(_) => "select",
            InsertSource::DefaultValues => "default_values",
        };
        debug!(
            table = %table_name,
            column_count = columns.as_ref().map_or(0, Vec::len),
            source = source_kind,
            row_count,
            "parsed INSERT statement"
        );
        Ok(Statement::insert(table_name, columns, source))
    }

    /// Parses the data source for an `INSERT` statement.
    ///
    /// Handles two forms:
    ///   - `VALUES (<row>)[, (<row>)]*`: parses one or more parenthesized row value lists.
    ///   - `SELECT ...`: parses an arbitrary `SELECT` statement (subquery).
    ///
    /// This function assumes the preceding context is valid for an `INSERT` source:
    /// after the table name (and optional columns).
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if:
    ///   - The next token is not `VALUES` or `SELECT`.
    ///   - A row value list is malformed (missing parens, bad token, etc).
    ///   - A value cannot be converted to a [`Value`] type.
    ///   - `SELECT` parsing fails.
    #[instrument(
        skip(self),
        fields(component = "parser", clause = "insert_source"),
        err(Debug)
    )]
    fn parse_insert_source(&mut self) -> Result<InsertSource, ParserError> {
        if self.peek_is(TokenType::Select)? {
            trace!("INSERT source is SELECT");
            Ok(InsertSource::Select(Box::new(self.parse_select()?)))
        } else {
            self.expect(TokenType::Values)?;
            let mut rows = Vec::new();
            loop {
                let row = self.paren_list(|p| {
                    let t = p.bump()?;
                    let value_kind = t.kind;
                    let value_position = t.span.start;
                    Value::try_from(t).map_err(|msg| {
                        warn!(
                            value_token_kind = ?value_kind,
                            value_position,
                            reason = %msg,
                            "invalid INSERT literal"
                        );
                        ParserError::ParsingError(msg)
                    })
                })?;

                rows.push(row);
                if self.if_peek_then_consume(TokenType::Comma)? {
                    continue;
                }
                break;
            }
            debug!(row_count = rows.len(), "parsed VALUES source");
            Ok(InsertSource::Values(rows))
        }
    }

    /// Parses `UPDATE <table> [<alias>] SET <col>=<val>[, ...] [WHERE <condition>]`.
    ///
    /// Only `=` is accepted as the assignment operator.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if any keyword is missing, a non-`=` operator is
    /// used, or a value token can't be converted to a [`Value`].
    #[instrument(
        skip(self),
        fields(component = "parser", statement = "update"),
        err(Debug)
    )]
    pub(super) fn parse_update(&mut self) -> Result<UpdateStatement, ParserError> {
        self.expect(TokenType::Update)?;
        let (table_name, alias) = self.parse_table_with_alias()?;
        self.expect(TokenType::Set)?;

        let mut assignments = Vec::new();
        loop {
            let field = self.expect_ident()?;
            let op = self.expect(TokenType::Operator)?;

            if op.value.ne("=") {
                warn!(operator = %op.value, "UPDATE assignment must use '='");
                return Err(ParserError::unexpected(TokenType::Operator, op.kind));
            }

            let value = self.parse_expression(Precedence::LOOSEST)?;

            assignments.push(Assignment {
                column: field,
                value,
            });

            if self.on_peek_token(TokenType::Comma, |_| Ok(()))?.is_none() {
                break;
            }
        }

        let where_clause = self.on_peek_token(TokenType::Where, Parser::parse_where)?;
        debug!(
            table = %table_name,
            has_alias = alias.is_some(),
            assignment_count = assignments.len(),
            has_where = where_clause.is_some(),
            "parsed UPDATE statement"
        );
        Ok(Statement::update(
            table_name,
            alias,
            assignments,
            where_clause,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{Parser, ParserError};
    use crate::{
        Value,
        parser::{
            parsers::expr::{BinOp, Expr},
            statements::{ColumnRef, InsertSource},
            token::TokenType,
        },
        primitives::NonEmptyString,
    };

    fn insert_rows(src: &InsertSource) -> &Vec<Vec<Value>> {
        match src {
            InsertSource::Values(rows) => rows,
            other => panic!("expected InsertSource::Values, got {other:?}"),
        }
    }

    fn column_names(columns: Option<&[NonEmptyString]>) -> Option<Vec<&str>> {
        columns.map(|cols| cols.iter().map(NonEmptyString::as_str).collect())
    }

    #[test]
    fn test_parse_delete_basic() {
        let mut p = Parser::new("DELETE FROM users");
        let d = p.parse_delete().unwrap();
        assert_eq!(d.table_name, "users");
        assert!(d.alias.is_none());
        assert!(d.where_clause.is_none());
    }

    #[test]
    fn test_parse_delete_with_alias() {
        let mut p = Parser::new("DELETE FROM users u");
        let d = p.parse_delete().unwrap();
        assert_eq!(d.table_name, "users");
        assert_eq!(d.alias.as_deref(), Some("u"));
    }

    #[test]
    fn test_parse_delete_where_predicate() {
        let mut p = Parser::new("DELETE FROM t WHERE id = 7");
        let d = p.parse_delete().unwrap();
        let wc = d.where_clause.expect("where clause");
        let Expr::BinaryOp { lhs, op, rhs } = wc else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::Eq);
        assert_eq!(*lhs, Expr::Column(ColumnRef::from("id")));
        assert_eq!(*rhs, Expr::Literal(Value::Int64(7)));
    }

    #[test]
    fn test_parse_insert_no_column_list_single_row() {
        let mut p = Parser::new("INSERT INTO items VALUES (42)");
        let i = p.parse_insert().unwrap();
        assert_eq!(i.table_name, "items");
        assert!(i.columns.is_none());
        let rows = insert_rows(&i.source);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![Value::Int64(42)]);
    }

    #[test]
    fn test_parse_insert_explicit_columns_and_multi_row() {
        let mut p = Parser::new("INSERT INTO items (a, b) VALUES (1, 'x'), (2, 'y')");
        let i = p.parse_insert().unwrap();
        assert_eq!(i.table_name, "items");
        assert_eq!(column_names(i.columns.as_deref()), Some(vec!["a", "b"]));
        let rows = insert_rows(&i.source);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int64(1));
        assert_eq!(rows[1][1], Value::String("y".into()));
    }

    #[test]
    fn test_parse_update_single_assignment_no_where() {
        let mut p = Parser::new("UPDATE users SET active = false");
        let u = p.parse_update().unwrap();
        assert_eq!(u.table_name, "users");
        assert!(u.alias.is_none());
        assert_eq!(u.assignments.len(), 1);
        assert_eq!(u.assignments[0].column, "active");
        assert_eq!(u.assignments[0].value, Expr::Literal(Value::Bool(false)));
        assert!(u.where_clause.is_none());
    }

    #[test]
    fn test_parse_update_alias_multiple_assignments_and_where() {
        let mut p = Parser::new("UPDATE users u SET a = 1, b = 'z' WHERE id = 2");
        let u = p.parse_update().unwrap();
        assert_eq!(u.alias.as_deref(), Some("u"));
        assert_eq!(u.assignments.len(), 2);
        let wc = u.where_clause.expect("where clause");
        let Expr::BinaryOp { lhs, op, rhs } = wc else {
            panic!("expected BinaryOp");
        };
        assert_eq!(op, BinOp::Eq);
        assert_eq!(*lhs, Expr::Column(ColumnRef::from("id")));
        assert_eq!(*rhs, Expr::Literal(Value::Int64(2)));
    }

    #[test]
    fn test_parse_insert_single_column_list_and_value() {
        let mut p = Parser::new("INSERT INTO t (k) VALUES (0)");
        let i = p.parse_insert().unwrap();
        assert_eq!(i.columns.as_ref().unwrap().as_slice(), ["k"]);
        assert_eq!(insert_rows(&i.source)[0], vec![Value::Int64(0)]);
    }

    #[test]
    fn test_parse_update_int_assignment_parses_as_int64() {
        let mut p = Parser::new("UPDATE counters SET n = 99");
        let u = p.parse_update().unwrap();
        assert_eq!(u.assignments[0].value, Expr::Literal(Value::Int64(99)));
    }

    #[test]
    fn test_parse_update_assignment_allows_column_expr() {
        let mut p = Parser::new("UPDATE t SET a = b");
        let u = p.parse_update().unwrap();
        assert_eq!(u.assignments.len(), 1);
        assert_eq!(u.assignments[0].column, "a");
        assert_eq!(u.assignments[0].value, Expr::Column(ColumnRef::from("b")));
    }

    #[test]
    fn test_parse_update_assignment_allows_unary_not_expr() {
        let mut p = Parser::new("UPDATE t SET active = NOT active");
        let u = p.parse_update().unwrap();
        assert_eq!(u.assignments.len(), 1);
        assert_eq!(u.assignments[0].column, "active");
        assert!(matches!(u.assignments[0].value, Expr::UnaryOp {
            op: _,
            operand: _
        }));
    }

    #[test]
    fn test_parse_insert_empty_column_list_parentheses_only() {
        let mut p = Parser::new("INSERT INTO t () VALUES (1)");
        let err = p.parse_insert().unwrap_err();
        assert!(
            matches!(err, ParserError::UnexpectedToken {
                expected: TokenType::Identifier,
                ..
            }),
            "got {err:?}"
        );
    }

    #[test]
    fn test_parse_insert_row_with_only_string_literal() {
        let mut p = Parser::new("INSERT INTO t VALUES ('only')");
        let i = p.parse_insert().unwrap();
        assert_eq!(insert_rows(&i.source)[0], vec![Value::String(
            "only".into()
        )]);
    }

    #[test]
    fn test_parse_update_where_and_or_precedence_matches_delete() {
        let mut p = Parser::new("UPDATE t SET x = 1 WHERE p = 0 OR q = 1 AND r = 2");
        let u = p.parse_update().unwrap();
        let wc = u.where_clause.expect("where");
        let Expr::BinaryOp { lhs, op, rhs } = wc else {
            panic!("expected BinaryOp at root");
        };
        assert_eq!(op, BinOp::Or);
        assert!(matches!(lhs.as_ref(), Expr::BinaryOp { op: BinOp::Eq, .. }));
        assert!(matches!(rhs.as_ref(), Expr::BinaryOp {
            op: BinOp::And,
            ..
        }));
    }

    #[test]
    fn test_parse_delete_missing_from_keyword() {
        let mut p = Parser::new("DELETE users");
        let err = p.parse_delete().unwrap_err();
        assert!(
            matches!(err, ParserError::UnexpectedToken {
                expected: TokenType::From,
                ..
            }),
            "got {err:?}"
        );
    }

    #[test]
    fn test_parse_delete_missing_table() {
        let mut p = Parser::new("DELETE FROM");
        assert!(matches!(p.parse_delete(), Err(ParserError::WantedToken)));
    }

    #[test]
    fn test_parse_delete_wrong_leading_keyword() {
        let mut p = Parser::new("SELECT * FROM t");
        let err = p.parse_delete().unwrap_err();
        assert!(
            matches!(err, ParserError::UnexpectedToken {
                expected: TokenType::Delete,
                ..
            }),
            "got {err:?}"
        );
    }

    #[test]
    fn test_parse_delete_where_clause_incomplete() {
        let mut p = Parser::new("DELETE FROM t WHERE");
        let err = p.parse_delete().unwrap_err();
        assert!(
            matches!(err, ParserError::WantedToken | ParserError::ParsingError(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn test_parse_insert_missing_into_sequence() {
        let mut p = Parser::new("INSERT users VALUES (1)");
        let err = p.parse_insert().unwrap_err();
        assert!(
            matches!(err, ParserError::UnexpectedToken {
                expected: TokenType::Into,
                ..
            }),
            "got {err:?}"
        );
    }

    #[test]
    fn test_parse_insert_missing_values_keyword() {
        let mut p = Parser::new("INSERT INTO t (a) (1)");
        let err = p.parse_insert().unwrap_err();
        assert!(
            matches!(err, ParserError::UnexpectedToken {
                expected: TokenType::Values,
                ..
            }),
            "got {err:?}"
        );
    }

    #[test]
    fn test_parse_insert_value_rejects_non_literal_token() {
        let mut p = Parser::new("INSERT INTO t VALUES (WHERE)");
        let err = p.parse_insert().unwrap_err();
        assert!(matches!(err, ParserError::ParsingError(_)), "got {err:?}");
    }

    #[test]
    fn test_parse_insert_missing_table_after_into() {
        let mut p = Parser::new("INSERT INTO VALUES (1)");
        let err = p.parse_insert().unwrap_err();
        assert!(
            matches!(err, ParserError::UnexpectedToken {
                expected: TokenType::Identifier,
                ..
            }),
            "got {err:?}"
        );
    }

    #[test]
    fn test_parse_insert_values_without_opening_paren() {
        let mut p = Parser::new("INSERT INTO t VALUES 1");
        let err = p.parse_insert().unwrap_err();
        assert!(
            matches!(err, ParserError::UnexpectedToken {
                expected: TokenType::Lparen,
                ..
            }),
            "got {err:?}"
        );
    }

    #[test]
    fn test_parse_insert_empty_values_row() {
        let mut p = Parser::new("INSERT INTO t VALUES ()");
        let err = p.parse_insert().unwrap_err();
        assert!(
            matches!(err, ParserError::WantedToken | ParserError::ParsingError(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn test_parse_update_missing_leading_update_keyword() {
        let mut p = Parser::new("SET x = 1");
        let err = p.parse_update().unwrap_err();
        assert!(
            matches!(err, ParserError::UnexpectedToken {
                expected: TokenType::Update,
                ..
            }),
            "got {err:?}"
        );
    }

    #[test]
    fn test_parse_update_missing_table_name() {
        let mut p = Parser::new("UPDATE SET x = 1");
        let err = p.parse_update().unwrap_err();
        assert!(
            matches!(err, ParserError::UnexpectedToken {
                expected: TokenType::Identifier,
                ..
            }),
            "got {err:?}"
        );
    }

    #[test]
    fn test_parse_update_set_clause_incomplete() {
        let mut p = Parser::new("UPDATE t SET");
        assert!(matches!(p.parse_update(), Err(ParserError::WantedToken)));
    }

    #[test]
    fn test_parse_update_where_incomplete() {
        let mut p = Parser::new("UPDATE t SET x = 1 WHERE");
        let err = p.parse_update().unwrap_err();
        assert!(
            matches!(err, ParserError::WantedToken | ParserError::ParsingError(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn test_parse_update_rejects_non_equals_assignment_operator() {
        let mut p = Parser::new("UPDATE t SET x < 1");
        let err = p.parse_update().unwrap_err();
        assert!(
            matches!(err, ParserError::UnexpectedToken {
                expected: TokenType::Operator,
                found: TokenType::Operator,
            }),
            "got {err:?}"
        );
    }

    #[test]
    fn test_parse_update_rhs_value_conversion_error() {
        let mut p = Parser::new("UPDATE t SET x = bogus_id");
        let u = p.parse_update().unwrap();
        assert_eq!(u.assignments.len(), 1);
        assert_eq!(u.assignments[0].column, "x");
        assert_eq!(
            u.assignments[0].value,
            Expr::Column(ColumnRef::from("bogus_id"))
        );
    }

    #[test]
    fn test_parse_update_missing_set() {
        let mut p = Parser::new("UPDATE t x = 1");
        let err = p.parse_update().unwrap_err();
        assert!(
            matches!(err, ParserError::UnexpectedToken {
                expected: TokenType::Set,
                found: TokenType::Operator,
            }),
            "got {err:?}"
        );
    }

    // --- property / invariant: WHERE shape matches parse_predicate folding ---

    // --- DEFAULT VALUES ---

    #[test]
    fn test_parse_insert_default_values() {
        let mut p = Parser::new("INSERT INTO logs DEFAULT VALUES");
        let i = p.parse_insert().unwrap();
        assert_eq!(i.table_name, "logs");
        assert!(i.columns.is_none());
        assert!(matches!(i.source, InsertSource::DefaultValues));
    }

    #[test]
    fn test_parse_insert_default_without_values_keyword() {
        // Bare `DEFAULT` at end-of-input surfaces as WantedToken (no more tokens).
        // `DEFAULT <something-else>` would give UnexpectedToken { expected: Values }.
        let mut p = Parser::new("INSERT INTO logs DEFAULT");
        let err = p.parse_insert().unwrap_err();
        assert!(
            matches!(
                err,
                ParserError::WantedToken
                    | ParserError::UnexpectedToken {
                        expected: TokenType::Values,
                        ..
                    }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn test_parse_insert_default_values_rejects_column_list() {
        // `(a)` parses as a column list; the following DEFAULT then fails the
        // VALUES expectation. We accept either an UnexpectedToken { expected: Values }
        // or a ParsingError depending on lexer routing for the literal-conversion path.
        let mut p = Parser::new("INSERT INTO t (a) DEFAULT VALUES");
        let err = p.parse_insert().unwrap_err();
        assert!(
            matches!(err, ParserError::UnexpectedToken {
                expected: TokenType::Values,
                ..
            }),
            "got {err:?}"
        );
    }

    // --- INSERT … SELECT … ---

    #[test]
    fn test_parse_insert_select_star() {
        let mut p = Parser::new("INSERT INTO archive SELECT * FROM users");
        let i = p.parse_insert().unwrap();
        assert_eq!(i.table_name, "archive");
        assert!(i.columns.is_none());
        let InsertSource::Select(sel) = &i.source else {
            panic!("expected Select source, got {:?}", i.source);
        };
        assert_eq!(sel.from.len(), 1);
    }

    #[test]
    fn test_parse_insert_select_with_column_list_and_where() {
        let mut p = Parser::new(
            "INSERT INTO archive (id, name) SELECT id, name FROM users WHERE deleted = true",
        );
        let i = p.parse_insert().unwrap();
        assert_eq!(column_names(i.columns.as_deref()), Some(vec!["id", "name"]));
        let InsertSource::Select(sel) = &i.source else {
            panic!("expected Select source, got {:?}", i.source);
        };
        assert!(sel.where_clause.is_some());
    }

    #[test]
    fn test_parse_insert_neither_values_nor_select() {
        // After a column list we expect VALUES (or SELECT). A bare integer
        // should surface as an UnexpectedToken pointing at VALUES.
        let mut p = Parser::new("INSERT INTO t (a) 1");
        let err = p.parse_insert().unwrap_err();
        assert!(
            matches!(err, ParserError::UnexpectedToken {
                expected: TokenType::Values,
                ..
            }),
            "got {err:?}"
        );
    }

    #[test]
    fn test_parse_delete_where_and_binds_tighter_than_or() {
        let mut p = Parser::new("DELETE FROM t WHERE a = 1 OR b = 2 AND c = 3");
        let d = p.parse_delete().unwrap();
        let wc = d.where_clause.expect("where");
        let Expr::BinaryOp { lhs, op, rhs } = wc else {
            panic!("expected OR at top");
        };
        assert_eq!(op, BinOp::Or);
        let Expr::BinaryOp {
            lhs: l_lhs,
            op: l_op,
            ..
        } = lhs.as_ref()
        else {
            panic!("expected left comparison");
        };
        assert_eq!(l_op, &BinOp::Eq);
        let Expr::Column(col) = l_lhs.as_ref() else {
            panic!("expected column on left of a = 1");
        };
        assert_eq!(col.name.as_str(), "a");
        let Expr::BinaryOp { op: r_op, .. } = rhs.as_ref() else {
            panic!("expected AND group on right");
        };
        assert_eq!(r_op, &BinOp::And);
    }
}
