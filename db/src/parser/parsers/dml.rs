use super::ParserError;
use crate::{
    Value,
    parser::{
        Parser,
        statements::{Assignment, DeleteStatement, InsertStatement, Statement, UpdateStatement},
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
    pub(super) fn parse_delete(&mut self) -> Result<DeleteStatement, ParserError> {
        self.expect_seq(&[TokenType::Delete, TokenType::From])?;
        let (table_name, alias) = self.parse_table_with_alias()?;

        if !self.peek_is(TokenType::Where)? {
            return Ok(Statement::delete(table_name, alias, None));
        }

        let where_clause = self.on_peek_token(TokenType::Where, Parser::parse_where)?;
        Ok(Statement::delete(table_name, alias, where_clause))
    }

    /// Parses `INSERT INTO <table> [(<cols>)] VALUES (<row>)[, (<row>)...]`.
    ///
    /// The column list is optional.  Multiple value rows are supported.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if the syntax is malformed or a value token
    /// can't be converted to a [`Value`].
    pub(super) fn parse_insert(&mut self) -> Result<InsertStatement, ParserError> {
        self.expect_seq(&[TokenType::Insert, TokenType::Into])?;
        let (table_name, _) = self.parse_table_with_alias()?;

        let columns = self.on_peek_token(TokenType::Lparen, |p| {
            p.parse_delimited_list(TokenType::Comma, TokenType::Rparen, |p| {
                Ok(p.expect(TokenType::Identifier)?.value)
            })
        })?;

        self.expect(TokenType::Values)?;
        let mut values = Vec::new();
        loop {
            self.expect(TokenType::Lparen)?;
            let row = self.parse_delimited_list(TokenType::Comma, TokenType::Rparen, |p| {
                let t = p.bump()?;
                Value::try_from(t).map_err(ParserError::ParsingError)
            })?;
            values.push(row);
            if self.on_peek_token(TokenType::Comma, |_| Ok(()))?.is_none() {
                break;
            }
        }

        Ok(Statement::insert(table_name, columns, values))
    }

    /// Parses `UPDATE <table> [<alias>] SET <col>=<val>[, ...] [WHERE <condition>]`.
    ///
    /// Only `=` is accepted as the assignment operator.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if any keyword is missing, a non-`=` operator is
    /// used, or a value token can't be converted to a [`Value`].
    pub(super) fn parse_update(&mut self) -> Result<UpdateStatement, ParserError> {
        self.expect(TokenType::Update)?;
        let (table_name, alias) = self.parse_table_with_alias()?;
        self.expect(TokenType::Set)?;

        let mut assignments = Vec::new();
        loop {
            let field = self.expect(TokenType::Identifier)?;
            let op = self.expect(TokenType::Operator)?;

            if op.value.ne("=") {
                return Err(ParserError::unexpected(TokenType::Operator, op.kind));
            }

            let t = self.bump()?;
            let val = Value::try_from(t).map_err(ParserError::ParsingError)?;

            assignments.push(Assignment {
                column: field.value,
                value: val,
            });

            if self.on_peek_token(TokenType::Comma, |_| Ok(()))?.is_none() {
                break;
            }
        }

        let where_clause = self.on_peek_token(TokenType::Where, Parser::parse_where)?;
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
        parser::{statements::WhereCondition, token::TokenType},
        primitives::Predicate,
    };

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
        let wc = d.where_clause.as_ref().unwrap();
        let WhereCondition::Predicate { field, op, value } = wc else {
            panic!("expected Predicate");
        };
        assert_eq!(field, "id");
        assert_eq!(*op, Predicate::Equals);
        assert_eq!(*value, Value::Int64(7));
    }

    #[test]
    fn test_parse_insert_no_column_list_single_row() {
        let mut p = Parser::new("INSERT INTO items VALUES (42)");
        let i = p.parse_insert().unwrap();
        assert_eq!(i.table_name, "items");
        assert!(i.columns.is_none());
        assert_eq!(i.values.len(), 1);
        assert_eq!(i.values[0], vec![Value::Int64(42)]);
    }

    #[test]
    fn test_parse_insert_explicit_columns_and_multi_row() {
        let mut p = Parser::new("INSERT INTO items (a, b) VALUES (1, 'x'), (2, 'y')");
        let i = p.parse_insert().unwrap();
        assert_eq!(i.table_name, "items");
        assert_eq!(
            i.columns.as_deref(),
            Some(&["a".to_string(), "b".to_string()][..])
        );
        assert_eq!(i.values.len(), 2);
        assert_eq!(i.values[0][0], Value::Int64(1));
        assert_eq!(i.values[1][1], Value::String("y".into()));
    }

    #[test]
    fn test_parse_update_single_assignment_no_where() {
        let mut p = Parser::new("UPDATE users SET active = false");
        let u = p.parse_update().unwrap();
        assert_eq!(u.table_name, "users");
        assert!(u.alias.is_none());
        assert_eq!(u.assignments.len(), 1);
        assert_eq!(u.assignments[0].column, "active");
        assert_eq!(u.assignments[0].value, Value::Bool(false));
        assert!(u.where_clause.is_none());
    }

    #[test]
    fn test_parse_update_alias_multiple_assignments_and_where() {
        let mut p = Parser::new("UPDATE users u SET a = 1, b = 'z' WHERE id = 2");
        let u = p.parse_update().unwrap();
        assert_eq!(u.alias.as_deref(), Some("u"));
        assert_eq!(u.assignments.len(), 2);
        let wc = u.where_clause.as_ref().unwrap();
        let WhereCondition::Predicate { field, op, value } = wc else {
            panic!("expected Predicate");
        };
        assert_eq!(field, "id");
        assert_eq!(*op, Predicate::Equals);
        assert_eq!(*value, Value::Int64(2));
    }

    #[test]
    fn test_parse_insert_single_column_list_and_value() {
        let mut p = Parser::new("INSERT INTO t (k) VALUES (0)");
        let i = p.parse_insert().unwrap();
        assert_eq!(i.columns.as_ref().unwrap().as_slice(), ["k"]);
        assert_eq!(i.values[0], vec![Value::Int64(0)]);
    }

    #[test]
    fn test_parse_update_int_assignment_parses_as_int64() {
        let mut p = Parser::new("UPDATE counters SET n = 99");
        let u = p.parse_update().unwrap();
        assert_eq!(u.assignments[0].value, Value::Int64(99));
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
        assert_eq!(i.values[0], vec![Value::String("only".into())]);
    }

    #[test]
    fn test_parse_update_where_and_or_precedence_matches_delete() {
        let mut p = Parser::new("UPDATE t SET x = 1 WHERE p = 0 OR q = 1 AND r = 2");
        let u = p.parse_update().unwrap();
        let wc = u.where_clause.expect("where");
        let WhereCondition::Or(left, right) = wc else {
            panic!("expected OR at top");
        };
        assert!(matches!(left.as_ref(), WhereCondition::Predicate { .. }));
        assert!(matches!(right.as_ref(), WhereCondition::And(_, _)));
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
        let err = p.parse_update().unwrap_err();
        match &err {
            ParserError::ParsingError(msg) => assert!(msg.contains("bogus_id"), "got {msg}"),
            _ => panic!("expected ParsingError, got {err:?}"),
        }
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

    #[test]
    fn test_parse_delete_where_and_binds_tighter_than_or() {
        let mut p = Parser::new("DELETE FROM t WHERE a = 1 OR b = 2 AND c = 3");
        let d = p.parse_delete().unwrap();
        let wc = d.where_clause.expect("where");
        let WhereCondition::Or(left, right) = wc else {
            panic!("expected OR at top");
        };
        let WhereCondition::Predicate { field, .. } = left.as_ref() else {
            panic!("expected left predicate");
        };
        assert_eq!(field, "a");
        let WhereCondition::And(..) = right.as_ref() else {
            panic!("expected AND group on right");
        };
    }
}
