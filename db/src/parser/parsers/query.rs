use tracing::{debug, instrument, trace, warn};

use super::ParserError;
use crate::{
    parser::{
        Parser,
        statements::{
            ColumnRef, Join, JoinKind, LimitClause, OrderBy, OrderDirection, SelectColumns,
            SelectItem, SelectStatement, TableRef, TableWithJoins,
        },
        token::TokenType,
    },
    primitives::NonEmptyString,
};

impl Parser {
    /// Parses a full `SELECT` statement including all optional clauses.
    ///
    /// Grammar (simplified):
    /// ```text
    /// SELECT [DISTINCT] (* | <expr>[, ...])
    ///   FROM <table> [<alias>]
    ///   [<join> ...]
    ///   [WHERE <condition>]
    ///   [GROUP BY <col>]
    ///   [ORDER BY <col> [ASC|DESC]]
    ///   [LIMIT <n> [OFFSET <m>]]
    /// ```
    ///
    /// Aggregate functions (`COUNT(*)`, `SUM`, `AVG`, `MIN`, `MAX`) are
    /// supported in the select list.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if any required token is missing or an aggregate
    /// function name is unrecognized.
    #[instrument(
        skip(self),
        fields(component = "parser", statement = "select"),
        err(Debug)
    )]
    pub(super) fn parse_select(&mut self) -> Result<SelectStatement, ParserError> {
        self.expect(TokenType::Select)?;

        let distinct = self
            .on_peek_token(TokenType::Distinct, |_p| Ok(true))?
            .unwrap_or(false);

        let columns = if self.peek_is(TokenType::Asterisk)? {
            self.expect_seq(&[TokenType::Asterisk, TokenType::From])?;
            SelectColumns::All
        } else {
            SelectColumns::Exprs(self.parse_select_list()?)
        };

        let from = self.parse_tables()?;
        let where_clause = self.on_peek_token(TokenType::Where, Parser::parse_where)?;
        let group_by = self.parse_group_by()?;
        let having = self.on_peek_token(TokenType::Having, Parser::parse_where)?;
        let order_by = self.parse_order_by()?;
        let limit = self.parse_limit_offset()?;

        let join_count: usize = from.iter().map(|t| t.joins.len()).sum();
        let projection_count = match &columns {
            SelectColumns::All => 0,
            SelectColumns::Exprs(exprs) => exprs.len(),
        };
        debug!(
            distinct,
            select_all = matches!(columns, SelectColumns::All),
            projection_count,
            from_count = from.len(),
            join_count,
            has_where = where_clause.is_some(),
            group_by_count = group_by.len(),
            has_having = having.is_some(),
            order_by_count = order_by.len(),
            has_limit = limit.is_some(),
            "parsed SELECT statement"
        );

        Ok(SelectStatement {
            distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
        })
    }

    /// Parses the comma-separated table list that follows the `FROM` keyword.
    ///
    /// Each entry is a table name with an optional alias (via
    /// [`parse_table_with_alias`]) followed by zero or more JOIN clauses (via
    /// [`parse_joins`]).  Parsing stops as soon as the next token is not a
    /// comma, leaving that token on the stream for the caller.
    ///
    /// ```text
    /// <tables> ::= <table_with_joins> ( "," <table_with_joins> )*
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if any table name is missing, an alias is
    /// malformed, or a JOIN clause cannot be parsed.
    fn parse_tables(&mut self) -> Result<Vec<TableWithJoins>, ParserError> {
        let mut tables = vec![];

        loop {
            let (name, alias) = self.parse_table_with_alias()?;
            let joins = self.parse_joins()?;
            tables.push(TableWithJoins {
                table: TableRef { name, alias },
                joins,
            });

            if !self.peek_is(TokenType::Comma)? {
                return Ok(tables);
            }
            self.expect(TokenType::Comma)?;
        }
    }

    /// Parses a comma-separated `SELECT` projection list up to but not
    /// including the mandatory `FROM` keyword.
    ///
    /// Each item is a full expression (literal, column ref, arithmetic, aggregate, boolean, etc.)
    /// parsed by [`Parser::parse_expr`], followed by an optional `AS alias` or implicit alias.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if any item expression is invalid, commas or the
    /// terminating `FROM` are wrong, or an alias identifier is missing after `AS`.
    fn parse_select_list(&mut self) -> Result<Vec<SelectItem>, ParserError> {
        let parse_select_item = |p: &mut Parser| -> Result<SelectItem, ParserError> {
            let expr = p.parse_expression()?;
            let alias =
                if p.if_peek_then_consume(TokenType::As)? || p.peek_is(TokenType::Identifier)? {
                    let tok = p.expect(TokenType::Identifier)?;
                    Some(tok.value)
                } else {
                    None
                };
            Ok(SelectItem {
                expr,
                alias: alias.map(NonEmptyString::try_from).transpose()?,
            })
        };
        self.parse_delimited_list(TokenType::Comma, TokenType::From, parse_select_item)
    }

    /// Parses an optional `ORDER BY` clause with one or more sort keys.
    ///
    /// Each key is `<column> [ASC|DESC]`; keys are separated by commas and
    /// listed in priority order (the first key is the primary sort). When a
    /// direction keyword is omitted, [`OrderDirection::Asc`] is used.
    ///
    /// If the next token is not `ORDER`, returns an empty vec without
    /// consuming input. An empty vec therefore encodes "no clause".
    ///
    /// ```text
    /// <order_by> ::= "ORDER" "BY" <key> ( "," <key> )*
    /// <key>      ::= <column> [ "ASC" | "DESC" ]
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] when `ORDER` is present but `BY`, any sort
    /// column, or an explicit `ASC`/`DESC` token is malformed.
    #[instrument(
        skip(self),
        fields(component = "parser", clause = "order_by"),
        err(Debug)
    )]
    fn parse_order_by(&mut self) -> Result<Vec<OrderBy>, ParserError> {
        let parse_key = |p: &mut Parser| -> Result<OrderBy, ParserError> {
            let col = p.parse_column_ref()?;
            let dir = if p.if_peek_then_consume(TokenType::Desc)? {
                OrderDirection::Desc
            } else {
                p.on_peek_token(TokenType::Asc, |_p| Ok(()))?;
                OrderDirection::Asc
            };
            Ok(OrderBy(col, dir))
        };

        let keys = self
            .on_peek_token(TokenType::Order, |p| {
                p.expect(TokenType::By)?;
                let mut keys = vec![parse_key(p)?];
                while p.if_peek_then_consume(TokenType::Comma)? {
                    keys.push(parse_key(p)?);
                }
                Ok(keys)
            })?
            .unwrap_or_default();

        debug!(key_count = keys.len(), "parsed ORDER BY clause");
        Ok(keys)
    }

    /// Parses an optional `GROUP BY` clause with one or more grouping columns.
    ///
    /// Columns are comma-separated. If the next token is not `GROUP`, returns
    /// an empty vec without consuming input — empty encodes "no clause".
    ///
    /// ```text
    /// <group_by> ::= "GROUP" "BY" <column> ( "," <column> )*
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if `GROUP` is present but `BY` does not follow
    /// it, or if any column identifier in the list is missing or malformed.
    #[instrument(
        skip(self),
        fields(component = "parser", clause = "group_by"),
        err(Debug)
    )]
    fn parse_group_by(&mut self) -> Result<Vec<ColumnRef>, ParserError> {
        let cols = self
            .on_peek_token(TokenType::Group, |p| {
                p.expect(TokenType::By)?;
                let mut cols = vec![p.parse_column_ref()?];
                while p.if_peek_then_consume(TokenType::Comma)? {
                    cols.push(p.parse_column_ref()?);
                }
                Ok(cols)
            })?
            .unwrap_or_default();

        debug!(column_count = cols.len(), "parsed GROUP BY clause");
        Ok(cols)
    }

    /// Parses an optional `LIMIT` / `OFFSET` tail.
    ///
    /// Three accepted forms:
    /// - `LIMIT n`              → `LimitClause { limit: Some(n), offset: 0 }`
    /// - `LIMIT n OFFSET m`     → `LimitClause { limit: Some(n), offset: m }`
    /// - `OFFSET m`             → `LimitClause { limit: None,    offset: m }`
    ///
    /// If neither keyword is the next token, returns [`None`] without
    /// consuming input. Note that `LIMIT 0` (return zero rows) is a distinct
    /// state from "no limit" — the AST encodes the difference via
    /// `Option<u64>` so the executor cannot conflate them.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] when a numeric value following `LIMIT` or
    /// `OFFSET` is not an [`TokenType::Int`] or does not fit in `u64`.
    #[instrument(
        skip(self),
        fields(component = "parser", clause = "limit_offset"),
        err(Debug)
    )]
    fn parse_limit_offset(&mut self) -> Result<Option<LimitClause>, ParserError> {
        let parse_int = |name: &str, p: &mut Parser| {
            p.expect(TokenType::Int)?.value.parse::<u64>().map_err(|e| {
                warn!(kind = name, reason = %e, "invalid LIMIT/OFFSET literal");
                ParserError::ParsingError(format!(
                    "invalid {name}: {e} expected non-negative integer like 1, 2, 3, etc."
                ))
            })
        };

        if self.if_peek_then_consume(TokenType::Limit)? {
            let limit = parse_int("limit", self)?;
            let offset = self
                .on_peek_token(TokenType::Offset, |p| parse_int("offset", p))?
                .unwrap_or(0);
            debug!(limit, offset, "parsed LIMIT/OFFSET clause");
            return Ok(Some(LimitClause {
                limit: Some(limit),
                offset,
            }));
        }

        if self.if_peek_then_consume(TokenType::Offset)? {
            let offset = parse_int("offset", self)?;
            debug!(offset, "parsed OFFSET-only clause");
            return Ok(Some(LimitClause {
                limit: None,
                offset,
            }));
        }

        trace!("no LIMIT/OFFSET clause");
        Ok(None)
    }

    /// Parses zero or more JOIN clauses following a `FROM` target.
    ///
    /// Supports `[INNER] JOIN`, `LEFT JOIN`, and `RIGHT JOIN`.  Each clause
    /// must include an `ON <condition>` predicate.  Parsing stops when no
    /// recognized join keyword is found next.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError`] if a join keyword is present but the rest of the
    /// clause (`JOIN <table> ON <condition>`) is malformed.
    #[instrument(skip(self), fields(component = "parser", clause = "joins"), err(Debug))]
    fn parse_joins(&mut self) -> Result<Vec<Join>, ParserError> {
        let mut joins = vec![];

        loop {
            let kind = if self.if_peek_then_consume(TokenType::Inner)? {
                self.expect(TokenType::Join)?;
                JoinKind::Inner
            } else if self.if_peek_then_consume(TokenType::Left)? {
                self.expect(TokenType::Join)?;
                JoinKind::Left
            } else if self.if_peek_then_consume(TokenType::Right)? {
                self.expect(TokenType::Join)?;
                JoinKind::Right
            } else if self.if_peek_then_consume(TokenType::Join)? {
                JoinKind::Inner
            } else {
                break;
            };

            let (table, alias) = self.parse_table_with_alias()?;
            self.expect(TokenType::On)?;
            let on = self.parse_where()?;

            joins.push(Join {
                kind,
                table: TableRef { name: table, alias },
                on,
            });
        }

        debug!(join_count = joins.len(), "parsed JOIN chain");
        Ok(joins)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        parser::{
            Parser,
            parsers::{
                ParserError,
                expr::{BinOp, Expr},
            },
            statements::{
                AggFunc, ColumnRef, JoinKind, LimitClause, OrderBy, OrderDirection, SelectColumns,
                SelectItem, SelectStatement, Statement,
            },
        },
        primitives::NonEmptyString,
        types::Value,
    };

    fn select(sql: &str) -> Result<SelectStatement, ParserError> {
        match Parser::new(sql).parse()? {
            Statement::Select(s) => Ok(s),
            Statement::Delete(_) => panic!("expected Statement::Select, got Delete"),
            Statement::Insert(_) => panic!("expected Statement::Select, got Insert"),
            Statement::Update(_) => panic!("expected Statement::Select, got Update"),
            Statement::ShowIndexes(_) => panic!("expected Statement::Select, got ShowIndexes"),
            Statement::Drop(_) => panic!("expected Statement::Select, got Drop"),
            Statement::DropIndex(_) => panic!("expected Statement::Select, got DropIndex"),
            Statement::CreateIndex(_) => panic!("expected Statement::Select, got CreateIndex"),
            Statement::CreateTable(_) => panic!("expected Statement::Select, got CreateTable"),
            Statement::AlterTable(_) => panic!("expected Statement::Select, got AlterTable"),
        }
    }

    #[test]
    fn test_parse_select_star_from_table() {
        let s = select("SELECT * FROM users").unwrap();
        assert!(!s.distinct);
        assert!(matches!(s.columns, SelectColumns::All));
        assert_eq!(s.from[0].table.name, "users");
        assert!(s.from[0].table.alias.is_none());
        assert!(s.from[0].joins.is_empty());
        assert!(s.where_clause.is_none());
        assert!(s.group_by.is_empty());
        assert!(s.order_by.is_empty());
        assert_eq!(s.limit, None);
        assert!(s.having.is_none());
    }

    #[test]
    fn test_parse_select_distinct_all_columns() {
        let s = select("SELECT DISTINCT * FROM items").unwrap();
        assert!(s.distinct);
        assert!(matches!(s.columns, SelectColumns::All));
        assert_eq!(s.from[0].table.name, "items");
    }

    #[test]
    fn test_parse_select_single_column_projection() {
        let s = select("SELECT id FROM orders").unwrap();
        match &s.columns {
            SelectColumns::Exprs(v) => {
                assert_eq!(v, &vec![SelectItem::bare(Expr::Column("id".into()))]);
            }
            SelectColumns::All => panic!("expected Exprs, got All"),
        }
    }

    #[test]
    fn test_parse_select_multiple_columns_and_delimiters() {
        let s = select("SELECT a, b, c FROM t").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v, &vec![
            SelectItem::bare(Expr::Column("a".into())),
            SelectItem::bare(Expr::Column("b".into())),
            SelectItem::bare(Expr::Column("c".into())),
        ]);
    }

    #[test]
    fn test_parse_select_count_star() {
        let s = select("SELECT COUNT(*) FROM events").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v, &vec![SelectItem::bare(Expr::CountStar)]);
    }

    #[test]
    fn test_parse_select_count_column_aggregate() {
        let s = select("SELECT COUNT(user_id) FROM events").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v, &vec![SelectItem::bare(Expr::Agg {
            func: AggFunc::Count,
            arg: Box::new(Expr::Column("user_id".into())),
        })]);
    }

    #[test]
    fn test_parse_select_sum_case_insensitive() {
        let s = select("SELECT sum(amount) FROM ledger").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v, &vec![SelectItem::bare(Expr::Agg {
            func: AggFunc::Sum,
            arg: Box::new(Expr::Column("amount".into())),
        })]);
    }

    #[test]
    fn test_parse_select_all_agg_variants() {
        let sql = "SELECT SUM(x), AVG(x), MIN(x), MAX(x) FROM t";
        let s = select(sql).unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v.len(), 4);
        let x = || Expr::Column("x".into());
        assert_eq!(v[0].expr, Expr::Agg {
            func: AggFunc::Sum,
            arg: Box::new(x()),
        });
        assert_eq!(v[1].expr, Expr::Agg {
            func: AggFunc::Avg,
            arg: Box::new(x()),
        });
        assert_eq!(v[2].expr, Expr::Agg {
            func: AggFunc::Min,
            arg: Box::new(x()),
        });
        assert_eq!(v[3].expr, Expr::Agg {
            func: AggFunc::Max,
            arg: Box::new(x()),
        });
        assert!(v.iter().all(|item| item.alias.is_none()));
    }

    #[test]
    fn test_parse_select_from_table_alias() {
        let s = select("SELECT * FROM users u").unwrap();
        assert_eq!(s.from[0].table.name, "users");
        assert_eq!(s.from[0].table.alias.as_deref(), Some("u"));
    }

    #[test]
    fn test_parse_select_inner_join_explicit_on_condition() {
        let s = select("SELECT * FROM a INNER JOIN b ON id = 1").unwrap();
        assert_eq!(s.from[0].joins.len(), 1);
        let j = &s.from[0].joins[0];
        assert_eq!(j.kind, JoinKind::Inner);
        assert_eq!(j.table.name, "b");
        assert!(j.table.alias.is_none());
        let Expr::BinaryOp { lhs, op, rhs } = &j.on else {
            panic!("expected binary ON expression");
        };
        assert_eq!(**lhs, Expr::Column(ColumnRef::from("id")));
        assert_eq!(*op, BinOp::Eq);
        assert_eq!(**rhs, Expr::Literal(Value::Int64(1)));
    }

    #[test]
    fn test_parse_select_bare_join_defaults_to_inner() {
        let s = select("SELECT * FROM a JOIN b ON x = 2").unwrap();
        assert_eq!(s.from[0].joins.len(), 1);
        assert_eq!(s.from[0].joins[0].kind, JoinKind::Inner);
        assert_eq!(s.from[0].joins[0].table.name, "b");
    }

    #[test]
    fn test_parse_select_left_and_right_join() {
        let s = select("SELECT * FROM a LEFT JOIN b ON i = 0 RIGHT JOIN c ON j = 3").unwrap();
        assert_eq!(s.from[0].joins.len(), 2);
        assert_eq!(s.from[0].joins[0].kind, JoinKind::Left);
        assert_eq!(s.from[0].joins[0].table.name, "b");
        assert_eq!(s.from[0].joins[1].kind, JoinKind::Right);
        assert_eq!(s.from[0].joins[1].table.name, "c");
    }

    #[test]
    fn test_parse_select_join_with_table_alias() {
        let s = select("SELECT * FROM orders o JOIN customers c ON cid = 1").unwrap();
        assert_eq!(s.from[0].joins[0].table.alias.as_deref(), Some("c"));
        assert_eq!(s.from[0].table.name, "orders");
        assert_eq!(s.from[0].table.alias.as_deref(), Some("o"));
    }

    #[test]
    fn test_parse_select_where_optional() {
        let s = select("SELECT * FROM t WHERE status = 1").unwrap();
        let wc = s.where_clause.as_ref().unwrap();
        let Expr::BinaryOp { lhs, op, rhs } = wc else {
            panic!("expected binary WHERE expression");
        };
        assert_eq!(**lhs, Expr::Column(ColumnRef::from("status")));
        assert_eq!(*op, BinOp::Eq);
        assert_eq!(**rhs, Expr::Literal(Value::Int64(1)));
    }

    #[test]
    fn test_parse_select_where_float_literal() {
        let s = select("SELECT * FROM t WHERE price = 2.5").unwrap();
        let wc = s.where_clause.as_ref().unwrap();
        let Expr::BinaryOp { lhs, op, rhs } = wc else {
            panic!("expected binary WHERE expression");
        };
        assert_eq!(**lhs, Expr::Column(ColumnRef::from("price")));
        assert_eq!(*op, BinOp::Eq);
        assert_eq!(**rhs, Expr::Literal(Value::Float64(2.5)));
    }

    #[test]
    fn test_parse_select_where_string_literal() {
        let s = select("SELECT * FROM t WHERE name = 'alice'").unwrap();
        let wc = s.where_clause.as_ref().unwrap();
        let Expr::BinaryOp { op, rhs, .. } = wc else {
            panic!("expected binary WHERE expression");
        };
        assert_eq!(*op, BinOp::Eq);
        assert_eq!(**rhs, Expr::Literal(Value::String("alice".to_string())));
    }

    #[test]
    fn test_parse_select_where_null_literal() {
        let s = select("SELECT * FROM t WHERE deleted_at = NULL").unwrap();
        let wc = s.where_clause.as_ref().unwrap();
        let Expr::BinaryOp { op, rhs, .. } = wc else {
            panic!("expected binary WHERE expression");
        };
        assert_eq!(*op, BinOp::Eq);
        assert_eq!(**rhs, Expr::Literal(Value::Null));
    }

    #[test]
    fn test_parse_select_where_bool_literal() {
        let s = select("SELECT * FROM t WHERE active = true").unwrap();
        let wc = s.where_clause.as_ref().unwrap();
        let Expr::BinaryOp { op, rhs, .. } = wc else {
            panic!("expected binary WHERE expression");
        };
        assert_eq!(*op, BinOp::Eq);
        assert_eq!(**rhs, Expr::Literal(Value::Bool(true)));
    }

    #[test]
    fn test_parse_select_where_and_or_structure() {
        let s = select("SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3").unwrap();
        let wc = s.where_clause.as_ref().unwrap();
        // Grammar: AND binds tighter than OR → (a=1) OR ((b=2) AND (c=3))
        let Expr::BinaryOp { op, lhs, rhs } = wc else {
            panic!("expected binary WHERE expression");
        };
        assert_eq!(*op, BinOp::Or);
        assert!(matches!(**lhs, Expr::BinaryOp { .. }));
        let Expr::BinaryOp { op: rhs_op, .. } = &**rhs else {
            panic!("expected AND on rhs");
        };
        assert_eq!(*rhs_op, BinOp::And);
    }

    #[test]
    fn test_parse_select_group_by_single_column() {
        let s = select("SELECT a FROM t GROUP BY a").unwrap();
        assert_eq!(s.group_by, vec!["a".into()]);
    }

    #[test]
    fn test_parse_select_order_by_default_asc() {
        let s = select("SELECT * FROM t ORDER BY name").unwrap();
        assert_eq!(s.order_by.len(), 1);
        let OrderBy(col, dir) = &s.order_by[0];
        assert_eq!(col, &ColumnRef::from("name"));
        assert_eq!(*dir, OrderDirection::Asc);
    }

    #[test]
    fn test_parse_select_order_by_desc() {
        let s = select("SELECT * FROM t ORDER BY score DESC").unwrap();
        assert_eq!(s.order_by.len(), 1);
        let OrderBy(col, dir) = &s.order_by[0];
        assert_eq!(col, &ColumnRef::from("score"));
        assert_eq!(*dir, OrderDirection::Desc);
    }

    #[test]
    fn test_parse_select_order_by_explicit_asc() {
        let s = select("SELECT * FROM t ORDER BY z ASC").unwrap();
        assert_eq!(s.order_by.len(), 1);
        let OrderBy(col, dir) = &s.order_by[0];
        assert_eq!(col, &ColumnRef::from("z"));
        assert_eq!(*dir, OrderDirection::Asc);
    }

    #[test]
    fn test_parse_select_limit_only() {
        let s = select("SELECT * FROM t LIMIT 10").unwrap();
        assert_eq!(
            s.limit,
            Some(LimitClause {
                limit: Some(10),
                offset: 0
            })
        );
    }

    #[test]
    fn test_parse_select_limit_with_offset() {
        let s = select("SELECT * FROM t LIMIT 5 OFFSET 2").unwrap();
        assert_eq!(
            s.limit,
            Some(LimitClause {
                limit: Some(5),
                offset: 2
            })
        );
    }

    #[test]
    fn test_parse_select_full_clause_chain() {
        let s = select(
            "SELECT DISTINCT x, SUM(y) FROM u uu INNER JOIN v ON a = 1 WHERE b = 2 GROUP BY x ORDER BY x DESC LIMIT 3 OFFSET 1",
        )
        .unwrap();
        assert!(s.distinct);
        assert_eq!(s.from[0].table.alias.as_deref(), Some("uu"));
        assert_eq!(s.from[0].joins.len(), 1);
        assert_eq!(s.group_by, vec!["x".into()]);
        assert_eq!(s.order_by.len(), 1);
        let OrderBy(col, dir) = &s.order_by[0];
        assert_eq!(col, &ColumnRef::from("x"));
        assert_eq!(*dir, OrderDirection::Desc);
        assert_eq!(
            s.limit,
            Some(LimitClause {
                limit: Some(3),
                offset: 1
            })
        );
    }

    #[test]
    fn test_parse_select_column_vs_function_by_paren() {
        // No `(` → column reference preserves lexer casing
        let s = select("SELECT count FROM t").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v, &vec![SelectItem::bare(Expr::Column("count".into()))]);
    }

    #[test]
    fn test_parse_select_integer_literal_projection() {
        let s = select("SELECT 1 FROM dual").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v[0].expr, Expr::Literal(Value::Int64(1)));
        assert!(v[0].alias.is_none());
    }

    #[test]
    fn test_parse_select_string_literal_projection() {
        let s = select("SELECT 'hello' FROM t").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v[0].expr, Expr::Literal(Value::String("hello".into())));
    }

    #[test]
    fn test_parse_select_null_literal_projection() {
        let s = select("SELECT NULL FROM t").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v[0].expr, Expr::Literal(Value::Null));
    }

    #[test]
    fn test_parse_select_float_literal_projection() {
        let s = select("SELECT 3.12 FROM t").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v[0].expr, Expr::Literal(Value::Float64(3.12)));
    }

    #[test]
    fn test_parse_select_literal_with_alias() {
        let s = select("SELECT 1 AS one, 'hi' greeting FROM t").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].expr, Expr::Literal(Value::Int64(1)));
        assert_eq!(v[0].alias.as_deref(), Some("one"));
        assert_eq!(v[1].expr, Expr::Literal(Value::String("hi".into())));
        assert_eq!(v[1].alias.as_deref(), Some("greeting"));
    }

    #[test]
    fn test_parse_select_mixed_literals_and_columns() {
        let s = select("SELECT id, 1, name FROM users").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v.len(), 3);
        assert_eq!(v[0].expr, Expr::Column("id".into()));
        assert_eq!(v[1].expr, Expr::Literal(Value::Int64(1)));
        assert_eq!(v[2].expr, Expr::Column("name".into()));
    }

    #[test]
    fn test_parse_select_missing_from_keyword() {
        assert!(select("SELECT *").is_err());
    }

    #[test]
    fn test_parse_select_unknown_aggregate_function() {
        let e = select("SELECT MEDIAN(x) FROM t").unwrap_err();
        match e {
            ParserError::ParsingError(msg) => {
                assert!(msg.contains("aggregate") || msg.contains("MEDIAN"));
            }
            _ => panic!("expected ParsingError, got {e:?}"),
        }
    }

    #[test]
    fn test_parse_select_projection_list_bad_delimiter() {
        // `SELECT a b FROM t` is now valid (implicit alias), so test with a
        // non-identifier token that can't be an alias.
        assert!(select("SELECT a 1 FROM t").is_err());
    }

    #[test]
    fn test_parse_select_order_by_missing_by() {
        assert!(select("SELECT * FROM t ORDER name").is_err());
    }

    #[test]
    fn test_parse_select_limit_requires_integer() {
        assert!(select("SELECT * FROM t LIMIT foo").is_err());
    }

    #[test]
    fn test_parse_select_limit_offset_requires_integer() {
        assert!(select("SELECT * FROM t LIMIT 1 OFFSET bar").is_err());
    }

    #[test]
    fn test_parse_select_offset_only() {
        let s = select("SELECT * FROM t OFFSET 5").unwrap();
        assert_eq!(
            s.limit,
            Some(LimitClause {
                limit: None,
                offset: 5
            })
        );
    }

    #[test]
    fn test_parse_select_limit_zero_distinct_from_no_limit() {
        // `LIMIT 0` (return zero rows) must be representable separately from
        // "no limit at all." The AST encodes the difference via Option<u64>.
        let zero = select("SELECT * FROM t LIMIT 0").unwrap();
        assert_eq!(
            zero.limit,
            Some(LimitClause {
                limit: Some(0),
                offset: 0
            })
        );

        let none = select("SELECT * FROM t").unwrap();
        assert_eq!(none.limit, None);

        let offset_only = select("SELECT * FROM t OFFSET 3").unwrap();
        assert!(offset_only.limit.unwrap().limit.is_none());
    }

    #[test]
    fn test_parse_select_offset_requires_integer() {
        assert!(select("SELECT * FROM t OFFSET foo").is_err());
    }

    #[test]
    fn test_parse_select_malformed_count_star_missing_paren() {
        assert!(select("SELECT COUNT(* FROM t").is_err());
    }

    #[test]
    fn test_parse_select_join_missing_on() {
        assert!(select("SELECT * FROM a JOIN b").is_err());
    }

    #[test]
    fn test_parse_select_alias_with_as() {
        let s = select("SELECT a AS x FROM t").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v[0].expr, Expr::Column("a".into()));
        assert_eq!(v[0].alias.as_deref(), Some("x"));
    }

    #[test]
    fn test_parse_select_alias_without_as() {
        let s = select("SELECT a x FROM t").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v[0].expr, Expr::Column("a".into()));
        assert_eq!(v[0].alias.as_deref(), Some("x"));
    }

    #[test]
    fn test_parse_select_alias_on_aggregate() {
        let s = select("SELECT SUM(amt) AS total FROM t").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v[0].expr, Expr::Agg {
            func: AggFunc::Sum,
            arg: Box::new(Expr::Column("amt".into())),
        });
        assert_eq!(v[0].alias.as_deref(), Some("total"));
    }

    #[test]
    fn test_parse_select_alias_on_count_star() {
        let s = select("SELECT COUNT(*) AS n FROM t").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v[0].expr, Expr::CountStar);
        assert_eq!(v[0].alias.as_deref(), Some("n"));
    }

    #[test]
    fn test_parse_select_alias_on_qualified_column() {
        let s = select("SELECT u.name AS who FROM users u").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(
            v[0].expr,
            Expr::Column(ColumnRef {
                qualifier: Some(NonEmptyString::new("u").unwrap()),
                name: NonEmptyString::new("name").unwrap()
            })
        );
        assert_eq!(v[0].alias.as_deref(), Some("who"));
    }

    #[test]
    fn test_parse_select_mixed_aliased_and_bare() {
        let s = select("SELECT a, b AS bb, c cc FROM t").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert_eq!(v.len(), 3);
        assert!(v[0].alias.is_none());
        assert_eq!(v[1].alias.as_deref(), Some("bb"));
        assert_eq!(v[2].alias.as_deref(), Some("cc"));
    }

    #[test]
    fn test_parse_select_as_without_alias_errors() {
        assert!(select("SELECT a AS FROM t").is_err());
    }

    #[test]
    fn test_parse_select_group_by_multiple_columns() {
        let s = select("SELECT a, b FROM t GROUP BY a, b").unwrap();
        assert_eq!(s.group_by, vec!["a".into(), "b".into()]);
    }

    #[test]
    fn test_parse_select_group_by_qualified_columns() {
        let s = select("SELECT * FROM t GROUP BY t.a, t.b").unwrap();
        assert_eq!(s.group_by.len(), 2);
        assert_eq!(s.group_by[0].qualifier.as_deref(), Some("t"));
        assert_eq!(s.group_by[0].name, "a");
        assert_eq!(s.group_by[1].qualifier.as_deref(), Some("t"));
        assert_eq!(s.group_by[1].name, "b");
    }

    #[test]
    fn test_parse_select_group_by_trailing_comma_errors() {
        // `GROUP BY a,` should fail — there must be a column after the comma.
        assert!(select("SELECT a FROM t GROUP BY a,").is_err());
    }

    #[test]
    fn test_parse_select_order_by_multiple_keys() {
        // Mixed directions, in priority order: a ASC, b DESC, c ASC (default).
        let s = select("SELECT * FROM t ORDER BY a, b DESC, c").unwrap();
        assert_eq!(s.order_by.len(), 3);
        assert_eq!(s.order_by[0].0, ColumnRef::from("a"));
        assert_eq!(s.order_by[0].1, OrderDirection::Asc);
        assert_eq!(s.order_by[1].0, ColumnRef::from("b"));
        assert_eq!(s.order_by[1].1, OrderDirection::Desc);
        assert_eq!(s.order_by[2].0, ColumnRef::from("c"));
        assert_eq!(s.order_by[2].1, OrderDirection::Asc);
    }

    #[test]
    fn test_parse_select_order_by_qualified_keys() {
        let s = select("SELECT * FROM users u ORDER BY u.name ASC, u.id DESC").unwrap();
        assert_eq!(s.order_by.len(), 2);
        assert_eq!(s.order_by[0].0.qualifier.as_deref(), Some("u"));
        assert_eq!(s.order_by[0].0.name, "name");
        assert_eq!(s.order_by[1].0.qualifier.as_deref(), Some("u"));
        assert_eq!(s.order_by[1].1, OrderDirection::Desc);
    }

    #[test]
    fn test_parse_select_order_by_multi_key_with_limit() {
        // The trailing LIMIT terminates the ORDER BY list cleanly.
        let s = select("SELECT * FROM t ORDER BY a, b DESC LIMIT 5").unwrap();
        assert_eq!(s.order_by.len(), 2);
        assert_eq!(s.limit.as_ref().unwrap().limit, Some(5));
    }

    #[test]
    fn test_parse_select_order_by_trailing_comma_errors() {
        assert!(select("SELECT * FROM t ORDER BY a,").is_err());
    }

    #[test]
    fn test_parse_select_having_simple_predicate() {
        // HAVING with a column predicate. Aggregate predicates like
        // `HAVING SUM(x) > 10` will land once aggregate calls are accepted in
        // predicate expressions.
        let s = select("SELECT a FROM t GROUP BY a HAVING a = 1").unwrap();
        let h = s.having.as_ref().expect("expected having");
        let Expr::BinaryOp { lhs, op, rhs } = h else {
            panic!("expected predicate");
        };
        assert_eq!(lhs.as_ref(), &Expr::Column(ColumnRef::from("a")));
        assert_eq!(*op, BinOp::Eq);
        assert_eq!(rhs.as_ref(), &Expr::Literal(Value::Int64(1)));
    }

    #[test]
    fn test_parse_select_having_reuses_where_boolean_grammar() {
        // AND binds tighter than OR — same as WHERE.
        let s = select("SELECT a FROM t GROUP BY a HAVING a = 1 OR b = 2 AND c = 3").unwrap();
        let h = s.having.as_ref().unwrap();
        let Expr::BinaryOp {
            op: BinOp::Or, rhs, ..
        } = h
        else {
            panic!("expected Or at top");
        };
        assert!(matches!(rhs.as_ref(), Expr::BinaryOp {
            op: BinOp::And,
            ..
        }));
    }

    #[test]
    fn test_parse_select_having_without_group_by_is_parser_ok() {
        // The parser accepts HAVING without GROUP BY; rejecting that
        // combination is the binder's job (it needs aggregate context).
        let s = select("SELECT a FROM t HAVING a = 1").unwrap();
        assert!(s.having.is_some());
        assert!(s.group_by.is_empty());
    }

    #[test]
    fn test_parse_select_having_sits_between_group_by_and_order_by() {
        let s = select("SELECT a FROM t GROUP BY a HAVING a = 1 ORDER BY a DESC LIMIT 5").unwrap();
        assert!(s.having.is_some());
        assert_eq!(s.order_by.len(), 1);
        let OrderBy(col, dir) = &s.order_by[0];
        assert_eq!(col, &ColumnRef::from("a"));
        assert_eq!(*dir, OrderDirection::Desc);
        assert_eq!(s.limit.as_ref().unwrap().limit, Some(5));
    }

    #[test]
    fn test_parse_select_having_missing_predicate_errors() {
        assert!(select("SELECT a FROM t GROUP BY a HAVING").is_err());
    }

    #[test]
    fn test_parse_select_alias_does_not_swallow_keyword() {
        // 'WHERE' is a reserved keyword, not an Identifier, so it must not be
        // consumed as an alias for `a`.
        let s = select("SELECT a FROM t WHERE a = 1").unwrap();
        let SelectColumns::Exprs(v) = &s.columns else {
            panic!("expected Exprs");
        };
        assert!(v[0].alias.is_none());
        assert!(s.where_clause.is_some());
    }
}
