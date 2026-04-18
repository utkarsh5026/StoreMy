use crate::parser::{
    Parser,
    statements::{
        AggFunc, Join, JoinKind, OrderBy, OrderDirection, SelectColumns, SelectExpr,
        SelectStatement,
    },
    token::TokenType,
};

use super::ParserError;

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
    pub(super) fn parse_select(&mut self) -> Result<SelectStatement, ParserError> {
        self.ensure_next_of_kind(TokenType::Select)?;

        let distinct = self
            .on_peek_token(TokenType::Distinct, |_p| Ok(true))?
            .unwrap_or(false);

        let columns = if self.is_peek_token(TokenType::Asterisk)? {
            self.ensure_next_of_kind(TokenType::Asterisk)?;
            SelectColumns::All
        } else {
            let select_expressions =
                self.parse_delimited_list(TokenType::Comma, TokenType::From, |p| {
                    let name_tok = p.ensure_next_of_kind(TokenType::Identifier)?;
                    let name = name_tok.value.to_uppercase();

                    if p.is_peek_token(TokenType::Lparen)? {
                        p.ensure_next_of_kind(TokenType::Lparen)?;

                        if name == "COUNT" && p.is_peek_token(TokenType::Asterisk)? {
                            p.ensure_next_of_kind(TokenType::Asterisk)?;
                            p.ensure_next_of_kind(TokenType::Rparen)?;
                            return Ok(SelectExpr::CountStar);
                        }

                        let agg =
                            AggFunc::try_from(name.as_str()).map_err(ParserError::ParsingError)?;

                        let col_tok = p.ensure_next_of_kind(TokenType::Identifier)?;
                        p.ensure_next_of_kind(TokenType::Rparen)?;
                        Ok(SelectExpr::Agg(agg, col_tok.value))
                    } else {
                        Ok(SelectExpr::Column(name_tok.value))
                    }
                })?;
            SelectColumns::Exprs(select_expressions)
        };

        self.ensure_next_of_kind(TokenType::From)?;
        let (table_name, alias) = self.parse_table_with_alias()?;

        let joins = self.parse_joins()?;

        let where_clause = self.on_peek_token(TokenType::Where, Parser::parse_where)?;

        let group_by = self
            .on_peek_token(TokenType::Group, |p| {
                p.ensure_next_of_kind(TokenType::By)?;
                let column = p.ensure_next_of_kind(TokenType::Identifier)?;
                Ok(vec![column.value])
            })?
            .unwrap_or_default();

        let order_by = self.on_peek_token(TokenType::Order, |p| {
            p.ensure_next_of_kind(TokenType::By)?;
            let order_col = p.ensure_next_of_kind(TokenType::Identifier)?;

            let dir = if p.is_peek_token(TokenType::Desc)? {
                p.ensure_next_of_kind(TokenType::Desc)?;
                OrderDirection::Desc
            } else {
                p.on_peek_token(TokenType::Asc, |_p| Ok(()))?;
                OrderDirection::Asc
            };

            Ok(OrderBy(order_col.value, dir))
        })?;

        let limit_offset = self.on_peek_token(TokenType::Limit, |p| {
            let limit = p.parse_int("limit")?;

            let offset = p
                .on_peek_token(TokenType::Offset, |p| p.parse_int("offset"))?
                .unwrap_or(0);

            Ok((limit, offset))
        })?;

        Ok(SelectStatement {
            distinct,
            columns,
            table_name,
            alias,
            joins,
            where_clause,
            group_by,
            having: None,
            order_by,
            limit_offset,
        })
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
    fn parse_joins(&mut self) -> Result<Vec<Join>, ParserError> {
        let mut joins = vec![];

        loop {
            let kind = if self.is_peek_token(TokenType::Inner)? {
                self.ensure_next_of_kind(TokenType::Inner)?;
                JoinKind::Inner
            } else if self.is_peek_token(TokenType::Left)? {
                self.ensure_next_of_kind(TokenType::Left)?;
                JoinKind::Left
            } else if self.is_peek_token(TokenType::Right)? {
                self.ensure_next_of_kind(TokenType::Right)?;
                JoinKind::Right
            } else if self.is_peek_token(TokenType::Join)? {
                JoinKind::Inner
            } else {
                break;
            };

            self.ensure_next_of_kind(TokenType::Join)?;
            let (table, alias) = self.parse_table_with_alias()?;
            self.ensure_next_of_kind(TokenType::On)?;
            let on = self.parse_where()?;

            joins.push(Join {
                kind,
                table,
                alias,
                on,
            });
        }

        Ok(joins)
    }

    /// Consumes the next token and parses it as a non-negative integer.
    ///
    /// `name` is used only in the error message to identify which integer was
    /// expected (e.g. `"limit"` or `"offset"`).
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::UnexpectedToken`] if the token is not an
    /// [`TokenType::Int`], or [`ParserError::ParsingError`] if the integer
    /// value overflows `u64`.
    fn parse_int(&mut self, name: &str) -> Result<u64, ParserError> {
        let tok = self.ensure_next_of_kind(TokenType::Int)?;
        tok.value
            .parse::<u64>()
            .map_err(|e| ParserError::ParsingError(format!("invalid {name}: {e}")))
    }
}
