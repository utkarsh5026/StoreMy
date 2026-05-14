use crate::parser::lexer::Lexer;

mod lexer;
mod parsers;
pub mod statements;
pub use parsers::ParserError;
mod token;

pub struct Parser {
    lexer: Lexer,
}

impl Parser {
    pub fn new(input: &str) -> Self {
        Parser {
            lexer: Lexer::new(input),
        }
    }
}

#[cfg(test)]
mod tests {
    //! End-to-end tests for [`Parser::parse`].
    //!
    //! These tests exercise every arm of the top-level dispatcher in
    //! [`parsers::mod::Parser::parse`], going through the public
    //! `Parser::new(sql).parse()` entry point. They are deliberately shallow
    //! per statement type — the submodule tests (`ddl`, `dml`, `query`,
    //! `lexer`, `token`) cover clause-level variations in depth.
    use super::*;
    use crate::{
        parser::statements::{
            AggFunc, BinOp, Expr, JoinKind, OrderDirection, ReferentialAction, SelectColumns,
            Statement, TableConstraint, Uniqueness,
        },
        primitives::NonEmptyString,
    };

    fn parse(sql: &str) -> Statement {
        Parser::new(sql)
            .parse()
            .unwrap_or_else(|e| panic!("parse failed for {sql:?}: {e:?}"))
    }

    #[test]
    fn parse_dispatches_create_table() {
        let stmt = parse("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR NOT NULL)");
        let Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable, got {stmt}");
        };
        assert_eq!(ct.table_name, "users");
        assert!(!ct.if_not_exists);
        assert_eq!(ct.columns.len(), 2);
        assert_eq!(ct.columns[0].name, "id");
        assert!(ct.columns[0].primary_key);
        assert_eq!(ct.columns[1].name, "name");
        assert!(!ct.columns[1].nullable);
    }

    #[test]
    fn parse_dispatches_create_table_if_not_exists() {
        let stmt = parse("CREATE TABLE IF NOT EXISTS t (id INT)");
        let Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable, got {stmt}");
        };
        assert_eq!(ct.table_name, "t");
        assert!(ct.if_not_exists);
    }

    #[test]
    fn parse_dispatches_create_table_column_unique_and_references() {
        let stmt = parse(concat!(
            "CREATE TABLE p (",
            "id INT, ",
            "user_id INT UNIQUE REFERENCES users(id) ON DELETE CASCADE",
            ")",
        ));
        let Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable, got {stmt}");
        };

        assert_eq!(ct.table_name, "p");
        assert_eq!(ct.columns.len(), 2);

        let col = &ct.columns[1];
        assert_eq!(col.name, "user_id");
        assert_eq!(col.unique, Uniqueness::Unique);

        let r = col.references.as_ref().expect("references");
        assert_eq!(r.table, "users");
        assert_eq!(r.column, "id");
        assert!(matches!(r.on_delete, Some(ReferentialAction::Cascade)));
    }

    #[test]
    fn parse_dispatches_create_table_table_level_unique_and_foreign_key() {
        let stmt = parse(concat!(
            "CREATE TABLE t (",
            "id INT, ",
            "x INT, ",
            "UNIQUE (x), ",
            "FOREIGN KEY (x) REFERENCES parent(id)",
            ")",
        ));
        let Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable, got {stmt}");
        };

        assert_eq!(ct.table_name, "t");
        assert!(ct
            .constraints
            .iter()
            .any(|(_, c)| matches!(c, TableConstraint::Unique { columns } if columns.as_slice() == [NonEmptyString::new("x").unwrap()].as_slice())));

        assert!(ct.constraints.iter().any(|(_, c)| matches!(
            c,
            TableConstraint::ForeignKey {
                local_cols,
                ref_table,
                ref_cols,
                on_delete: _,
                on_update: _,
            } if local_cols.as_slice() == [NonEmptyString::new("x").unwrap()].as_slice()
                && ref_table.as_str() == "parent"
                && ref_cols.as_slice() == [NonEmptyString::new("id").unwrap()].as_slice()
        )));
    }

    #[test]
    fn parse_dispatches_drop_table() {
        let stmt = parse("DROP TABLE IF EXISTS users");
        let Statement::Drop(d) = stmt else {
            panic!("expected Drop, got {stmt}");
        };
        assert_eq!(d.table_name, "users");
        assert!(d.if_exists);
    }

    #[test]
    fn parse_dispatches_create_index() {
        let stmt = parse("CREATE INDEX idx_name ON users (name) USING HASH");
        assert!(matches!(stmt, Statement::CreateIndex(_)));
    }

    #[test]
    fn parse_dispatches_create_index_if_not_exists() {
        let stmt = parse("CREATE INDEX IF NOT EXISTS idx ON t (c) USING BTREE");
        let Statement::CreateIndex(c) = stmt else {
            panic!("expected CreateIndex");
        };
        assert!(c.if_not_exists);
    }

    #[test]
    fn parse_dispatches_drop_index() {
        let stmt = parse("DROP INDEX IF EXISTS idx_name ON users");
        assert!(matches!(stmt, Statement::DropIndex(_)));
    }

    #[test]
    fn parse_dispatches_show_indexes() {
        let stmt = parse("SHOW INDEXES FROM users");
        let Statement::ShowIndexes(s) = stmt else {
            panic!("expected ShowIndexes, got {stmt}");
        };
        assert_eq!(s.0.as_deref(), Some("users"));
    }

    #[test]
    fn parse_dispatches_insert() {
        let stmt = parse("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')");
        let Statement::Insert(ins) = stmt else {
            panic!("expected Insert, got {stmt}");
        };
        assert_eq!(ins.table_name, "users");
        assert_eq!(
            ins.columns.as_deref(),
            Some(
                &[
                    NonEmptyString::new("id").unwrap(),
                    NonEmptyString::new("name").unwrap()
                ][..]
            )
        );
        let crate::parser::statements::InsertSource::Values(rows) = &ins.source else {
            panic!("expected Values source, got {:?}", ins.source);
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 2);
    }

    #[test]
    fn parse_dispatches_update() {
        let stmt = parse("UPDATE users SET name = 'x' WHERE id = 1");
        let Statement::Update(u) = stmt else {
            panic!("expected Update, got {stmt}");
        };
        assert_eq!(u.table_name, "users");
        assert_eq!(u.assignments.len(), 1);
        assert_eq!(u.assignments[0].column, "name");
        assert!(matches!(
            u.where_clause,
            Some(crate::parser::statements::Expr::BinaryOp { op: BinOp::Eq, .. })
        ));
    }

    #[test]
    fn parse_dispatches_delete() {
        let stmt = parse("DELETE FROM users WHERE id = 1");
        let Statement::Delete(d) = stmt else {
            panic!("expected Delete, got {stmt}");
        };
        assert_eq!(d.table_name, "users");
        assert!(matches!(
            d.where_clause,
            Some(crate::parser::statements::Expr::BinaryOp { op: BinOp::Eq, .. })
        ));
    }

    #[test]
    fn parse_dispatches_select_star() {
        let stmt = parse("SELECT * FROM users");
        let Statement::Select(s) = stmt else {
            panic!("expected Select, got {stmt}");
        };
        assert!(matches!(s.columns, SelectColumns::All));
        assert_eq!(s.from.len(), 1);
        assert_eq!(s.from[0].table.name, "users");
    }

    #[test]
    fn parse_dispatches_select_with_joins_and_clauses() {
        let sql = "SELECT u.id, COUNT(*) FROM users u \
                   LEFT JOIN orders o ON o.user_id = 1 \
                   WHERE u.active = true \
                   GROUP BY u.id \
                   ORDER BY u.id DESC \
                   LIMIT 10 OFFSET 5";
        let stmt = parse(sql);
        let Statement::Select(s) = stmt else {
            panic!("expected Select, got {stmt}");
        };

        let SelectColumns::Exprs(exprs) = &s.columns else {
            panic!("expected explicit select list");
        };
        assert_eq!(exprs.len(), 2);
        assert!(matches!(exprs[1].expr, Expr::CountStar));

        assert_eq!(s.from.len(), 1);
        assert_eq!(s.from[0].joins.len(), 1);
        assert_eq!(s.from[0].joins[0].kind, JoinKind::Left);
        assert_eq!(s.from[0].joins[0].table.name, "orders");

        assert!(s.where_clause.is_some());
        assert_eq!(s.group_by.len(), 1);

        assert_eq!(s.order_by.len(), 1);
        assert_eq!(s.order_by[0].1, OrderDirection::Desc);

        let limit = s.limit.expect("limit set");
        assert_eq!(limit.limit, Some(10));
        assert_eq!(limit.offset, 5);
    }

    #[test]
    fn parse_dispatches_select_with_having() {
        let stmt = parse("SELECT a FROM t GROUP BY a HAVING a = 1");
        let Statement::Select(s) = stmt else {
            panic!("expected Select");
        };
        let having = s.having.expect("having");
        assert!(matches!(
            having,
            crate::parser::statements::Expr::BinaryOp { op: BinOp::Eq, .. }
        ));
    }

    #[test]
    fn parse_dispatches_select_distinct_with_aggregates() {
        let stmt = parse("SELECT DISTINCT SUM(amount), AVG(amount) FROM orders");
        let Statement::Select(s) = stmt else {
            panic!("expected Select, got {stmt}");
        };
        assert!(s.distinct);
        let SelectColumns::Exprs(exprs) = &s.columns else {
            panic!("expected explicit select list");
        };
        assert_eq!(exprs.len(), 2);
        assert!(matches!(exprs[0].expr, Expr::Agg(AggFunc::Sum, _)));
        assert!(matches!(exprs[1].expr, Expr::Agg(AggFunc::Avg, _)));
    }

    #[test]
    fn parse_errors_on_unknown_leading_token() {
        match Parser::new("FOO BAR").parse() {
            Err(parsers::ParserError::ParsingError(_)) => {}
            Err(e) => panic!("expected ParsingError, got {e:?}"),
            Ok(_) => panic!("expected parse error, got Ok"),
        }
    }

    // --- end-of-input enforcement ---

    #[test]
    fn parse_accepts_trailing_semicolon() {
        assert!(Parser::new("SELECT * FROM users;").parse().is_ok());
        assert!(Parser::new("DROP TABLE users;").parse().is_ok());
        assert!(Parser::new("CREATE TABLE t (id INT);").parse().is_ok());
    }

    #[test]
    fn parse_accepts_no_trailing_semicolon() {
        // A trailing `;` is optional, not required.
        assert!(Parser::new("SELECT * FROM users").parse().is_ok());
    }

    #[test]
    fn parse_rejects_trailing_garbage_after_select() {
        // Use trailing tokens that can't be absorbed by any optional SELECT
        // clause (a bare identifier after `FROM users` would be parsed as a
        // table alias). A semicolon followed by another token is the cleanest
        // way to demonstrate "extra input after a complete statement."
        let Err(parsers::ParserError::ParsingError(msg)) =
            Parser::new("SELECT * FROM users; garbage").parse()
        else {
            panic!("expected ParsingError for trailing garbage");
        };
        assert!(
            msg.contains("after end of statement"),
            "error should mention end of statement: {msg}"
        );
    }

    #[test]
    fn parse_rejects_trailing_garbage_after_drop() {
        assert!(Parser::new("DROP TABLE users blah").parse().is_err());
    }

    #[test]
    fn parse_rejects_double_statement() {
        // Two statements in one input must fail. Multi-statement parsing would
        // be a separate API; `parse` is strictly single-statement.
        assert!(Parser::new("SELECT 1; SELECT 2").parse().is_err());
    }

    #[test]
    fn parse_rejects_alter_drop_column_if_exists_wrong_order() {
        // Standard SQL is `DROP COLUMN IF EXISTS bio` — IF EXISTS *before* the
        // name. The reverse order leaves trailing tokens and is now caught.
        assert!(
            Parser::new("ALTER TABLE users DROP COLUMN bio IF EXISTS")
                .parse()
                .is_err()
        );
    }

    fn parse_all(sql: &str) -> Vec<Statement> {
        Parser::new(sql)
            .parse_all()
            .unwrap_or_else(|e| panic!("parse_all failed for {sql:?}: {e:?}"))
    }

    #[test]
    fn parse_all_empty_input_returns_empty_vec() {
        assert!(parse_all("").is_empty());
    }

    #[test]
    fn parse_all_whitespace_only_returns_empty_vec() {
        assert!(parse_all("   \t\n  ").is_empty());
    }

    #[test]
    fn parse_all_semicolons_only_returns_empty_vec() {
        assert!(parse_all(";;;").is_empty());
    }

    #[test]
    fn parse_all_single_statement_no_semicolon() {
        let stmts = parse_all("SELECT * FROM users");
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], Statement::Select(_)));
    }

    #[test]
    fn parse_all_single_statement_with_trailing_semicolon() {
        let stmts = parse_all("SELECT * FROM users;");
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], Statement::Select(_)));
    }

    #[test]
    fn parse_all_two_statements() {
        let stmts = parse_all("CREATE TABLE t (id INT); INSERT INTO t VALUES (1)");
        assert_eq!(stmts.len(), 2);
        assert!(matches!(stmts[0], Statement::CreateTable(_)));
        assert!(matches!(stmts[1], Statement::Insert(_)));
    }

    #[test]
    fn parse_all_three_statements_frontend_use_case() {
        // This is the exact input the UI's sample SQL sends.
        let sql = "CREATE TABLE users (id INT, name VARCHAR);\nINSERT INTO users VALUES (1, 'alice'), (2, 'bob');\nSELECT * FROM users;";
        let stmts = parse_all(sql);
        assert_eq!(stmts.len(), 3);
        assert!(matches!(stmts[0], Statement::CreateTable(_)));
        assert!(matches!(stmts[1], Statement::Insert(_)));
        assert!(matches!(stmts[2], Statement::Select(_)));
    }

    #[test]
    fn parse_all_multiple_semicolons_between_statements() {
        let stmts = parse_all("SELECT * FROM a;;;SELECT * FROM b");
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn parse_all_leading_semicolons_ignored() {
        let stmts = parse_all(";;SELECT * FROM t");
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], Statement::Select(_)));
    }

    #[test]
    fn parse_all_stops_and_errors_on_bad_first_statement() {
        assert!(Parser::new("GARBAGE; SELECT * FROM t").parse_all().is_err());
    }

    #[test]
    fn parse_all_stops_and_errors_on_bad_second_statement() {
        assert!(Parser::new("SELECT * FROM t; GARBAGE").parse_all().is_err());
    }

    #[test]
    fn parse_all_statement_kinds_are_preserved() {
        let stmts = parse_all(
            "DROP TABLE t; CREATE INDEX i ON t (id) USING BTREE; DELETE FROM t WHERE id = 1",
        );
        assert_eq!(stmts.len(), 3);
        assert!(matches!(stmts[0], Statement::Drop(_)));
        assert!(matches!(stmts[1], Statement::CreateIndex(_)));
        assert!(matches!(stmts[2], Statement::Delete(_)));
    }

    #[test]
    fn parse_all_ui_sample_sql_with_leading_comments() {
        // Mirrors the SAMPLE_SQL constant shown in the browser editor.
        let sql = "\
-- Welcome to StoreMy.\n\
-- Cmd/Ctrl + Enter to run the highlighted block (or all of it).\n\
\n\
CREATE TABLE users (id INT, name VARCHAR);\n\
INSERT INTO users VALUES (1, 'alice'), (2, 'bob');\n\
SELECT * FROM users;";
        let stmts = parse_all(sql);
        assert_eq!(stmts.len(), 3);
        assert!(matches!(stmts[0], Statement::CreateTable(_)));
        assert!(matches!(stmts[1], Statement::Insert(_)));
        assert!(matches!(stmts[2], Statement::Select(_)));
    }

    #[test]
    fn parse_single_statement_with_inline_block_comment() {
        let stmt = parse("SELECT /* pick all */ * FROM users");
        assert!(matches!(stmt, Statement::Select(_)));
    }
}
