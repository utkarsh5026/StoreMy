use crate::parser::lexer::Lexer;

mod lexer;
mod parsers;
pub mod statements;
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
    use crate::parser::statements::{
        AggFunc, JoinKind, OrderDirection, SelectColumns, SelectExpr, Statement,
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
        let stmt = parse("CREATE INDEX idx_name (name) USING HASH");
        assert!(matches!(stmt, Statement::CreateIndex(_)));
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
            Some(&["id".to_string(), "name".to_string()][..])
        );
        assert_eq!(ins.values.len(), 2);
        assert_eq!(ins.values[0].len(), 2);
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
        assert!(u.where_clause.is_some());
    }

    #[test]
    fn parse_dispatches_delete() {
        let stmt = parse("DELETE FROM users WHERE id = 1");
        let Statement::Delete(d) = stmt else {
            panic!("expected Delete, got {stmt}");
        };
        assert_eq!(d.table_name, "users");
        assert!(d.where_clause.is_some());
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
        assert!(matches!(exprs[1].expr, SelectExpr::CountStar));

        assert_eq!(s.from.len(), 1);
        assert_eq!(s.from[0].joins.len(), 1);
        assert_eq!(s.from[0].joins[0].kind, JoinKind::Left);
        assert_eq!(s.from[0].joins[0].table, "orders");

        assert!(s.where_clause.is_some());
        assert_eq!(s.group_by.len(), 1);

        let order = s.order_by.expect("order_by set");
        assert_eq!(order.1, OrderDirection::Desc);

        let limit = s.limit.expect("limit set");
        assert_eq!(limit.limit, Some(10));
        assert_eq!(limit.offset, 5);
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
        assert!(matches!(exprs[0].expr, SelectExpr::Agg(AggFunc::Sum, _)));
        assert!(matches!(exprs[1].expr, SelectExpr::Agg(AggFunc::Avg, _)));
    }

    #[test]
    fn parse_errors_on_unknown_leading_token() {
        match Parser::new("FOO BAR").parse() {
            Err(parsers::ParserError::ParsingError(_)) => {}
            Err(e) => panic!("expected ParsingError, got {e:?}"),
            Ok(_) => panic!("expected parse error, got Ok"),
        }
    }
}
