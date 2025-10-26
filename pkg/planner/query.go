package planner

import (
	"fmt"
	"storemy/pkg/logging"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner/internal/ddl"
	"storemy/pkg/planner/internal/dml"
	"storemy/pkg/planner/internal/indexops"
	"storemy/pkg/planner/internal/result"
)

// Plan represents an executable query plan that can be executed to produce a result.
type Plan interface {
	Execute() (result.Result, error)
}

// QueryPlanner is responsible for converting parsed SQL statements into executable plans.
// It uses the database context to access schema information and transaction context for execution.
type QueryPlanner struct {
	ctx DbContext
}

// NewQueryPlanner creates a new QueryPlanner instance with the provided database context.
func NewQueryPlanner(ctx DbContext) *QueryPlanner {
	return &QueryPlanner{
		ctx: ctx,
	}
}

// Plan converts a parsed SQL statement into an executable plan.
// It supports DDL operations (CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX),
// DML operations (INSERT, DELETE, SELECT, UPDATE), and utility operations (SHOW INDEXES).
//
// Parameters:
//   - stmt: The parsed SQL statement to plan
//   - tx: The transaction context for executing the plan
//
// Returns:
//   - Plan: An executable plan for the statement
//   - error: An error if the statement type is unsupported
func (qp *QueryPlanner) Plan(stmt statements.Statement, tx TxContext) (Plan, error) {
	log := logging.WithTx(int(tx.ID.ID())).With("component", "query_planner")

	var stmtType string
	switch s := stmt.(type) {
	case *statements.CreateStatement:
		stmtType = "CREATE_TABLE"
		log.Info("planning query", "statement_type", stmtType, "table", s.TableName)
		return ddl.NewCreateTablePlan(s, qp.ctx, tx), nil
	case *statements.DropStatement:
		stmtType = "DROP_TABLE"
		log.Info("planning query", "statement_type", stmtType, "table", s.TableName)
		return ddl.NewDropTablePlan(s, qp.ctx, tx), nil
	case *statements.CreateIndexStatement:
		stmtType = "CREATE_INDEX"
		log.Info("planning query", "statement_type", stmtType, "index", s.IndexName, "table", s.TableName)
		return indexops.NewCreateIndexPlan(s, qp.ctx, tx), nil
	case *statements.DropIndexStatement:
		stmtType = "DROP_INDEX"
		log.Info("planning query", "statement_type", stmtType, "index", s.IndexName)
		return indexops.NewDropIndexPlan(s, qp.ctx, tx), nil
	case *statements.InsertStatement:
		stmtType = "INSERT"
		log.Info("planning query", "statement_type", stmtType, "table", s.TableName, "num_rows", len(s.Values))
		return dml.NewInsertPlan(s, tx, qp.ctx), nil
	case *statements.DeleteStatement:
		stmtType = "DELETE"
		log.Info("planning query", "statement_type", stmtType, "table", s.TableName)
		return dml.NewDeletePlan(s, tx, qp.ctx), nil
	case *statements.SelectStatement:
		stmtType = "SELECT"
		log.Info("planning query", "statement_type", stmtType)
		return dml.NewSelectPlan(s, tx, qp.ctx), nil
	case *statements.UpdateStatement:
		stmtType = "UPDATE"
		log.Info("planning query", "statement_type", stmtType, "table", s.TableName)
		return dml.NewUpdatePlan(s, tx, qp.ctx), nil
	case *statements.ShowIndexesStatement:
		stmtType = "SHOW_INDEXES"
		log.Info("planning query", "statement_type", stmtType, "table", s.TableName)
		return indexops.NewShowIndexesPlan(s, qp.ctx, tx), nil
	default:
		log.Error("unsupported statement type", "type", fmt.Sprintf("%T", stmt))
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}
