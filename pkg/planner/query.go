package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/storage/page"
	"storemy/pkg/tables"
)

type Plan interface {
	Execute() (any, error)
}

type QueryPlanner struct {
	tableManager *tables.TableManager
	pageStore    *page.PageStore
}

func NewQueryPlanner(t *tables.TableManager, ps *page.PageStore) *QueryPlanner {
	return &QueryPlanner{
		tableManager: t,
		pageStore:    ps,
	}
}

func (qp *QueryPlanner) Plan(stmt statements.Statement, tid *transaction.TransactionID) (Plan, error) {
	switch s := stmt.(type) {
	case *statements.CreateStatement:
		return NewCreateTablePlan(s, qp.tableManager, tid), nil
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}
