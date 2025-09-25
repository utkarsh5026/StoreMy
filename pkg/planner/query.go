package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/parser/statements"
)

type Plan interface {
	Execute() (any, error)
}

type QueryPlanner struct {
	tableManager *memory.TableManager
	pageStore    *memory.PageStore
}

func NewQueryPlanner(t *memory.TableManager, ps *memory.PageStore) *QueryPlanner {
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
