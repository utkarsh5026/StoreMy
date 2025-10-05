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
	dataDir      string
}

func NewQueryPlanner(t *memory.TableManager, ps *memory.PageStore) *QueryPlanner {
	return &QueryPlanner{
		tableManager: t,
		pageStore:    ps,
		dataDir:      "",
	}
}

func (qp *QueryPlanner) SetDataDir(dataDir string) {
	qp.dataDir = dataDir
}

func (qp *QueryPlanner) Plan(stmt statements.Statement, tid *transaction.TransactionID) (Plan, error) {
	switch s := stmt.(type) {
	case *statements.CreateStatement:
		return NewCreateTablePlan(s, qp.tableManager, tid, qp.dataDir), nil
	case *statements.InsertStatement:
		return NewInsertPlan(s, qp.pageStore, tid, qp.tableManager), nil
	case *statements.DeleteStatement:
		return NewDeletePlan(s, qp.pageStore, tid, qp.tableManager), nil
	case *statements.SelectStatement:
		return NewSelectPlan(s, qp.tableManager, qp.pageStore, tid), nil
	case *statements.UpdateStatement:
		return NewUpdatePlan(s, qp.tableManager, qp.pageStore, tid), nil
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}
