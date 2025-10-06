package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/registry"
)

type Plan interface {
	Execute() (any, error)
}

type QueryPlanner struct {
	ctx *registry.DatabaseContext
}

func NewQueryPlanner(ctx *registry.DatabaseContext) *QueryPlanner {
	return &QueryPlanner{
		ctx: ctx,
	}
}

func (qp *QueryPlanner) Plan(stmt statements.Statement, tid *transaction.TransactionID) (Plan, error) {
	store := qp.ctx.PageStore()
	tableMgr := qp.ctx.TableManager()

	switch s := stmt.(type) {
	case *statements.CreateStatement:
		return NewCreateTablePlan(s, qp.ctx, tid), nil
	case *statements.InsertStatement:
		return NewInsertPlan(s, store, tid, tableMgr), nil
	case *statements.DeleteStatement:
		return NewDeletePlan(s, store, tid, tableMgr), nil
	case *statements.SelectStatement:
		return NewSelectPlan(s, tableMgr, store, tid), nil
	case *statements.UpdateStatement:
		return NewUpdatePlan(s, tableMgr, store, tid), nil
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}
