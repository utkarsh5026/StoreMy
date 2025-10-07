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

func (qp *QueryPlanner) Plan(stmt statements.Statement, tx *transaction.TransactionContext) (Plan, error) {
	switch s := stmt.(type) {
	case *statements.CreateStatement:
		return NewCreateTablePlan(s, qp.ctx, tx), nil
	case *statements.InsertStatement:
		return NewInsertPlan(s, tx, qp.ctx), nil
	case *statements.DeleteStatement:
		return NewDeletePlan(s, tx, qp.ctx), nil
	case *statements.SelectStatement:
		return NewSelectPlan(s, tx, qp.ctx), nil
	case *statements.UpdateStatement:
		return NewUpdatePlan(s, tx, qp.ctx), nil
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}
