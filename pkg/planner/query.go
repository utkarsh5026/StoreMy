package planner

import (
	"fmt"
	"storemy/pkg/parser/statements"
	"storemy/pkg/primitives"
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

func (qp *QueryPlanner) Plan(stmt statements.Statement, tid *primitives.TransactionID) (Plan, error) {
	switch s := stmt.(type) {
	case *statements.CreateStatement:
		return NewCreateTablePlan(s, qp.ctx, tid), nil
	case *statements.InsertStatement:
		return NewInsertPlan(s, tid, qp.ctx), nil
	case *statements.DeleteStatement:
		return NewDeletePlan(s, tid, qp.ctx), nil
	case *statements.SelectStatement:
		return NewSelectPlan(s, tid, qp.ctx), nil
	case *statements.UpdateStatement:
		return NewUpdatePlan(s, tid, qp.ctx), nil
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}
