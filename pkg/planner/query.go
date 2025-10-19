package planner

import (
	"fmt"
	"storemy/pkg/parser/statements"
)

type Plan interface {
	Execute() (Result, error)
}

type QueryPlanner struct {
	ctx DbContext
}

func NewQueryPlanner(ctx DbContext) *QueryPlanner {
	return &QueryPlanner{
		ctx: ctx,
	}
}

func (qp *QueryPlanner) Plan(stmt statements.Statement, tx TransactionCtx) (Plan, error) {
	switch s := stmt.(type) {
	case *statements.CreateStatement:
		return NewCreateTablePlan(s, qp.ctx, tx), nil
	case *statements.DropStatement:
		return NewDropTablePlan(s, qp.ctx, tx), nil
	case *statements.CreateIndexStatement:
		return NewCreateIndexPlan(s, qp.ctx, tx), nil
	case *statements.DropIndexStatement:
		return NewDropIndexPlan(s, qp.ctx, tx), nil
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
