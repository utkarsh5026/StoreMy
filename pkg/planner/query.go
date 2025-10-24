package planner

import (
	"fmt"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner/internal/ddl"
	"storemy/pkg/planner/internal/dml"
	"storemy/pkg/planner/internal/indexops"
	"storemy/pkg/planner/internal/result"
)

type Plan interface {
	Execute() (result.Result, error)
}

type QueryPlanner struct {
	ctx DbContext
}

func NewQueryPlanner(ctx DbContext) *QueryPlanner {
	return &QueryPlanner{
		ctx: ctx,
	}
}

func (qp *QueryPlanner) Plan(stmt statements.Statement, tx TxContext) (Plan, error) {
	switch s := stmt.(type) {
	case *statements.CreateStatement:
		return ddl.NewCreateTablePlan(s, qp.ctx, tx), nil
	case *statements.DropStatement:
		return ddl.NewDropTablePlan(s, qp.ctx, tx), nil
	case *statements.CreateIndexStatement:
		return indexops.NewCreateIndexPlan(s, qp.ctx, tx), nil
	case *statements.DropIndexStatement:
		return indexops.NewDropIndexPlan(s, qp.ctx, tx), nil
	case *statements.InsertStatement:
		return dml.NewInsertPlan(s, tx, qp.ctx), nil
	case *statements.DeleteStatement:
		return dml.NewDeletePlan(s, tx, qp.ctx), nil
	case *statements.SelectStatement:
		return dml.NewSelectPlan(s, tx, qp.ctx), nil
	case *statements.UpdateStatement:
		return dml.NewUpdatePlan(s, tx, qp.ctx), nil
	case *statements.ShowIndexesStatement:
		return indexops.NewShowIndexesPlan(s, qp.ctx, tx), nil
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}
