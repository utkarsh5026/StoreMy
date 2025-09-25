package statements

import "storemy/pkg/parser/plan"

type SelectStatement struct {
	Plan *plan.SelectPlan
}

func NewSelectStatement(plan *plan.SelectPlan) *SelectStatement {
	return &SelectStatement{
		Plan: plan,
	}
}
func (ss *SelectStatement) GetType() StatementType {
	return Select
}

func (ss *SelectStatement) String() string {
	return ss.Plan.String()
}
