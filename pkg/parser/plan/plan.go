package plan

import (
	"fmt"
	"storemy/pkg/types"
)

type ScanPlan struct {
	TableID int
	Alias   string
}

func NewScanPlan(tableID int, alias string) *ScanPlan {
	return &ScanPlan{
		TableID: tableID,
		Alias:   alias,
	}
}

func (sp *ScanPlan) String() string {
	return fmt.Sprintf("Scan[table=%d, alias=%s]", sp.TableID, sp.Alias)
}

type JoinPlan struct {
	Table1    string          // Left table alias
	Table2    string          // Right table alias
	Field1    string          // Field from left table
	Field2    string          // Field from right table
	Predicate types.Predicate // Join predicate (=, <, >, etc.)
}

func NewJoinPlan(table1, table2, field1, field2 string, predicate types.Predicate) *JoinPlan {
	return &JoinPlan{
		Table1:    table1,
		Table2:    table2,
		Field1:    field1,
		Field2:    field2,
		Predicate: predicate,
	}
}

func (jp *JoinPlan) String() string {
	return fmt.Sprintf("Join[%s.%s %s %s.%s]",
		jp.Table1, jp.Field1, jp.Predicate, jp.Table2, jp.Field2)
}
