package planner

import (
	"fmt"

	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tables"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type CreateTablePlan struct {
	Statement    *statements.CreateStatement
	tableManager *tables.TableManager
	tid          *transaction.TransactionID
}

type DDLResult struct {
	Success bool
	Message string
}

func (r *DDLResult) String() string {
	return fmt.Sprintf("DDL Result - Success: %t, Message: %s", r.Success, r.Message)
}

func NewCreateTablePlan(
	stmt *statements.CreateStatement,
	t *tables.TableManager,
	tid *transaction.TransactionID) *CreateTablePlan {
	return &CreateTablePlan{
		Statement:    stmt,
		tableManager: t,
		tid:          tid,
	}
}

func (p *CreateTablePlan) Execute() (*DDLResult, error) {
	if p.Statement.IfNotExists && p.tableManager.TableExists(p.Statement.TableName) {
		return &DDLResult{
			Success: true,
			Message: fmt.Sprintf("Table %s already exists (IF NOT EXISTS)", p.Statement.TableName),
		}, nil
	}

	if p.tableManager.TableExists(p.Statement.TableName) {
		return nil, fmt.Errorf("table %s already exists", p.Statement.TableName)
	}

	fieldNames := make([]string, 0, len(p.Statement.Fields))
	fieldTypes := make([]types.Type, 0, len(p.Statement.Fields))
	for _, field := range p.Statement.Fields {
		fieldNames = append(fieldNames, field.Name)
		fieldTypes = append(fieldTypes, field.Type)
	}

	tupleDesc, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		return nil, err
	}

	fileName := fmt.Sprintf("data/%s.dat", p.Statement.TableName)
	heapFile, err := heap.NewHeapFile(fileName, tupleDesc)
	if err != nil {
		return nil, fmt.Errorf("failed to create heap file: %v", err)
	}

	if err := p.tableManager.AddTable(heapFile, p.Statement.TableName, p.Statement.PrimaryKey); err != nil {
		return nil, err
	}

	return &DDLResult{
		Success: true,
		Message: fmt.Sprintf("Table %s created successfully", p.Statement.TableName),
	}, nil
}
