package planner

import (
	"fmt"

	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/parser/statements"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type CreateTablePlan struct {
	Statement    *statements.CreateStatement
	tableManager *memory.TableManager
	tid          *transaction.TransactionID
	dataDir      string
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
	t *memory.TableManager,
	tid *transaction.TransactionID,
	dataDir string) *CreateTablePlan {
	return &CreateTablePlan{
		Statement:    stmt,
		tableManager: t,
		tid:          tid,
		dataDir:      dataDir,
	}
}

func (p *CreateTablePlan) Execute() (any, error) {
	if p.Statement.IfNotExists && p.tableManager.TableExists(p.Statement.TableName) {
		return &DDLResult{
			Success: true,
			Message: fmt.Sprintf("Table %s already exists (IF NOT EXISTS)", p.Statement.TableName),
		}, nil
	}

	if p.tableManager.TableExists(p.Statement.TableName) {
		return nil, fmt.Errorf("table %s already exists", p.Statement.TableName)
	}

	tupleDesc, err := p.buildTupleDescriptor()
	if err != nil {
		return nil, err
	}

	heapFile, err := p.createHeapFile(tupleDesc)
	if err != nil {
		return nil, err
	}

	if err := p.tableManager.AddTable(heapFile, p.Statement.TableName, p.Statement.PrimaryKey); err != nil {
		return nil, err
	}

	return &DDLResult{
		Success: true,
		Message: fmt.Sprintf("Table %s created successfully", p.Statement.TableName),
	}, nil
}

func (p *CreateTablePlan) buildTupleDescriptor() (*tuple.TupleDescription, error) {
	fieldCount := len(p.Statement.Fields)
	fieldNames := make([]string, 0, fieldCount)
	fieldTypes := make([]types.Type, 0, fieldCount)

	for _, field := range p.Statement.Fields {
		fieldNames = append(fieldNames, field.Name)
		fieldTypes = append(fieldTypes, field.Type)
	}

	tupleDesc, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		return nil, fmt.Errorf("failed to build tuple descriptor: %v", err)
	}

	return tupleDesc, nil
}

func (p *CreateTablePlan) createHeapFile(tupleDesc *tuple.TupleDescription) (*heap.HeapFile, error) {
	var fileName string
	if p.dataDir != "" {
		fileName = fmt.Sprintf("%s/%s.dat", p.dataDir, p.Statement.TableName)
	} else {
		fileName = fmt.Sprintf("data/%s.dat", p.Statement.TableName)
	}
	heapFile, err := heap.NewHeapFile(fileName, tupleDesc)
	if err != nil {
		return nil, fmt.Errorf("failed to create heap file: %v", err)
	}
	return heapFile, nil
}
