package planner

import (
	"fmt"

	"storemy/pkg/catalog"
	"storemy/pkg/parser/statements"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type CreateTablePlan struct {
	Statement *statements.CreateStatement
	ctx       *registry.DatabaseContext
	tid       *primitives.TransactionID
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
	ctx *registry.DatabaseContext,
	tid *primitives.TransactionID,
) *CreateTablePlan {
	return &CreateTablePlan{
		Statement: stmt,
		ctx:       ctx,
		tid:       tid,
	}
}

func (p *CreateTablePlan) Execute() (any, error) {
	tableManager := p.ctx.TableManager()
	tableCatalog := p.ctx.Catalog()

	if p.Statement.IfNotExists && tableManager.TableExists(p.Statement.TableName) {
		return &DDLResult{
			Success: true,
			Message: fmt.Sprintf("Table %s already exists (IF NOT EXISTS)", p.Statement.TableName),
		}, nil
	}

	if tableManager.TableExists(p.Statement.TableName) {
		return nil, fmt.Errorf("table %s already exists", p.Statement.TableName)
	}

	tupleDesc, err := p.buildTupleDescriptor()
	if err != nil {
		return nil, err
	}

	heapFile, fileName, err := p.createHeapFile(tupleDesc)
	if err != nil {
		return nil, err
	}

	if err := tableManager.AddTable(heapFile, p.Statement.TableName, p.Statement.PrimaryKey); err != nil {
		return nil, err
	}

	if tableCatalog != nil {
		fieldMetadata := make([]catalog.FieldMetadata, len(p.Statement.Fields))
		for i, field := range p.Statement.Fields {
			fieldMetadata[i] = catalog.FieldMetadata{
				Name: field.Name,
				Type: field.Type,
			}
		}

		err = tableCatalog.RegisterTable(
			p.tid,
			heapFile.GetID(),
			p.Statement.TableName,
			fileName,
			p.Statement.PrimaryKey,
			fieldMetadata,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to register table in catalog: %v", err)
		}
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

func (p *CreateTablePlan) createHeapFile(tupleDesc *tuple.TupleDescription) (*heap.HeapFile, string, error) {
	var fileName string
	if p.ctx.DataDir() != "" {
		fileName = fmt.Sprintf("%s/%s.dat", p.ctx.DataDir(), p.Statement.TableName)
	} else {
		fileName = fmt.Sprintf("data/%s.dat", p.Statement.TableName)
	}
	heapFile, err := heap.NewHeapFile(fileName, tupleDesc)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create heap file: %v", err)
	}
	return heapFile, fileName, nil
}
