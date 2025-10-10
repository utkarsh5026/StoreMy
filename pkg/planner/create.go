package planner

import (
	"fmt"

	"storemy/pkg/catalog/schema"
	"storemy/pkg/parser/statements"
)

type CreateTablePlan struct {
	Statement      *statements.CreateStatement
	ctx            DbContext
	transactionCtx TransactionCtx
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
	ctx DbContext,
	transactionCtx TransactionCtx,
) *CreateTablePlan {
	return &CreateTablePlan{
		Statement:      stmt,
		ctx:            ctx,
		transactionCtx: transactionCtx,
	}
}

// Execute performs the CREATE TABLE operation within the current transaction.
//
// Execution steps:
//  1. Validates table doesn't already exist (respects IF NOT EXISTS clause)
//  2. Transforms parsed field definitions into schema metadata
//  3. Creates table entry in catalog with schema
//  4. CatalogManager handles file path generation and persistence
func (p *CreateTablePlan) Execute() (any, error) {
	catalogMgr := p.ctx.CatalogManager()
	if catalogMgr.TableExists(p.transactionCtx.ID, p.Statement.TableName) {
		if p.Statement.IfNotExists {
			return &DDLResult{
				Success: true,
				Message: fmt.Sprintf("Table %s already exists (IF NOT EXISTS)", p.Statement.TableName),
			}, nil
		}
		return nil, fmt.Errorf("table %s already exists", p.Statement.TableName)
	}

	tableSchema, err := p.makeTableSchema()
	if err != nil {
		return nil, err
	}

	_, err = catalogMgr.CreateTable(p.transactionCtx, tableSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}

	return &DDLResult{
		Success: true,
		Message: fmt.Sprintf("Table %s created successfully", p.Statement.TableName),
	}, nil
}

func (p *CreateTablePlan) makeTableSchema() (*schema.Schema, error) {
	columns := make([]schema.ColumnMetadata, len(p.Statement.Fields))
	for i, field := range p.Statement.Fields {
		isPrimary := field.Name == p.Statement.PrimaryKey
		columns[i] = schema.ColumnMetadata{
			Name:      field.Name,
			FieldType: field.Type,
			Position:  i,
			IsPrimary: isPrimary,
			IsAutoInc: field.AutoIncrement,
			TableID:   0, // Will be set by CatalogManager
		}
	}

	tableSchema, err := schema.NewSchema(0, p.Statement.TableName, columns)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema: %v", err)
	}
	tableSchema.PrimaryKey = p.Statement.PrimaryKey
	return tableSchema, nil
}
