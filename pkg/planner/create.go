package planner

import (
	"fmt"

	"storemy/pkg/catalog"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/registry"
)

type CreateTablePlan struct {
	Statement      *statements.CreateStatement
	ctx            *registry.DatabaseContext
	transactionCtx *transaction.TransactionContext
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
	transactionCtx *transaction.TransactionContext,
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
//  2. Transforms parsed field definitions into catalog metadata
//  3. Generates heap file name based on table name
//  4. Creates table entry in catalog with schema and primary key
//  5. Initializes backing heap file in the data directory
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

	fieldMetadata := make([]catalog.FieldMetadata, len(p.Statement.Fields))
	for i, field := range p.Statement.Fields {
		fieldMetadata[i] = catalog.FieldMetadata{
			Name: field.Name,
			Type: field.Type,
		}
	}

	filename := p.Statement.TableName + ".dat"

	_, err := catalogMgr.CreateTable(
		p.transactionCtx,
		p.Statement.TableName,
		filename,
		p.Statement.PrimaryKey,
		fieldMetadata,
		p.ctx.DataDir(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}

	return &DDLResult{
		Success: true,
		Message: fmt.Sprintf("Table %s created successfully", p.Statement.TableName),
	}, nil
}
