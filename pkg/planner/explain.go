package planner

import (
	"fmt"
	"storemy/pkg/optimizer"
	"storemy/pkg/parser/statements"
	"storemy/pkg/plan"
	"storemy/pkg/planner/internal/shared"
	"strings"
)

// ExplainPlan generates an execution plan for a query without executing it.
// It builds the logical plan, applies optimizer transformations, and returns
// a formatted representation of the plan tree with cost estimates.
type ExplainPlan struct {
	ctx       DbContext
	tx        TxContext
	statement *statements.ExplainStatement
}

// NewExplainPlan creates a new EXPLAIN plan.
func NewExplainPlan(stmt *statements.ExplainStatement, ctx DbContext, tx TxContext) *ExplainPlan {
	return &ExplainPlan{
		ctx:       ctx,
		tx:        tx,
		statement: stmt,
	}
}

// Execute builds the query plan and returns an ExplainResult with the plan representation.
func (p *ExplainPlan) Execute() (shared.Result, error) {
	// Build the logical plan for the underlying statement
	planNode, err := p.buildLogicalPlan()
	if err != nil {
		return nil, fmt.Errorf("failed to build logical plan: %w", err)
	}

	// Apply optimizer if we have a plan node
	var optimizedPlan plan.PlanNode
	if planNode != nil {
		optimizedPlan, err = p.optimizePlan(planNode)
		if err != nil {
			return nil, fmt.Errorf("failed to optimize plan: %w", err)
		}
	}

	// Format the plan for output
	planText := p.formatPlan(optimizedPlan)

	// Return the explain result
	return shared.NewExplainResult(
		planText,
		p.statement.Options.Format,
		p.statement.Options.Analyze,
	), nil
}

// buildLogicalPlan constructs the logical plan tree for the underlying statement.
// It converts the parsed statement into a PlanNode tree that can be optimized.
func (p *ExplainPlan) buildLogicalPlan() (plan.PlanNode, error) {
	stmt := p.statement.Statement

	switch s := stmt.(type) {
	case *statements.SelectStatement:
		return p.buildSelectPlan(s)
	case *statements.InsertStatement:
		return p.buildInsertPlan(s)
	case *statements.UpdateStatement:
		return p.buildUpdatePlan(s)
	case *statements.DeleteStatement:
		return p.buildDeletePlan(s)
	case *statements.CreateStatement:
		return p.buildDDLPlan("CREATE TABLE", s.TableName), nil
	case *statements.CreateIndexStatement:
		return p.buildDDLPlan("CREATE INDEX", s.IndexName), nil
	case *statements.DropStatement:
		return p.buildDDLPlan("DROP TABLE", s.TableName), nil
	case *statements.DropIndexStatement:
		return p.buildDDLPlan("DROP INDEX", s.IndexName), nil
	default:
		return nil, fmt.Errorf("EXPLAIN not supported for statement type: %T", stmt)
	}
}

// buildSelectPlan builds a plan tree for SELECT statements.
// This is the most complex case as it involves scans, filters, joins, aggregation, etc.
func (p *ExplainPlan) buildSelectPlan(stmt *statements.SelectStatement) (plan.PlanNode, error) {
	selectPlan := stmt.Plan

	// Handle set operations (UNION, INTERSECT, EXCEPT)
	if selectPlan.IsSetOperation() {
		return p.buildSetOperationPlan(selectPlan)
	}

	// Start with scan nodes for all tables
	tables := selectPlan.Tables()
	if len(tables) == 0 {
		return nil, fmt.Errorf("SELECT requires at least one table")
	}

	// Build scan for first table
	currentNode, err := p.buildScanNode(tables[0], selectPlan.Filters())
	if err != nil {
		return nil, err
	}

	// Apply joins if present
	joins := selectPlan.Joins()
	if len(joins) > 0 {
		currentNode, err = p.buildJoinNodes(currentNode, joins)
		if err != nil {
			return nil, err
		}
	}

	// Apply aggregation if present
	if selectPlan.HasAgg() {
		currentNode = p.buildAggregateNode(currentNode, selectPlan)
	}

	// Apply projection if not SELECT *
	if !selectPlan.SelectAll() {
		currentNode = p.buildProjectNode(currentNode, selectPlan.SelectList())
	}

	// Apply DISTINCT if specified
	if selectPlan.IsDistinct() && !selectPlan.HasAgg() {
		currentNode = p.buildDistinctNode(currentNode)
	}

	// Apply ORDER BY if present
	if selectPlan.HasOrderBy() {
		currentNode = p.buildSortNode(currentNode, selectPlan.OrderByField(), selectPlan.OrderByAsc())
	}

	// Apply LIMIT if present
	if selectPlan.HasLimit() {
		currentNode = p.buildLimitNode(currentNode, int(selectPlan.Limit()), int(selectPlan.Offset())) // #nosec G115
	}

	return currentNode, nil
}

// buildScanNode creates a scan node for a table with optional filters.
func (p *ExplainPlan) buildScanNode(table *plan.ScanNode, filters []*plan.FilterNode) (plan.PlanNode, error) {
	scanNode := &plan.ScanNode{
		BasePlanNode: plan.BasePlanNode{},
		TableName:    table.TableName,
		Alias:        table.Alias,
		AccessMethod: "seqscan", // Default to sequential scan
		Predicates:   make([]plan.PredicateInfo, 0),
	}

	// Add filter predicates from the first filter (if any)
	if len(filters) > 0 && filters[0] != nil {
		filter := filters[0]
		scanNode.Predicates = append(scanNode.Predicates, plan.PredicateInfo{
			Column:    filter.Field,
			Predicate: filter.Predicate,
			Value:     filter.Constant,
			Type:      plan.StandardPredicate,
		})
	}

	return scanNode, nil
}

// buildJoinNodes creates a left-deep join tree from the join list.
func (p *ExplainPlan) buildJoinNodes(leftNode plan.PlanNode, joins []*plan.JoinNode) (plan.PlanNode, error) {
	currentNode := leftNode

	for _, joinSpec := range joins {
		// Build scan for right table
		rightNode := &plan.ScanNode{
			BasePlanNode: plan.BasePlanNode{},
			TableName:    joinSpec.RightTable.TableName,
			Alias:        joinSpec.RightTable.Alias,
			AccessMethod: "seqscan",
			Predicates:   make([]plan.PredicateInfo, 0),
		}

		// Create join node
		joinNode := &plan.JoinNode{
			BasePlanNode: plan.BasePlanNode{
				Children: []plan.PlanNode{currentNode, rightNode},
			},
			LeftChild:   currentNode,
			RightChild:  rightNode,
			JoinType:    joinSpec.JoinType,
			LeftColumn:  joinSpec.LeftField,
			RightColumn: joinSpec.RightField,
		}

		currentNode = joinNode
	}

	return currentNode, nil
}

// buildAggregateNode creates an aggregation node.
func (p *ExplainPlan) buildAggregateNode(child plan.PlanNode, selectPlan *plan.SelectPlan) plan.PlanNode {
	groupBy := make([]string, 0)
	if selectPlan.GroupByField() != "" {
		groupBy = append(groupBy, selectPlan.GroupByField())
	}

	aggFunctions := []string{
		fmt.Sprintf("%s(%s)", selectPlan.AggOp(), selectPlan.AggField()),
	}

	return &plan.AggregateNode{
		BasePlanNode: plan.BasePlanNode{
			Children: []plan.PlanNode{child},
		},
		Child:        child,
		GroupByExprs: groupBy,
		AggFunctions: aggFunctions,
	}
}

// buildProjectNode creates a projection node.
func (p *ExplainPlan) buildProjectNode(child plan.PlanNode, selectList []*plan.SelectListNode) plan.PlanNode {
	columns := make([]string, 0, len(selectList))
	for _, field := range selectList {
		columns = append(columns, field.FieldName)
	}

	return &plan.ProjectNode{
		BasePlanNode: plan.BasePlanNode{
			Children: []plan.PlanNode{child},
		},
		Child:   child,
		Columns: columns,
	}
}

// buildDistinctNode creates a distinct node.
func (p *ExplainPlan) buildDistinctNode(child plan.PlanNode) plan.PlanNode {
	return &plan.DistinctNode{
		BasePlanNode: plan.BasePlanNode{
			Children: []plan.PlanNode{child},
		},
		Child: child,
	}
}

// buildSortNode creates a sort node.
func (p *ExplainPlan) buildSortNode(child plan.PlanNode, orderByField string, ascending bool) plan.PlanNode {
	order := "ASC"
	if !ascending {
		order = "DESC"
	}

	return &plan.SortNode{
		BasePlanNode: plan.BasePlanNode{
			Children: []plan.PlanNode{child},
		},
		Child:     child,
		SortKey:   orderByField,
		Ascending: ascending,
		Order:     order,
	}
}

// buildLimitNode creates a limit node.
func (p *ExplainPlan) buildLimitNode(child plan.PlanNode, limit, offset int) plan.PlanNode {
	return &plan.LimitNode{
		BasePlanNode: plan.BasePlanNode{
			Children: []plan.PlanNode{child},
		},
		Child:  child,
		Limit:  limit,
		Offset: offset,
	}
}

// buildSetOperationPlan builds a plan for set operations (UNION, INTERSECT, EXCEPT).
func (p *ExplainPlan) buildSetOperationPlan(selectPlan *plan.SelectPlan) (plan.PlanNode, error) {
	leftStmt := statements.NewSelectStatement(selectPlan.LeftPlan())
	leftPlan, err := p.buildSelectPlan(leftStmt)
	if err != nil {
		return nil, fmt.Errorf("failed to build left side of set operation: %w", err)
	}

	rightStmt := statements.NewSelectStatement(selectPlan.RightPlan())
	rightPlan, err := p.buildSelectPlan(rightStmt)
	if err != nil {
		return nil, fmt.Errorf("failed to build right side of set operation: %w", err)
	}

	setOpType := "UNION"
	switch selectPlan.SetOpType() {
	case plan.UnionOp:
		setOpType = "UNION"
	case plan.IntersectOp:
		setOpType = "INTERSECT"
	case plan.ExceptOp:
		setOpType = "EXCEPT"
	}

	if selectPlan.SetOpAll() {
		setOpType += " ALL"
	}

	return &plan.SetOpNode{
		BasePlanNode: plan.BasePlanNode{
			Children: []plan.PlanNode{leftPlan, rightPlan},
		},
		LeftChild:  leftPlan,
		RightChild: rightPlan,
		OpType:     setOpType,
	}, nil
}

// buildInsertPlan builds a plan for INSERT statements.
func (p *ExplainPlan) buildInsertPlan(stmt *statements.InsertStatement) (plan.PlanNode, error) {
	return &plan.InsertNode{
		BasePlanNode: plan.BasePlanNode{},
		TableName:    stmt.TableName,
		NumRows:      len(stmt.Values),
	}, nil
}

// buildUpdatePlan builds a plan for UPDATE statements.
func (p *ExplainPlan) buildUpdatePlan(stmt *statements.UpdateStatement) (plan.PlanNode, error) {
	// Build scan with filter
	scanNode := &plan.ScanNode{
		BasePlanNode: plan.BasePlanNode{},
		TableName:    stmt.TableName,
		AccessMethod: "seqscan",
		Predicates:   make([]plan.PredicateInfo, 0),
	}

	// Add WHERE filter if present
	if stmt.WhereClause != nil {
		scanNode.Predicates = append(scanNode.Predicates, plan.PredicateInfo{
			Column:    stmt.WhereClause.Field,
			Predicate: stmt.WhereClause.Predicate,
			Value:     stmt.WhereClause.Constant,
			Type:      plan.StandardPredicate,
		})
	}

	// Wrap in update node
	return &plan.UpdateNode{
		BasePlanNode: plan.BasePlanNode{
			Children: []plan.PlanNode{scanNode},
		},
		Child:     scanNode,
		TableName: stmt.TableName,
		SetFields: len(stmt.SetClauses),
	}, nil
}

// buildDeletePlan builds a plan for DELETE statements.
func (p *ExplainPlan) buildDeletePlan(stmt *statements.DeleteStatement) (plan.PlanNode, error) {
	// Build scan with filter
	scanNode := &plan.ScanNode{
		BasePlanNode: plan.BasePlanNode{},
		TableName:    stmt.TableName,
		AccessMethod: "seqscan",
		Predicates:   make([]plan.PredicateInfo, 0),
	}

	// Add WHERE filter if present
	if stmt.WhereClause != nil {
		scanNode.Predicates = append(scanNode.Predicates, plan.PredicateInfo{
			Column:    stmt.WhereClause.Field,
			Predicate: stmt.WhereClause.Predicate,
			Value:     stmt.WhereClause.Constant,
			Type:      plan.StandardPredicate,
		})
	}

	// Wrap in delete node
	return &plan.DeleteNode{
		BasePlanNode: plan.BasePlanNode{
			Children: []plan.PlanNode{scanNode},
		},
		Child:     scanNode,
		TableName: stmt.TableName,
	}, nil
}

// buildDDLPlan builds a simple plan for DDL statements.
func (p *ExplainPlan) buildDDLPlan(operation, objectName string) plan.PlanNode {
	return &plan.DDLNode{
		BasePlanNode: plan.BasePlanNode{},
		Operation:    operation,
		ObjectName:   objectName,
	}
}

// optimizePlan applies the query optimizer to the logical plan.
func (p *ExplainPlan) optimizePlan(planNode plan.PlanNode) (plan.PlanNode, error) {
	// Get optimizer from context
	optimizerInstance, err := optimizer.NewQueryOptimizer(
		p.ctx.CatalogManager(),
		optimizer.DefaultOptimizerConfig(),
	)
	if err != nil {
		// If optimizer creation fails, return unoptimized plan
		return planNode, nil
	}

	// Apply optimization
	optimizedPlan, err := optimizerInstance.Optimize(p.tx, planNode)
	if err != nil {
		// If optimization fails, return original plan
		return planNode, nil
	}

	return optimizedPlan, nil
}

// formatPlan formats the plan tree for output.
func (p *ExplainPlan) formatPlan(planNode plan.PlanNode) string {
	if planNode == nil {
		return "No execution plan available"
	}

	format := p.statement.Options.Format
	switch format {
	case "JSON":
		return p.formatPlanJSON(planNode)
	case "EDUCATIONAL", "EDU":
		return p.formatPlanEducational(planNode)
	case "TEXT", "":
		return p.formatPlanText(planNode)
	default:
		return p.formatPlanText(planNode)
	}
}

// formatPlanText formats the plan as human-readable text.
func (p *ExplainPlan) formatPlanText(planNode plan.PlanNode) string {
	var sb strings.Builder
	sb.WriteString("Query Execution Plan:\n")
	sb.WriteString(strings.Repeat("=", 60))
	sb.WriteString("\n\n")
	sb.WriteString(p.formatPlanTextRecursive(planNode, 0))
	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("=", 60))
	sb.WriteString("\n")

	// Add optimization info
	sb.WriteString(fmt.Sprintf("\nTotal Cost: %.2f\n", planNode.GetCost()))
	sb.WriteString(fmt.Sprintf("Estimated Rows: %d\n", planNode.GetCardinality()))

	return sb.String()
}

// formatPlanTextRecursive recursively formats the plan tree with indentation.
func (p *ExplainPlan) formatPlanTextRecursive(node plan.PlanNode, depth int) string {
	if node == nil {
		return ""
	}

	indent := strings.Repeat("  ", depth)
	var sb strings.Builder

	// Format this node
	sb.WriteString(indent)
	sb.WriteString("-> ")
	sb.WriteString(p.formatNodeDetails(node))
	sb.WriteString("\n")

	// Recursively format children
	for _, child := range node.GetChildren() {
		if child != nil {
			sb.WriteString(p.formatPlanTextRecursive(child, depth+1))
		}
	}

	return sb.String()
}

// formatNodeDetails formats the details of a single node.
func (p *ExplainPlan) formatNodeDetails(node plan.PlanNode) string {
	cost := node.GetCost()
	rows := node.GetCardinality()

	baseInfo := fmt.Sprintf("%s (cost=%.2f, rows=%d)",
		node.GetNodeType(), cost, rows)

	// Add node-specific details
	switch n := node.(type) {
	case *plan.ScanNode:
		method := n.AccessMethod
		if method == "" {
			method = "seqscan"
		}
		details := fmt.Sprintf("Scan on %s [%s]", n.TableName, method)
		if n.IndexName != "" {
			details += fmt.Sprintf(" using index %s", n.IndexName)
		}
		if len(n.Predicates) > 0 {
			details += fmt.Sprintf(" with %d filter(s)", len(n.Predicates))
		}
		return fmt.Sprintf("%s %s", details, baseInfo)

	case *plan.JoinNode:
		joinTypeStr := n.JoinType
		if joinTypeStr == "" {
			joinTypeStr = "inner"
		}
		return fmt.Sprintf("%s JOIN on %s = %s %s",
			joinTypeStr, n.LeftColumn, n.RightColumn, baseInfo)

	case *plan.FilterNode:
		return fmt.Sprintf("Filter (%d predicates) %s",
			len(n.Predicates), baseInfo)

	case *plan.ProjectNode:
		return fmt.Sprintf("Project (%d columns) %s",
			len(n.Columns), baseInfo)

	case *plan.AggregateNode:
		aggInfo := fmt.Sprintf("Aggregate [%s]", strings.Join(n.AggFunctions, ", "))
		if len(n.GroupByExprs) > 0 {
			aggInfo += fmt.Sprintf(" GROUP BY %s", strings.Join(n.GroupByExprs, ", "))
		}
		return fmt.Sprintf("%s %s", aggInfo, baseInfo)

	case *plan.SortNode:
		return fmt.Sprintf("Sort on %s %s %s",
			n.SortKey, n.Order, baseInfo)

	case *plan.LimitNode:
		return fmt.Sprintf("Limit (limit=%d, offset=%d) %s",
			n.Limit, n.Offset, baseInfo)

	case *plan.DistinctNode:
		return fmt.Sprintf("Distinct %s", baseInfo)

	case *plan.SetOpNode:
		return fmt.Sprintf("%s %s", n.OpType, baseInfo)

	case *plan.InsertNode:
		return fmt.Sprintf("Insert into %s (%d rows) %s",
			n.TableName, n.NumRows, baseInfo)

	case *plan.UpdateNode:
		return fmt.Sprintf("Update %s (%d fields) %s",
			n.TableName, n.SetFields, baseInfo)

	case *plan.DeleteNode:
		return fmt.Sprintf("Delete from %s %s",
			n.TableName, baseInfo)

	case *plan.DDLNode:
		return fmt.Sprintf("DDL: %s %s %s",
			n.Operation, n.ObjectName, baseInfo)

	default:
		return baseInfo
	}
}

// formatPlanEducational formats the plan with explanations of what each node does,
// intended to help users understand query execution.
func (p *ExplainPlan) formatPlanEducational(planNode plan.PlanNode) string {
	var sb strings.Builder
	sb.WriteString("Educational Query Execution Plan:\n")
	sb.WriteString(strings.Repeat("=", 60))
	sb.WriteString("\n\n")
	sb.WriteString("This output explains how the database will execute your query.\n")
	sb.WriteString("Each step is performed in bottom-up order (innermost first).\n\n")
	sb.WriteString(p.formatPlanEducationalRecursive(planNode, 0, 1))
	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("=", 60))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("\nTotal Estimated Cost: %.2f\n", planNode.GetCost()))
	sb.WriteString(fmt.Sprintf("Estimated Output Rows: %d\n", planNode.GetCardinality()))
	return sb.String()
}

// formatPlanEducationalRecursive recursively formats nodes with plain-language explanations.
func (p *ExplainPlan) formatPlanEducationalRecursive(node plan.PlanNode, depth, step int) string {
	if node == nil {
		return ""
	}

	indent := strings.Repeat("  ", depth)
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("%sStep %d: %s\n", indent, step, p.educationalNodeExplanation(node)))
	sb.WriteString(fmt.Sprintf("%s  Cost: %.2f | Estimated rows: %d\n\n", indent, node.GetCost(), node.GetCardinality()))

	children := node.GetChildren()
	for i, child := range children {
		if child != nil {
			sb.WriteString(p.formatPlanEducationalRecursive(child, depth+1, step+i+1))
		}
	}

	return sb.String()
}

// educationalNodeExplanation returns a plain-language description of what a node does.
func (p *ExplainPlan) educationalNodeExplanation(node plan.PlanNode) string {
	switch n := node.(type) {
	case *plan.ScanNode:
		method := n.AccessMethod
		if method == "" {
			method = "seqscan"
		}
		if method == "seqscan" || method == "" {
			desc := fmt.Sprintf("Sequential Scan on %q — reads every row in the table from disk", n.TableName)
			if len(n.Predicates) > 0 {
				desc += fmt.Sprintf(", then filters rows matching %d condition(s)", len(n.Predicates))
			}
			return desc
		}
		desc := fmt.Sprintf("Index Scan on %q using index %q — uses an index to find matching rows efficiently", n.TableName, n.IndexName)
		return desc

	case *plan.JoinNode:
		joinType := n.JoinType
		if joinType == "" {
			joinType = "INNER"
		}
		return fmt.Sprintf("%s JOIN — combines rows from two inputs where %s = %s", strings.ToUpper(joinType), n.LeftColumn, n.RightColumn)

	case *plan.FilterNode:
		return fmt.Sprintf("Filter — evaluates %d predicate(s) and discards rows that do not match", len(n.Predicates))

	case *plan.ProjectNode:
		return fmt.Sprintf("Projection — selects only %d column(s) from the input, discarding the rest", len(n.Columns))

	case *plan.AggregateNode:
		desc := fmt.Sprintf("Aggregation — computes %s", strings.Join(n.AggFunctions, ", "))
		if len(n.GroupByExprs) > 0 {
			desc += fmt.Sprintf(" for each group defined by %s", strings.Join(n.GroupByExprs, ", "))
		} else {
			desc += " over all input rows"
		}
		return desc

	case *plan.SortNode:
		return fmt.Sprintf("Sort — orders all rows by %q %s (requires reading the full input before returning any rows)", n.SortKey, n.Order)

	case *plan.LimitNode:
		desc := fmt.Sprintf("Limit — returns at most %d row(s)", n.Limit)
		if n.Offset > 0 {
			desc += fmt.Sprintf(", skipping the first %d row(s)", n.Offset)
		}
		return desc

	case *plan.DistinctNode:
		return "Distinct — removes duplicate rows from the input"

	case *plan.SetOpNode:
		return fmt.Sprintf("%s — combines result sets from two sub-queries", n.OpType)

	case *plan.InsertNode:
		return fmt.Sprintf("Insert — writes %d new row(s) into %q", n.NumRows, n.TableName)

	case *plan.UpdateNode:
		return fmt.Sprintf("Update — modifies %d field(s) in matching rows of %q", n.SetFields, n.TableName)

	case *plan.DeleteNode:
		return fmt.Sprintf("Delete — removes matching rows from %q", n.TableName)

	case *plan.DDLNode:
		return fmt.Sprintf("DDL Operation: %s %q — modifies the schema, no rows are scanned", n.Operation, n.ObjectName)

	default:
		return fmt.Sprintf("%s (cost=%.2f, rows=%d)", node.GetNodeType(), node.GetCost(), node.GetCardinality())
	}
}

// formatPlanJSON formats the plan as JSON (basic implementation).
func (p *ExplainPlan) formatPlanJSON(planNode plan.PlanNode) string {
	// For now, return a simple JSON representation
	// A full implementation would use json.Marshal with proper structure
	var sb strings.Builder
	sb.WriteString("{\n")
	sb.WriteString(fmt.Sprintf("  \"nodeType\": %q,\n", planNode.GetNodeType()))
	sb.WriteString(fmt.Sprintf("  \"cost\": %.2f,\n", planNode.GetCost()))
	sb.WriteString(fmt.Sprintf("  \"rows\": %d,\n", planNode.GetCardinality()))
	sb.WriteString("  \"children\": [\n")

	children := planNode.GetChildren()
	for i, child := range children {
		if child != nil {
			sb.WriteString("    ")
			sb.WriteString(p.formatPlanJSON(child))
			if i < len(children)-1 {
				sb.WriteString(",")
			}
			sb.WriteString("\n")
		}
	}

	sb.WriteString("  ]\n")
	sb.WriteString("}")
	return sb.String()
}
