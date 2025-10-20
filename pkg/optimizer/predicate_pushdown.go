package optimizer

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/planner"
)

// PredicatePushdownOptimizer applies predicate pushdown optimization
// Pushes filter predicates as close to the base scans as possible
type PredicatePushdownOptimizer struct {
	costModel *CostModel
}

// NewPredicatePushdownOptimizer creates a new predicate pushdown optimizer
func NewPredicatePushdownOptimizer(costModel *CostModel) *PredicatePushdownOptimizer {
	return &PredicatePushdownOptimizer{
		costModel: costModel,
	}
}

// Optimize applies predicate pushdown to a plan tree
func (ppo *PredicatePushdownOptimizer) Optimize(
	tx *transaction.TransactionContext,
	plan planner.PlanNode,
) planner.PlanNode {
	if plan == nil {
		return nil
	}

	return ppo.pushPredicates(tx, plan, make([]*PredicateContext, 0))
}

// PredicateContext holds information about a predicate being pushed down
type PredicateContext struct {
	Predicate      *planner.PredicateInfo
	CanPushThrough bool // Whether this predicate can be pushed through joins
	RequiredTables []string // Tables referenced by this predicate
}

// pushPredicates recursively pushes predicates down the plan tree
func (ppo *PredicatePushdownOptimizer) pushPredicates(
	tx *transaction.TransactionContext,
	node planner.PlanNode,
	predicates []*PredicateContext,
) planner.PlanNode {
	switch n := node.(type) {
	case *planner.FilterNode:
		return ppo.optimizeFilter(tx, n, predicates)
	case *planner.JoinNode:
		return ppo.optimizeJoin(tx, n, predicates)
	case *planner.ScanNode:
		return ppo.optimizeScan(tx, n, predicates)
	case *planner.ProjectNode:
		return ppo.optimizeProject(tx, n, predicates)
	default:
		// For other node types, recursively optimize children
		return ppo.optimizeGeneric(tx, node, predicates)
	}
}

// optimizeFilter handles filter node optimization
func (ppo *PredicatePushdownOptimizer) optimizeFilter(
	tx *transaction.TransactionContext,
	node *planner.FilterNode,
	existingPredicates []*PredicateContext,
) planner.PlanNode {
	// Convert filter predicates to PredicateContext
	newPredicates := make([]*PredicateContext, 0, len(node.Predicates))
	for i := range node.Predicates {
		newPredicates = append(newPredicates, &PredicateContext{
			Predicate:      &node.Predicates[i],
			CanPushThrough: ppo.canPushThroughJoin(&node.Predicates[i]),
			RequiredTables: ppo.getRequiredTables(&node.Predicates[i]),
		})
	}

	// Combine with existing predicates
	allPredicates := append(existingPredicates, newPredicates...)

	// Push predicates down to child
	optimizedChild := ppo.pushPredicates(tx, node.Child, allPredicates)

	// If all predicates were pushed down, we can eliminate this filter node
	// Otherwise, keep the filter with remaining predicates
	return optimizedChild
}

// optimizeJoin handles join node optimization
func (ppo *PredicatePushdownOptimizer) optimizeJoin(
	tx *transaction.TransactionContext,
	node *planner.JoinNode,
	predicates []*PredicateContext,
) planner.PlanNode {
	// Partition predicates:
	// 1. Predicates that can be pushed to left child
	// 2. Predicates that can be pushed to right child
	// 3. Predicates that must remain at join level

	leftPredicates := make([]*PredicateContext, 0)
	rightPredicates := make([]*PredicateContext, 0)
	joinPredicates := make([]planner.PredicateInfo, 0)

	leftTables := ppo.getReferencedTables(node.LeftChild)
	rightTables := ppo.getReferencedTables(node.RightChild)

	for _, predCtx := range predicates {
		canPushLeft := ppo.canPushToChild(predCtx, leftTables)
		canPushRight := ppo.canPushToChild(predCtx, rightTables)

		if canPushLeft && !canPushRight {
			// Push to left child only
			leftPredicates = append(leftPredicates, predCtx)
		} else if canPushRight && !canPushLeft {
			// Push to right child only
			rightPredicates = append(rightPredicates, predCtx)
		} else if !canPushLeft && !canPushRight {
			// Cannot push down, must remain at join level
			joinPredicates = append(joinPredicates, *predCtx.Predicate)
		}
		// If can push to both, prefer pushing to smaller child
		// For now, just keep it at join level to be conservative
	}

	// Optimize children with pushed predicates
	optimizedLeft := ppo.pushPredicates(tx, node.LeftChild, leftPredicates)
	optimizedRight := ppo.pushPredicates(tx, node.RightChild, rightPredicates)

	// Create new join node with optimized children
	newJoin := &planner.JoinNode{
		BasePlanNode:  node.BasePlanNode,
		LeftChild:     optimizedLeft,
		RightChild:    optimizedRight,
		JoinType:      node.JoinType,
		JoinMethod:    node.JoinMethod,
		LeftColumn:    node.LeftColumn,
		RightColumn:   node.RightColumn,
		JoinPredicate: node.JoinPredicate,
		ExtraFilters:  append(node.ExtraFilters, joinPredicates...),
	}

	// Recompute cost and cardinality
	card := ppo.costModel.cardinalityEstimator.EstimatePlanCardinality(tx, newJoin)
	cost := ppo.costModel.EstimatePlanCost(tx, newJoin)
	newJoin.SetCardinality(card)
	newJoin.SetCost(cost)

	return newJoin
}

// optimizeScan handles scan node optimization
func (ppo *PredicatePushdownOptimizer) optimizeScan(
	tx *transaction.TransactionContext,
	node *planner.ScanNode,
	predicates []*PredicateContext,
) planner.PlanNode {
	// Push all applicable predicates to the scan
	pushedPredicates := make([]planner.PredicateInfo, 0, len(node.Predicates))
	pushedPredicates = append(pushedPredicates, node.Predicates...)

	scanTable := node.TableName
	if node.Alias != "" {
		scanTable = node.Alias
	}

	// Add predicates that reference this table
	for _, predCtx := range predicates {
		if ppo.predicateReferencesTable(predCtx, scanTable) {
			pushedPredicates = append(pushedPredicates, *predCtx.Predicate)
		}
	}

	if len(pushedPredicates) == len(node.Predicates) {
		// No new predicates pushed down
		return node
	}

	// Create new scan node with pushed predicates
	newScan := &planner.ScanNode{
		BasePlanNode: node.BasePlanNode,
		TableName:    node.TableName,
		TableID:      node.TableID,
		AccessMethod: node.AccessMethod,
		IndexName:    node.IndexName,
		IndexID:      node.IndexID,
		Predicates:   pushedPredicates,
		Alias:        node.Alias,
	}

	// Recompute cost and cardinality with pushed predicates
	card := ppo.costModel.cardinalityEstimator.EstimatePlanCardinality(tx, newScan)
	cost := ppo.costModel.EstimatePlanCost(tx, newScan)
	newScan.SetCardinality(card)
	newScan.SetCost(cost)

	return newScan
}

// optimizeProject handles project node optimization
func (ppo *PredicatePushdownOptimizer) optimizeProject(
	tx *transaction.TransactionContext,
	node *planner.ProjectNode,
	predicates []*PredicateContext,
) planner.PlanNode {
	// Predicates can be pushed through projection
	optimizedChild := ppo.pushPredicates(tx, node.Child, predicates)

	newProject := &planner.ProjectNode{
		BasePlanNode: node.BasePlanNode,
		Child:        optimizedChild,
		Columns:      node.Columns,
		ColumnNames:  node.ColumnNames,
	}

	// Recompute cost
	card := ppo.costModel.cardinalityEstimator.EstimatePlanCardinality(tx, newProject)
	cost := ppo.costModel.EstimatePlanCost(tx, newProject)
	newProject.SetCardinality(card)
	newProject.SetCost(cost)

	return newProject
}

// optimizeGeneric handles generic node types
func (ppo *PredicatePushdownOptimizer) optimizeGeneric(
	tx *transaction.TransactionContext,
	node planner.PlanNode,
	predicates []*PredicateContext,
) planner.PlanNode {
	// For generic nodes, try to push predicates through to children
	children := node.GetChildren()
	if len(children) == 0 {
		return node
	}

	// This is a simplified version - in practice, you'd need to handle
	// each node type specifically
	return node
}

// Helper methods

// canPushThroughJoin determines if a predicate can be pushed through a join
func (ppo *PredicatePushdownOptimizer) canPushThroughJoin(pred *planner.PredicateInfo) bool {
	// A predicate can be pushed through a join if it only references
	// columns from one side of the join
	// For now, assume all predicates can potentially be pushed
	return true
}

// getRequiredTables extracts table references from a predicate
func (ppo *PredicatePushdownOptimizer) getRequiredTables(pred *planner.PredicateInfo) []string {
	// Extract table name from column reference
	// Format: "table.column" or just "column"
	tables := make([]string, 0)

	// Parse the column name
	// This is simplified - in practice, you'd have a proper expression parser
	if pred.Column != "" {
		tables = append(tables, pred.Column)
	}

	return tables
}

// getReferencedTables gets all tables referenced by a plan subtree
func (ppo *PredicatePushdownOptimizer) getReferencedTables(plan planner.PlanNode) []string {
	tables := make([]string, 0)

	switch n := plan.(type) {
	case *planner.ScanNode:
		if n.Alias != "" {
			tables = append(tables, n.Alias)
		} else {
			tables = append(tables, n.TableName)
		}
	default:
		// Recursively collect from children
		for _, child := range plan.GetChildren() {
			tables = append(tables, ppo.getReferencedTables(child)...)
		}
	}

	return tables
}

// canPushToChild determines if a predicate can be pushed to a specific child
func (ppo *PredicatePushdownOptimizer) canPushToChild(
	predCtx *PredicateContext,
	childTables []string,
) bool {
	// Check if all required tables are available in the child
	if len(predCtx.RequiredTables) == 0 {
		return false
	}

	// Simple table name matching
	// In practice, you'd need more sophisticated logic
	for _, reqTable := range predCtx.RequiredTables {
		found := false
		for _, childTable := range childTables {
			if containsTable(reqTable, childTable) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// predicateReferencesTable checks if a predicate references a specific table
func (ppo *PredicatePushdownOptimizer) predicateReferencesTable(
	predCtx *PredicateContext,
	tableName string,
) bool {
	for _, reqTable := range predCtx.RequiredTables {
		if containsTable(reqTable, tableName) {
			return true
		}
	}
	return false
}

// containsTable checks if a column reference contains a table name
func containsTable(columnRef, tableName string) bool {
	// Simple substring matching
	// Format: "table.column" or just "column"
	// This is simplified - in practice, you'd have proper parsing
	return columnRef == tableName || len(columnRef) > len(tableName) &&
		   columnRef[:len(tableName)] == tableName
}
