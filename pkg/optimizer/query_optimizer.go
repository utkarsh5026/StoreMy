package optimizer

import (
	"fmt"
	"storemy/pkg/catalog"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/optimizer/cardinality"
	"storemy/pkg/planner"
)

// QueryOptimizer is the main optimizer that applies various optimization strategies
type QueryOptimizer struct {
	catalog            *catalog.SystemCatalog
	costModel          *CostModel
	predicatePushdown  *PredicatePushdownOptimizer
	joinOrderOptimizer *JoinOrderOptimizer

	// Configuration
	enablePredicatePushdown bool
	enableJoinReordering    bool
	enableBushyJoins        bool
	enableIndexSelection    bool
}

// OptimizerConfig holds configuration for the optimizer
type OptimizerConfig struct {
	EnablePredicatePushdown bool
	EnableJoinReordering    bool
	EnableBushyJoins        bool
	EnableIndexSelection    bool
	MaxJoinRelations        int
}

// DefaultOptimizerConfig returns default optimizer configuration
func DefaultOptimizerConfig() *OptimizerConfig {
	return &OptimizerConfig{
		EnablePredicatePushdown: true,
		EnableJoinReordering:    true,
		EnableBushyJoins:        false, // Left-deep by default for simplicity
		EnableIndexSelection:    true,
		MaxJoinRelations:        10,
	}
}

// NewQueryOptimizer creates a new query optimizer
func NewQueryOptimizer(cat *catalog.SystemCatalog, config *OptimizerConfig) *QueryOptimizer {
	if config == nil {
		config = DefaultOptimizerConfig()
	}

	costModel := NewCostModel(cat)
	predicatePushdown := NewPredicatePushdownOptimizer(costModel)
	joinOrderOptimizer := NewJoinOrderOptimizer(cat, costModel, config.EnableBushyJoins)
	joinOrderOptimizer.SetMaxRelations(config.MaxJoinRelations)

	return &QueryOptimizer{
		catalog:                 cat,
		costModel:               costModel,
		predicatePushdown:       predicatePushdown,
		joinOrderOptimizer:      joinOrderOptimizer,
		enablePredicatePushdown: config.EnablePredicatePushdown,
		enableJoinReordering:    config.EnableJoinReordering,
		enableBushyJoins:        config.EnableBushyJoins,
		enableIndexSelection:    config.EnableIndexSelection,
	}
}

// Optimize applies all optimization strategies to a query plan
func (qo *QueryOptimizer) Optimize(
	tx *transaction.TransactionContext,
	plan planner.PlanNode,
) (planner.PlanNode, error) {
	if plan == nil {
		return nil, nil
	}

	optimizedPlan := plan

	// Phase 1: Logical optimization
	// Apply predicate pushdown
	if qo.enablePredicatePushdown {
		optimizedPlan = qo.predicatePushdown.Optimize(tx, optimizedPlan)
	}

	// Phase 2: Join optimization
	// If the plan contains joins, optimize join order
	if qo.enableJoinReordering && qo.containsJoins(optimizedPlan) {
		graph, err := qo.extractJoinGraph(tx, optimizedPlan)
		if err == nil && graph != nil && graph.GetRelationCount() > 1 {
			reorderedPlan, err := qo.joinOrderOptimizer.OptimizeJoinOrder(tx, graph)
			if err == nil && reorderedPlan != nil {
				optimizedPlan = reorderedPlan
			}
		}
	}

	// Phase 3: Physical optimization
	// Select access methods (index vs seq scan)
	if qo.enableIndexSelection {
		optimizedPlan = qo.selectAccessMethods(tx, optimizedPlan)
	}

	// Phase 4: Final cost estimation
	qo.estimateFinalCosts(tx, optimizedPlan)

	return optimizedPlan, nil
}

// OptimizeJoinOrder optimizes just the join order of a query
func (qo *QueryOptimizer) OptimizeJoinOrder(
	tx *transaction.TransactionContext,
	graph *JoinGraph,
) (planner.PlanNode, error) {
	return qo.joinOrderOptimizer.OptimizeJoinOrder(tx, graph)
}

// extractJoinGraph extracts a join graph from a plan tree
func (qo *QueryOptimizer) extractJoinGraph(
	tx *transaction.TransactionContext,
	plan planner.PlanNode,
) (*JoinGraph, error) {
	graph := NewJoinGraph()
	relationID := 0

	err := qo.extractJoinGraphRecursive(tx, plan, graph, &relationID)
	if err != nil {
		return nil, err
	}

	return graph, nil
}

// extractJoinGraphRecursive recursively extracts join information
func (qo *QueryOptimizer) extractJoinGraphRecursive(
	tx *transaction.TransactionContext,
	node planner.PlanNode,
	graph *JoinGraph,
	relationID *int,
) error {
	if node == nil {
		return nil
	}

	switch n := node.(type) {
	case *planner.ScanNode:
		// Add base relation
		rel := &Relation{
			ID:         *relationID,
			TableName:  n.TableName,
			TableID:    n.TableID,
			Alias:      n.Alias,
			ScanNode:   n,
			Predicates: make([]*FilterPredicate, 0),
		}
		*relationID++

		// Extract filter predicates
		for i := range n.Predicates {
			pred := &FilterPredicate{
				Column:   n.Predicates[i].Column,
				Operator: "=", // Simplified
			}
			rel.Predicates = append(rel.Predicates, pred)
		}

		graph.AddRelation(rel)

	case *planner.JoinNode:
		// Recursively extract from children first
		err := qo.extractJoinGraphRecursive(tx, n.LeftChild, graph, relationID)
		if err != nil {
			return err
		}
		err = qo.extractJoinGraphRecursive(tx, n.RightChild, graph, relationID)
		if err != nil {
			return err
		}

		// Extract join predicate
		// Find the relations involved
		leftRelations := qo.findRelations(n.LeftChild, graph)
		rightRelations := qo.findRelations(n.RightChild, graph)

		if len(leftRelations) > 0 && len(rightRelations) > 0 {
			// Create join predicate (simplified - assumes single column joins)
			joinPred := &JoinPredicate{
				LeftRelation:  leftRelations[0],
				LeftColumn:    n.LeftColumn,
				RightRelation: rightRelations[0],
				RightColumn:   n.RightColumn,
				Operator:      "=",
			}
			graph.AddJoinPredicate(joinPred)
		}

	case *planner.FilterNode:
		// Extract predicates and recurse
		err := qo.extractJoinGraphRecursive(tx, n.Child, graph, relationID)
		if err != nil {
			return err
		}

	default:
		// For other node types, recurse to children
		for _, child := range node.GetChildren() {
			err := qo.extractJoinGraphRecursive(tx, child, graph, relationID)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// findRelations finds all relation IDs referenced in a plan subtree
func (qo *QueryOptimizer) findRelations(node planner.PlanNode, graph *JoinGraph) []int {
	if node == nil {
		return nil
	}

	var relations []int

	switch n := node.(type) {
	case *planner.ScanNode:
		// Find this scan in the graph
		for id, rel := range graph.Relations {
			if rel.TableID == n.TableID {
				relations = append(relations, id)
			}
		}
	default:
		// Recurse to children
		for _, child := range node.GetChildren() {
			relations = append(relations, qo.findRelations(child, graph)...)
		}
	}

	return relations
}

// containsJoins checks if a plan tree contains any joins
func (qo *QueryOptimizer) containsJoins(plan planner.PlanNode) bool {
	if plan == nil {
		return false
	}

	if _, ok := plan.(*planner.JoinNode); ok {
		return true
	}

	for _, child := range plan.GetChildren() {
		if qo.containsJoins(child) {
			return true
		}
	}

	return false
}

// selectAccessMethods chooses between index and sequential scans
func (qo *QueryOptimizer) selectAccessMethods(
	tx *transaction.TransactionContext,
	plan planner.PlanNode,
) planner.PlanNode {
	if plan == nil {
		return nil
	}

	switch n := plan.(type) {
	case *planner.ScanNode:
		return qo.chooseBestAccessMethod(tx, n)
	default:
		// Recursively optimize children
		children := plan.GetChildren()
		for i, child := range children {
			children[i] = qo.selectAccessMethods(tx, child)
		}
		return plan
	}
}

// chooseBestAccessMethod selects the best access method for a scan
func (qo *QueryOptimizer) chooseBestAccessMethod(
	tx *transaction.TransactionContext,
	scan *planner.ScanNode,
) planner.PlanNode {
	// If already using an access method, keep it
	if scan.AccessMethod != "" && scan.AccessMethod != "seqscan" {
		return scan
	}

	// Get available indexes for this table
	indexes, err := qo.catalog.GetIndexesByTable(tx, scan.TableID)
	if err != nil || len(indexes) == 0 {
		// No indexes available, use sequential scan
		scan.AccessMethod = "seqscan"
		return scan
	}

	// Estimate cost of sequential scan
	seqScanNode := &planner.ScanNode{
		BasePlanNode: scan.BasePlanNode,
		TableName:    scan.TableName,
		TableID:      scan.TableID,
		AccessMethod: "seqscan",
		Predicates:   scan.Predicates,
		Alias:        scan.Alias,
	}
	seqScanCost := qo.costModel.EstimatePlanCost(tx, seqScanNode)

	bestNode := seqScanNode
	bestCost := seqScanCost

	// Try each index
	for _, index := range indexes {
		// Check if any predicate can use this index
		canUseIndex := false
		for _, pred := range scan.Predicates {
			if qo.predicateMatchesIndex(pred, index) {
				canUseIndex = true
				break
			}
		}

		if !canUseIndex {
			continue
		}

		// Estimate index scan cost
		indexScanNode := &planner.ScanNode{
			BasePlanNode: scan.BasePlanNode,
			TableName:    scan.TableName,
			TableID:      scan.TableID,
			AccessMethod: "indexscan",
			IndexName:    index.IndexName,
			IndexID:      index.IndexID,
			Predicates:   scan.Predicates,
			Alias:        scan.Alias,
		}
		indexScanCost := qo.costModel.EstimatePlanCost(tx, indexScanNode)

		if indexScanCost < bestCost {
			bestCost = indexScanCost
			bestNode = indexScanNode
		}
	}

	return bestNode
}

// predicateMatchesIndex checks if a predicate can use an index
func (qo *QueryOptimizer) predicateMatchesIndex(
	pred planner.PredicateInfo,
	index *systemtable.IndexMetadata,
) bool {
	// Check if the predicate column matches the index key column
	// This is simplified - real implementation would handle multi-column indexes
	return pred.Column == index.ColumnName
}

// estimateFinalCosts recursively estimates costs for the final plan
func (qo *QueryOptimizer) estimateFinalCosts(
	tx *transaction.TransactionContext,
	plan planner.PlanNode,
) {
	if plan == nil {
		return
	}

	// Recursively estimate costs for children first
	for _, child := range plan.GetChildren() {
		qo.estimateFinalCosts(tx, child)
	}

	// Estimate cost for this node
	card, err := qo.costModel.cardinalityEstimator.EstimatePlanCardinality(tx, plan)
	if err != nil {
		// On error, use a default cardinality
		card = cardinality.DefaultTableCardinality
	}
	cost := qo.costModel.EstimatePlanCost(tx, plan)
	plan.SetCardinality(card)
	plan.SetCost(cost)
}

// ExplainPlan generates an explanation of the query plan with costs
func (qo *QueryOptimizer) ExplainPlan(plan planner.PlanNode) string {
	if plan == nil {
		return "No plan"
	}
	return qo.explainPlanRecursive(plan, 0)
}

// explainPlanRecursive generates indented plan explanation
func (qo *QueryOptimizer) explainPlanRecursive(node planner.PlanNode, indent int) string {
	if node == nil {
		return ""
	}

	prefix := ""
	for i := 0; i < indent; i++ {
		prefix += "  "
	}

	result := fmt.Sprintf("%s%s (cost=%.2f, rows=%d)\n",
		prefix, node.GetNodeType(), node.GetCost(), node.GetCardinality())

	for _, child := range node.GetChildren() {
		result += qo.explainPlanRecursive(child, indent+1)
	}

	return result
}

// GetCostModel returns the cost model (for testing/tuning)
func (qo *QueryOptimizer) GetCostModel() *CostModel {
	return qo.costModel
}

// CreateJoinGraph creates a join graph from scan nodes and join conditions
// This is a helper for manual join optimization
func (qo *QueryOptimizer) CreateJoinGraph(
	scans []*planner.ScanNode,
	joinPredicates []*JoinPredicate,
) *JoinGraph {
	graph := NewJoinGraph()

	// Add all relations
	for i, scan := range scans {
		rel := &Relation{
			ID:         i,
			TableName:  scan.TableName,
			TableID:    scan.TableID,
			Alias:      scan.Alias,
			ScanNode:   scan,
			Predicates: make([]*FilterPredicate, 0),
		}

		// Convert scan predicates
		for _, pred := range scan.Predicates {
			rel.Predicates = append(rel.Predicates, &FilterPredicate{
				Column:   pred.Column,
				Operator: "=",
			})
		}

		graph.AddRelation(rel)
	}

	// Add join predicates
	for _, pred := range joinPredicates {
		graph.AddJoinPredicate(pred)
	}

	return graph
}
