package optimizer

import (
	"math"
	"storemy/pkg/catalog/catalogmanager"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/optimizer/internal/cardinality"
	costmodel "storemy/pkg/optimizer/internal/cost_model"
	"storemy/pkg/plan"
)

// JoinOrderOptimizer uses dynamic programming to find optimal join order
// Supports both left-deep and bushy join trees
type JoinOrderOptimizer struct {
	catalog   *catalogmanager.CatalogManager
	costModel *costmodel.CostModel
	graph     *JoinGraph

	// DP table: maps relation set (bitmask) to best plan
	dpTable map[uint64]*JoinPlan

	// Configuration
	enableBushyTrees bool
	maxRelations     int // Maximum relations for full DP (default 10)
}

// JoinPlan represents a candidate join plan for a set of relations
type JoinPlan struct {
	RelationSet uint64        // Bitmask of relations in this plan
	PlanNode    plan.PlanNode // The actual plan tree
	Cost        float64       // Total cost
	Cardinality int64         // Estimated output rows
	JoinMethod  string        // Join method used (for multi-way joins)
}

// NewJoinOrderOptimizer creates a new join order optimizer
func NewJoinOrderOptimizer(
	cat *catalogmanager.CatalogManager,
	costModel *costmodel.CostModel,
	enableBushyTrees bool,
) *JoinOrderOptimizer {
	return &JoinOrderOptimizer{
		catalog:          cat,
		costModel:        costModel,
		dpTable:          make(map[uint64]*JoinPlan),
		enableBushyTrees: enableBushyTrees,
		maxRelations:     10, // Default limit for DP
	}
}

// OptimizeJoinOrder finds the optimal join order for a set of relations
func (joo *JoinOrderOptimizer) OptimizeJoinOrder(
	tx *transaction.TransactionContext,
	graph *JoinGraph,
) (plan.PlanNode, error) {
	joo.graph = graph
	joo.dpTable = make(map[uint64]*JoinPlan)

	relationCount := graph.GetRelationCount()
	if relationCount == 0 {
		return nil, nil
	}

	// Check if we should use DP or fall back to heuristic
	if relationCount > joo.maxRelations {
		// Too many relations for full DP, use greedy heuristic
		return joo.optimizeGreedy(tx, graph)
	}

	// Initialize DP table with base relations
	joo.initializeBaseRelations(tx, graph)

	// Build up plans for larger relation sets
	if joo.enableBushyTrees {
		joo.buildBushyPlans(tx, graph, relationCount)
	} else {
		joo.buildLeftDeepPlans(tx, graph, relationCount)
	}

	// Get the final plan for all relations
	allRelations := joo.getAllRelationSet(graph)
	finalPlan := joo.dpTable[allRelations]
	if finalPlan == nil {
		return nil, nil
	}

	return finalPlan.PlanNode, nil
}

// initializeBaseRelations initializes the DP table with single-relation plans
func (joo *JoinOrderOptimizer) initializeBaseRelations(
	tx *transaction.TransactionContext,
	graph *JoinGraph,
) {
	for _, rel := range graph.Relations {
		// Create a scan plan for this relation
		scanNode := rel.ScanNode
		if scanNode == nil {
			// Create a default scan node
			scanNode = &plan.ScanNode{
				TableName:    rel.TableName,
				TableID:      rel.TableID,
				AccessMethod: "seqscan",
				Alias:        rel.Alias,
				Predicates:   make([]plan.PredicateInfo, 0),
			}
		}

		// Apply pushed-down predicates
		for _, pred := range rel.Predicates {
			scanNode.Predicates = append(scanNode.Predicates, plan.PredicateInfo{
				Column:    pred.Column,
				Value:     "",
				Predicate: 0, // Would need to convert from string
			})
		}

		// Estimate cost and cardinality
		card, err := joo.costModel.GetCardinalityEstimator().EstimatePlanCardinality(scanNode)
		if err != nil {
			// On error, use a default cardinality
			card = cardinality.DefaultTableCardinality
		}
		cost := joo.costModel.EstimatePlanCost(scanNode)
		scanNode.SetCardinality(int64(card))
		scanNode.SetCost(cost)

		// Add to DP table
		joo.dpTable[rel.BitMask] = &JoinPlan{
			RelationSet: rel.BitMask,
			PlanNode:    scanNode,
			Cost:        cost,
			Cardinality: int64(card),
		}
	}
}

// buildLeftDeepPlans builds left-deep join trees using DP
func (joo *JoinOrderOptimizer) buildLeftDeepPlans(
	tx *transaction.TransactionContext,
	graph *JoinGraph,
	relationCount int,
) {
	// For each subset size from 2 to n
	for size := 2; size <= relationCount; size++ {
		// Generate all subsets of this size
		subsets := joo.generateSubsets(graph, size)

		for _, subset := range subsets {
			// Try joining this subset in all possible ways (left-deep only)
			joo.findBestLeftDeepJoin(tx, graph, subset)
		}
	}
}

// buildBushyPlans builds bushy join trees using DP
func (joo *JoinOrderOptimizer) buildBushyPlans(
	tx *transaction.TransactionContext,
	graph *JoinGraph,
	relationCount int,
) {
	// For each subset size from 2 to n
	for size := 2; size <= relationCount; size++ {
		// Generate all subsets of this size
		subsets := joo.generateSubsets(graph, size)

		for _, subset := range subsets {
			// Try all possible partitions of this subset (bushy trees)
			joo.findBestBushyJoin(tx, graph, subset)
		}
	}
}

// findBestLeftDeepJoin finds the best left-deep join for a relation set
func (joo *JoinOrderOptimizer) findBestLeftDeepJoin(
	tx *transaction.TransactionContext,
	graph *JoinGraph,
	relationSet uint64,
) {
	var bestPlan *JoinPlan
	bestCost := math.MaxFloat64

	// Try each relation as the right input
	for _, rel := range graph.Relations {
		if (relationSet & rel.BitMask) == 0 {
			// This relation is not in the set
			continue
		}

		// Left side is all relations except this one
		leftSet := relationSet & ^rel.BitMask
		if leftSet == 0 {
			// Can't have empty left side
			continue
		}

		// Check if we have a plan for the left side
		leftPlan := joo.dpTable[leftSet]
		rightPlan := joo.dpTable[rel.BitMask]
		if leftPlan == nil || rightPlan == nil {
			continue
		}

		// Check if these sets can be joined
		if !graph.IsConnected(leftSet, rel.BitMask) {
			continue
		}

		// Try different join methods
		joinMethods := []string{"hash", "nested", "merge"}
		for _, method := range joinMethods {
			plan := joo.createJoinPlan(tx, graph, leftPlan, rightPlan, method, relationSet)
			if plan != nil && plan.Cost < bestCost {
				bestCost = plan.Cost
				bestPlan = plan
			}
		}
	}

	if bestPlan != nil {
		joo.dpTable[relationSet] = bestPlan
	}
}

// findBestBushyJoin finds the best bushy join for a relation set
func (joo *JoinOrderOptimizer) findBestBushyJoin(
	tx *transaction.TransactionContext,
	graph *JoinGraph,
	relationSet uint64,
) {
	var bestPlan *JoinPlan
	bestCost := math.MaxFloat64

	// Try all possible partitions of the relation set
	// We enumerate all subsets and their complements
	subsets := joo.enumerateSubsets(relationSet)

	for _, leftSet := range subsets {
		// Skip empty set and full set
		if leftSet == 0 || leftSet == relationSet {
			continue
		}

		rightSet := relationSet & ^leftSet
		if rightSet == 0 {
			continue
		}

		// Skip if we've already considered this partition (avoid duplicates)
		if leftSet > rightSet {
			continue
		}

		// Check if we have plans for both sides
		leftPlan := joo.dpTable[leftSet]
		rightPlan := joo.dpTable[rightSet]
		if leftPlan == nil || rightPlan == nil {
			continue
		}

		// Check if these sets can be joined
		if !graph.IsConnected(leftSet, rightSet) {
			continue
		}

		// Try different join methods
		joinMethods := []string{"hash", "nested", "merge"}
		for _, method := range joinMethods {
			plan := joo.createJoinPlan(tx, graph, leftPlan, rightPlan, method, relationSet)
			if plan != nil && plan.Cost < bestCost {
				bestCost = plan.Cost
				bestPlan = plan
			}
		}
	}

	if bestPlan != nil {
		joo.dpTable[relationSet] = bestPlan
	}
}

// createJoinPlan creates a join plan between two subplans
func (joo *JoinOrderOptimizer) createJoinPlan(
	tx *transaction.TransactionContext,
	graph *JoinGraph,
	leftPlan, rightPlan *JoinPlan,
	joinMethod string,
	relationSet uint64,
) *JoinPlan {
	// Get join predicates for this relation set
	predicates := graph.GetApplicablePredicates(relationSet)
	if len(predicates) == 0 {
		// No join condition, this would be a cross product
		// Skip unless explicitly allowed
		return nil
	}

	// Find the most selective join predicate
	var bestPredicate *JoinPredicate
	for _, pred := range predicates {
		leftMask := graph.Relations[pred.LeftRelation].BitMask
		rightMask := graph.Relations[pred.RightRelation].BitMask

		// Check if this predicate connects the two subplans
		if ((leftMask&leftPlan.RelationSet) != 0 && (rightMask&rightPlan.RelationSet) != 0) ||
			((leftMask&rightPlan.RelationSet) != 0 && (rightMask&leftPlan.RelationSet) != 0) {
			bestPredicate = pred
			break
		}
	}

	if bestPredicate == nil {
		return nil
	}

	// Create join node
	joinNode := &plan.JoinNode{
		LeftChild:    leftPlan.PlanNode,
		RightChild:   rightPlan.PlanNode,
		JoinType:     "inner",
		JoinMethod:   joinMethod,
		LeftColumn:   bestPredicate.LeftColumn,
		RightColumn:  bestPredicate.RightColumn,
		ExtraFilters: make([]plan.PredicateInfo, 0),
	}

	// Estimate cost and cardinality
	card, err := joo.costModel.GetCardinalityEstimator().EstimatePlanCardinality(joinNode)
	if err != nil {
		// On error, use a default cardinality
		card = cardinality.DefaultTableCardinality
	}
	cost := joo.costModel.EstimatePlanCost(joinNode)
	joinNode.SetCardinality(int64(card))
	joinNode.SetCost(cost)

	return &JoinPlan{
		RelationSet: relationSet,
		PlanNode:    joinNode,
		Cost:        cost,
		Cardinality: int64(card),
		JoinMethod:  joinMethod,
	}
}

// generateSubsets generates all subsets of a given size
func (joo *JoinOrderOptimizer) generateSubsets(graph *JoinGraph, size int) []uint64 {
	subsets := make([]uint64, 0)
	relationIDs := graph.GetRelationIDs()

	// Use a recursive approach to generate combinations
	joo.generateCombinations(relationIDs, 0, 0, size, 0, &subsets)

	return subsets
}

// generateCombinations is a helper for generating subsets
func (joo *JoinOrderOptimizer) generateCombinations(
	relations []int,
	start int,
	current uint64,
	remaining int,
	count int,
	result *[]uint64,
) {
	if remaining == 0 {
		*result = append(*result, current)
		return
	}

	for i := start; i < len(relations) && len(relations)-i >= remaining; i++ {
		rel := joo.graph.Relations[relations[i]]
		joo.generateCombinations(
			relations,
			i+1,
			current|rel.BitMask,
			remaining-1,
			count+1,
			result,
		)
	}
}

// enumerateSubsets enumerates all non-empty proper subsets of a relation set
func (joo *JoinOrderOptimizer) enumerateSubsets(relationSet uint64) []uint64 {
	subsets := make([]uint64, 0)

	// Iterate through all possible subsets
	// Use bit manipulation to enumerate
	for subset := uint64(1); subset < relationSet; subset++ {
		if (subset & relationSet) == subset {
			subsets = append(subsets, subset)
		}
	}

	return subsets
}

// getAllRelationSet returns the bitmask for all relations
func (joo *JoinOrderOptimizer) getAllRelationSet(graph *JoinGraph) uint64 {
	var allSet uint64
	for _, rel := range graph.Relations {
		allSet |= rel.BitMask
	}
	return allSet
}

// optimizeGreedy uses a greedy algorithm for large join graphs
func (joo *JoinOrderOptimizer) optimizeGreedy(
	tx *transaction.TransactionContext,
	graph *JoinGraph,
) (plan.PlanNode, error) {
	// Greedy algorithm: repeatedly join the pair with lowest cost
	// This is a fallback for when DP is too expensive

	// Initialize with base relations
	joo.initializeBaseRelations(tx, graph)

	// Keep track of remaining plans
	remaining := make(map[uint64]*JoinPlan)
	for mask, plan := range joo.dpTable {
		remaining[mask] = plan
	}

	// Repeatedly join the best pair
	for len(remaining) > 1 {
		var bestLeft, bestRight uint64
		var bestPlan *JoinPlan
		bestCost := math.MaxFloat64

		// Find best pair to join
		for leftMask, leftPlan := range remaining {
			for rightMask, rightPlan := range remaining {
				if leftMask == rightMask || leftMask >= rightMask {
					continue
				}

				// Check if they can be joined
				if !graph.IsConnected(leftMask, rightMask) {
					continue
				}

				// Try joining them
				combinedSet := leftMask | rightMask
				plan := joo.createJoinPlan(tx, graph, leftPlan, rightPlan, "hash", combinedSet)
				if plan != nil && plan.Cost < bestCost {
					bestCost = plan.Cost
					bestPlan = plan
					bestLeft = leftMask
					bestRight = rightMask
				}
			}
		}

		if bestPlan == nil {
			// No valid join found, might be a cross product situation
			break
		}

		// Remove the joined plans and add the new plan
		delete(remaining, bestLeft)
		delete(remaining, bestRight)
		remaining[bestPlan.RelationSet] = bestPlan
	}

	// Return the final plan
	for _, plan := range remaining {
		return plan.PlanNode, nil
	}

	return nil, nil
}

// SetMaxRelations sets the maximum number of relations for full DP
func (joo *JoinOrderOptimizer) SetMaxRelations(max int) {
	joo.maxRelations = max
}

// GetDPTableSize returns the size of the DP table (for debugging)
func (joo *JoinOrderOptimizer) GetDPTableSize() int {
	return len(joo.dpTable)
}
