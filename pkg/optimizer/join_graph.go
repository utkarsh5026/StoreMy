package optimizer

import (
	"fmt"
	"storemy/pkg/plan"
	"storemy/pkg/primitives"
)

// JoinGraph represents a of relations and their join predicates
// Used for multi-way join optimization
type JoinGraph struct {
	// Relations maps relation ID to the base scan/relation
	Relations map[int]*Relation

	// Edges stores join predicates connecting relations
	// Key is a bitmask representing the set of relations involved
	Edges []*JoinEdge

	// PredicateMap maps relation sets to their join predicates
	PredicateMap map[uint64][]*JoinPredicate
}

// Relation represents a base relation (table) in the join graph
type Relation struct {
	ID         int                // Unique identifier
	TableName  string             // Table name
	TableID    primitives.FileID  // Database table ID
	Alias      string             // Table alias
	ScanNode   *plan.ScanNode     // Base scan plan
	Predicates []*FilterPredicate // Pushed-down predicates
	BitMask    uint64             // Bitmask for this relation (1 << ID)
}

// JoinEdge represents a join between multiple relations
type JoinEdge struct {
	// RelationSet is a bitmask of relations involved in this join
	RelationSet uint64

	// Predicates are the join conditions
	Predicates []*JoinPredicate

	// Selectivity is the estimated selectivity of the join
	Selectivity float64
}

// JoinPredicate represents a join condition between two relations
type JoinPredicate struct {
	LeftRelation  int    // Left relation ID
	LeftColumn    string // Left column name
	RightRelation int    // Right relation ID
	RightColumn   string // Right column name
	Operator      string // Join operator (=, <, >, etc.)
}

// FilterPredicate represents a filter condition on a single relation
type FilterPredicate struct {
	Column      string
	Operator    string
	Value       interface{}
	Selectivity float64
}

// NewJoinGraph creates a new join graph
func NewJoinGraph() *JoinGraph {
	return &JoinGraph{
		Relations:    make(map[int]*Relation),
		Edges:        make([]*JoinEdge, 0),
		PredicateMap: make(map[uint64][]*JoinPredicate),
	}
}

// AddRelation adds a relation to the join graph
func (jg *JoinGraph) AddRelation(rel *Relation) {
	rel.BitMask = 1 << uint(rel.ID) // #nosec G115
	jg.Relations[rel.ID] = rel
}

// AddJoinPredicate adds a join predicate between two relations
func (jg *JoinGraph) AddJoinPredicate(pred *JoinPredicate) {
	// Calculate the relation set for this predicate
	leftMask := jg.Relations[pred.LeftRelation].BitMask
	rightMask := jg.Relations[pred.RightRelation].BitMask
	relationSet := leftMask | rightMask

	// Add to predicate map
	jg.PredicateMap[relationSet] = append(jg.PredicateMap[relationSet], pred)

	// Find or create edge
	edge := jg.findEdge(relationSet)
	if edge == nil {
		edge = &JoinEdge{
			RelationSet: relationSet,
			Predicates:  make([]*JoinPredicate, 0),
			Selectivity: 1.0,
		}
		jg.Edges = append(jg.Edges, edge)
	}
	edge.Predicates = append(edge.Predicates, pred)
}

// AddFilterPredicate adds a filter predicate to a relation
func (jg *JoinGraph) AddFilterPredicate(relationID int, pred *FilterPredicate) error {
	rel, ok := jg.Relations[relationID]
	if !ok {
		return fmt.Errorf("relation %d not found", relationID)
	}
	rel.Predicates = append(rel.Predicates, pred)
	return nil
}

// GetJoinPredicates returns all join predicates for a given relation set
func (jg *JoinGraph) GetJoinPredicates(relationSet uint64) []*JoinPredicate {
	return jg.PredicateMap[relationSet]
}

// GetApplicablePredicates returns all predicates applicable to the given relation set
// This includes predicates between subsets of the relation set
func (jg *JoinGraph) GetApplicablePredicates(relationSet uint64) []*JoinPredicate {
	predicates := make([]*JoinPredicate, 0)

	// Check each stored predicate set
	for predSet, preds := range jg.PredicateMap {
		// If predSet is a subset of relationSet, these predicates are applicable
		if (predSet & relationSet) == predSet {
			predicates = append(predicates, preds...)
		}
	}

	return predicates
}

// IsConnected checks if two relation sets can be joined
// Two sets can be joined if there exists a join predicate connecting them
func (jg *JoinGraph) IsConnected(set1, set2 uint64) bool {
	// Check if there's a predicate that connects the two sets
	combinedSet := set1 | set2

	for predSet := range jg.PredicateMap {
		// Check if this predicate connects set1 and set2
		if (predSet&combinedSet) == predSet &&
			(predSet&set1) != 0 &&
			(predSet&set2) != 0 {
			return true
		}
	}

	return false
}

// GetRelationCount returns the number of relations in the graph
func (jg *JoinGraph) GetRelationCount() int {
	return len(jg.Relations)
}

// GetRelationIDs returns all relation IDs as a slice
func (jg *JoinGraph) GetRelationIDs() []int {
	ids := make([]int, 0, len(jg.Relations))
	for id := range jg.Relations {
		ids = append(ids, id)
	}
	return ids
}

// GetRelationSet returns the bitmask for a set of relation IDs
func (jg *JoinGraph) GetRelationSet(relationIDs []int) uint64 {
	var set uint64
	for _, id := range relationIDs {
		if rel, ok := jg.Relations[id]; ok {
			set |= rel.BitMask
		}
	}
	return set
}

// GetRelationsFromSet returns relation IDs from a bitmask
func (jg *JoinGraph) GetRelationsFromSet(relationSet uint64) []int {
	ids := make([]int, 0)
	for id, rel := range jg.Relations {
		if (relationSet & rel.BitMask) != 0 {
			ids = append(ids, id)
		}
	}
	return ids
}

// CountRelations counts the number of relations in a bitmask
func CountRelations(relationSet uint64) int {
	count := 0
	for relationSet != 0 {
		count++
		relationSet &= relationSet - 1 // Clear the lowest set bit
	}
	return count
}

// findEdge finds an edge with the given relation set
func (jg *JoinGraph) findEdge(relationSet uint64) *JoinEdge {
	for _, edge := range jg.Edges {
		if edge.RelationSet == relationSet {
			return edge
		}
	}
	return nil
}

// String returns a string representation of the join graph
func (jg *JoinGraph) String() string {
	result := fmt.Sprintf("JoinGraph with %d relations:\n", len(jg.Relations))
	for id, rel := range jg.Relations {
		result += fmt.Sprintf("  R%d: %s (bitmask: %b)\n", id, rel.TableName, rel.BitMask)
		for _, pred := range rel.Predicates {
			result += fmt.Sprintf("    Filter: %s %s ?\n", pred.Column, pred.Operator)
		}
	}
	result += fmt.Sprintf("Join predicates (%d):\n", len(jg.PredicateMap))
	for set, preds := range jg.PredicateMap {
		for _, pred := range preds {
			result += fmt.Sprintf("  R%d.%s %s R%d.%s (set: %b)\n",
				pred.LeftRelation, pred.LeftColumn, pred.Operator,
				pred.RightRelation, pred.RightColumn, set)
		}
	}
	return result
}
