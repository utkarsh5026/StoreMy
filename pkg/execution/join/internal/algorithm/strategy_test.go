package algorithm

import (
	"storemy/pkg/execution/join/internal/common"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// mockJoinAlgorithm is a test helper that implements common.JoinAlgorithm
type mockJoinAlgorithm struct {
	supportsType bool
	cost         float64
}

func (m *mockJoinAlgorithm) Initialize() error {
	return nil
}

func (m *mockJoinAlgorithm) Next() (*tuple.Tuple, error) {
	return nil, nil
}

func (m *mockJoinAlgorithm) Reset() error {
	return nil
}

func (m *mockJoinAlgorithm) Close() error {
	return nil
}

func (m *mockJoinAlgorithm) EstimateCost() float64 {
	return m.cost
}

func (m *mockJoinAlgorithm) SupportsPredicateType(pred common.JoinPredicate) bool {
	return m.supportsType
}

func TestNewJoinStrategy(t *testing.T) {
	tests := []struct {
		name  string
		stats *common.JoinStatistics
	}{
		{
			name:  "with nil stats",
			stats: nil,
		},
		{
			name: "with provided stats",
			stats: &common.JoinStatistics{
				LeftCardinality:  500,
				RightCardinality: 1500,
				LeftSize:         5,
				RightSize:        15,
				MemorySize:       200,
				Selectivity:      0.05,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
			left := newMockIterator([]*tuple.Tuple{}, tupleDesc)
			right := newMockIterator([]*tuple.Tuple{}, tupleDesc)
			pred, _ := common.NewJoinPredicate(0, 0, primitives.Equals)

			strategy := NewJoinStrategy(left, right, pred, tt.stats)

			if strategy == nil {
				t.Fatal("expected non-nil strategy")
			}

			if strategy.stats == nil {
				t.Fatal("expected non-nil stats")
			}

			if tt.stats == nil {
				// Check default stats
				if strategy.stats.LeftCardinality != 1000 {
					t.Errorf("expected default LeftCardinality=1000, got %d", strategy.stats.LeftCardinality)
				}
				if strategy.stats.RightCardinality != 1000 {
					t.Errorf("expected default RightCardinality=1000, got %d", strategy.stats.RightCardinality)
				}
				if strategy.stats.Selectivity != 0.1 {
					t.Errorf("expected default Selectivity=0.1, got %f", strategy.stats.Selectivity)
				}
			} else {
				// Check provided stats
				if strategy.stats.LeftCardinality != tt.stats.LeftCardinality {
					t.Errorf("expected LeftCardinality=%d, got %d", tt.stats.LeftCardinality, strategy.stats.LeftCardinality)
				}
				if strategy.stats.Selectivity != tt.stats.Selectivity {
					t.Errorf("expected Selectivity=%f, got %f", tt.stats.Selectivity, strategy.stats.Selectivity)
				}
			}

			// Verify algorithms are initialized
			if len(strategy.algorithms) != 3 {
				t.Errorf("expected 3 algorithms, got %d", len(strategy.algorithms))
			}
		})
	}
}

func TestSelectBestAlgorithm(t *testing.T) {
	tests := []struct {
		name          string
		algorithms    []common.JoinAlgorithm
		expectedError bool
		expectedCost  float64
	}{
		{
			name: "selects algorithm with lowest cost",
			algorithms: []common.JoinAlgorithm{
				&mockJoinAlgorithm{supportsType: true, cost: 100},
				&mockJoinAlgorithm{supportsType: true, cost: 50},
				&mockJoinAlgorithm{supportsType: true, cost: 200},
			},
			expectedError: false,
			expectedCost:  50,
		},
		{
			name: "skips algorithms that don't support predicate",
			algorithms: []common.JoinAlgorithm{
				&mockJoinAlgorithm{supportsType: false, cost: 10},
				&mockJoinAlgorithm{supportsType: true, cost: 100},
				&mockJoinAlgorithm{supportsType: false, cost: 5},
			},
			expectedError: false,
			expectedCost:  100,
		},
		{
			name: "returns error when no algorithm supports predicate",
			algorithms: []common.JoinAlgorithm{
				&mockJoinAlgorithm{supportsType: false, cost: 10},
				&mockJoinAlgorithm{supportsType: false, cost: 20},
			},
			expectedError: true,
		},
		{
			name:          "returns error with empty algorithms",
			algorithms:    []common.JoinAlgorithm{},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := &JoinStrategy{
				algorithms: tt.algorithms,
				stats:      &common.JoinStatistics{},
			}

			pred, _ := common.NewJoinPredicate(0, 0, primitives.Equals)

			alg, err := strategy.SelectBestAlgorithm(pred)

			if tt.expectedError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if alg != nil {
					t.Error("expected nil algorithm when error occurs")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if alg == nil {
					t.Fatal("expected non-nil algorithm")
				}
				actualCost := alg.EstimateCost()
				if actualCost != tt.expectedCost {
					t.Errorf("expected cost=%f, got %f", tt.expectedCost, actualCost)
				}
			}
		})
	}
}

func TestGetStatistics(t *testing.T) {
	tests := []struct {
		name              string
		leftCount         int
		rightCount        int
		expectedLeftCard  int
		expectedRightCard int
		expectedLeftSize  int
		expectedRightSize int
	}{
		{
			name:              "counts tuples correctly",
			leftCount:         3,
			rightCount:        5,
			expectedLeftCard:  3,
			expectedRightCard: 5,
			expectedLeftSize:  1, // (3 + 99) / 100 = 1
			expectedRightSize: 1, // (5 + 99) / 100 = 1
		},
		{
			name:              "handles empty iterators",
			leftCount:         0,
			rightCount:        0,
			expectedLeftCard:  0,
			expectedRightCard: 0,
			expectedLeftSize:  0,
			expectedRightSize: 0,
		},
		{
			name:              "handles large tuple counts",
			leftCount:         250,
			rightCount:        150,
			expectedLeftCard:  250,
			expectedRightCard: 150,
			expectedLeftSize:  3, // (250 + 99) / 100 = 3
			expectedRightSize: 2, // (150 + 99) / 100 = 2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})

			// Create tuples
			leftTuples := make([]*tuple.Tuple, tt.leftCount)
			for i := 0; i < tt.leftCount; i++ {
				leftTuples[i] = createJoinTestTuple(tupleDesc, []interface{}{int32(i)})
			}

			rightTuples := make([]*tuple.Tuple, tt.rightCount)
			for i := 0; i < tt.rightCount; i++ {
				rightTuples[i] = createJoinTestTuple(tupleDesc, []interface{}{int32(i)})
			}

			left := newMockIterator(leftTuples, tupleDesc)
			right := newMockIterator(rightTuples, tupleDesc)

			// Open iterators (required by mockIterator)
			if err := left.Open(); err != nil {
				t.Fatalf("failed to open left iterator: %v", err)
			}
			if err := right.Open(); err != nil {
				t.Fatalf("failed to open right iterator: %v", err)
			}

			stats, err := GetStatistics(left, right)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if stats == nil {
				t.Fatal("expected non-nil stats")
			}

			if stats.LeftCardinality != tt.expectedLeftCard {
				t.Errorf("expected LeftCardinality=%d, got %d", tt.expectedLeftCard, stats.LeftCardinality)
			}

			if stats.RightCardinality != tt.expectedRightCard {
				t.Errorf("expected RightCardinality=%d, got %d", tt.expectedRightCard, stats.RightCardinality)
			}

			if stats.LeftSize != tt.expectedLeftSize {
				t.Errorf("expected LeftSize=%d, got %d", tt.expectedLeftSize, stats.LeftSize)
			}

			if stats.RightSize != tt.expectedRightSize {
				t.Errorf("expected RightSize=%d, got %d", tt.expectedRightSize, stats.RightSize)
			}

			// Verify default values
			if stats.MemorySize != 100 {
				t.Errorf("expected default MemorySize=100, got %d", stats.MemorySize)
			}

			if stats.Selectivity != 0.1 {
				t.Errorf("expected default Selectivity=0.1, got %f", stats.Selectivity)
			}
		})
	}
}

func TestGetStatistics_ErrorHandling(t *testing.T) {
	t.Run("error on left iterator", func(t *testing.T) {
		tupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
		left := newMockIterator([]*tuple.Tuple{}, tupleDesc)
		right := newMockIterator([]*tuple.Tuple{}, tupleDesc)

		// Set error flag
		left.hasError = true

		// Open iterators
		if err := left.Open(); err != nil {
			// Open will fail, which is expected
		}
		right.Open()

		stats, err := GetStatistics(left, right)

		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if stats != nil {
			t.Error("expected nil stats when error occurs")
		}
	})

	t.Run("error on right iterator", func(t *testing.T) {
		tupleDesc := createTestTupleDesc([]types.Type{types.IntType}, []string{"id"})
		left := newMockIterator([]*tuple.Tuple{}, tupleDesc)
		right := newMockIterator([]*tuple.Tuple{}, tupleDesc)

		// Set error flag
		right.hasError = true

		// Open iterators
		left.Open()
		if err := right.Open(); err != nil {
			// Open will fail, which is expected
		}

		stats, err := GetStatistics(left, right)

		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if stats != nil {
			t.Error("expected nil stats when error occurs")
		}
	})
}
