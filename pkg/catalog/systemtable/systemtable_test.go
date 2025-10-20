package systemtable

import (
	"storemy/pkg/catalog/schema"
	"storemy/pkg/storage/index"
	"storemy/pkg/types"
	"testing"
	"time"
)

// TestTablesTable_RoundTrip tests CreateTuple and Parse for TablesTable
func TestTablesTable_RoundTrip(t *testing.T) {
	tt := &TablesTable{}

	metadata := TableMetadata{
		TableID:       1,
		TableName:     "users",
		FilePath:      "users.dat",
		PrimaryKeyCol: "id",
	}

	// Create tuple
	tuple := tt.CreateTuple(metadata)
	if tuple == nil {
		t.Fatal("CreateTuple returned nil")
	}

	// Parse it back
	parsed, err := tt.Parse(tuple)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Verify fields
	if parsed.TableID != metadata.TableID {
		t.Errorf("TableID mismatch: expected %d, got %d", metadata.TableID, parsed.TableID)
	}
	if parsed.TableName != metadata.TableName {
		t.Errorf("TableName mismatch: expected %s, got %s", metadata.TableName, parsed.TableName)
	}
	if parsed.FilePath != metadata.FilePath {
		t.Errorf("FilePath mismatch: expected %s, got %s", metadata.FilePath, parsed.FilePath)
	}
	if parsed.PrimaryKeyCol != metadata.PrimaryKeyCol {
		t.Errorf("PrimaryKeyCol mismatch: expected %s, got %s", metadata.PrimaryKeyCol, parsed.PrimaryKeyCol)
	}
}

// TestTablesTable_ParseValidation tests validation in Parse method
func TestTablesTable_ParseValidation(t *testing.T) {
	tt := &TablesTable{}

	tests := []struct {
		name      string
		metadata  TableMetadata
		shouldErr bool
		errMsg    string
	}{
		{
			name: "valid metadata",
			metadata: TableMetadata{
				TableID:       1,
				TableName:     "users",
				FilePath:      "users.dat",
				PrimaryKeyCol: "id",
			},
			shouldErr: false,
		},
		{
			name: "invalid table_id",
			metadata: TableMetadata{
				TableID:       InvalidTableID,
				TableName:     "users",
				FilePath:      "users.dat",
				PrimaryKeyCol: "id",
			},
			shouldErr: true,
			errMsg:    "invalid table_id",
		},
		{
			name: "empty table_name",
			metadata: TableMetadata{
				TableID:       1,
				TableName:     "",
				FilePath:      "users.dat",
				PrimaryKeyCol: "id",
			},
			shouldErr: true,
			errMsg:    "table_name cannot be empty",
		},
		{
			name: "empty file_path",
			metadata: TableMetadata{
				TableID:       1,
				TableName:     "users",
				FilePath:      "",
				PrimaryKeyCol: "id",
			},
			shouldErr: true,
			errMsg:    "file_path cannot be empty",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tuple := tt.CreateTuple(tc.metadata)
			_, err := tt.Parse(tuple)

			if tc.shouldErr && err == nil {
				t.Errorf("expected error containing '%s', got nil", tc.errMsg)
			}
			if !tc.shouldErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		})
	}
}

// TestColumnsTable_RoundTrip tests CreateTuple and Parse for ColumnsTable
func TestColumnsTable_RoundTrip(t *testing.T) {
	ct := &ColumnsTable{}

	colMeta := schema.ColumnMetadata{
		TableID:       1,
		Name:          "id",
		FieldType:     types.IntType,
		Position:      0,
		IsPrimary:     true,
		IsAutoInc:     true,
		NextAutoValue: 100,
	}

	// Create tuple
	tuple := ct.CreateTuple(colMeta)
	if tuple == nil {
		t.Fatal("CreateTuple returned nil")
	}

	// Parse it back
	parsed, err := ct.Parse(tuple)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Verify fields
	if parsed.TableID != colMeta.TableID {
		t.Errorf("TableID mismatch: expected %d, got %d", colMeta.TableID, parsed.TableID)
	}
	if parsed.Name != colMeta.Name {
		t.Errorf("Name mismatch: expected %s, got %s", colMeta.Name, parsed.Name)
	}
	if parsed.FieldType != colMeta.FieldType {
		t.Errorf("FieldType mismatch: expected %v, got %v", colMeta.FieldType, parsed.FieldType)
	}
	if parsed.Position != colMeta.Position {
		t.Errorf("Position mismatch: expected %d, got %d", colMeta.Position, parsed.Position)
	}
	if parsed.IsPrimary != colMeta.IsPrimary {
		t.Errorf("IsPrimary mismatch: expected %v, got %v", colMeta.IsPrimary, parsed.IsPrimary)
	}
	if parsed.IsAutoInc != colMeta.IsAutoInc {
		t.Errorf("IsAutoInc mismatch: expected %v, got %v", colMeta.IsAutoInc, parsed.IsAutoInc)
	}
	// Note: NextAutoValue is set to 1 in CreateTuple
	if parsed.NextAutoValue != 1 {
		t.Errorf("NextAutoValue mismatch: expected 1, got %d", parsed.NextAutoValue)
	}
}

// TestIndexesTable_RoundTrip tests CreateTuple and Parse for IndexesTable
func TestIndexesTable_RoundTrip(t *testing.T) {
	it := &IndexesTable{}

	now := time.Now().Unix()
	metadata := IndexMetadata{
		IndexID:    1,
		IndexName:  "idx_users_email",
		TableID:    1,
		ColumnName: "email",
		IndexType:  index.BTreeIndex,
		FilePath:   "idx_users_email.dat",
		CreatedAt:  now,
	}

	// Create tuple
	tuple := it.CreateTuple(metadata)
	if tuple == nil {
		t.Fatal("CreateTuple returned nil")
	}

	// Parse it back
	parsed, err := it.Parse(tuple)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Verify fields
	if parsed.IndexID != metadata.IndexID {
		t.Errorf("IndexID mismatch: expected %d, got %d", metadata.IndexID, parsed.IndexID)
	}
	if parsed.IndexName != metadata.IndexName {
		t.Errorf("IndexName mismatch: expected %s, got %s", metadata.IndexName, parsed.IndexName)
	}
	if parsed.TableID != metadata.TableID {
		t.Errorf("TableID mismatch: expected %d, got %d", metadata.TableID, parsed.TableID)
	}
	if parsed.ColumnName != metadata.ColumnName {
		t.Errorf("ColumnName mismatch: expected %s, got %s", metadata.ColumnName, parsed.ColumnName)
	}
	if parsed.IndexType != metadata.IndexType {
		t.Errorf("IndexType mismatch: expected %v, got %v", metadata.IndexType, parsed.IndexType)
	}
	if parsed.FilePath != metadata.FilePath {
		t.Errorf("FilePath mismatch: expected %s, got %s", metadata.FilePath, parsed.FilePath)
	}
	if parsed.CreatedAt != metadata.CreatedAt {
		t.Errorf("CreatedAt mismatch: expected %d, got %d", metadata.CreatedAt, parsed.CreatedAt)
	}
}

// TestStatsTable_RoundTrip tests CreateTuple and Parse for StatsTable
func TestStatsTable_RoundTrip(t *testing.T) {
	st := &StatsTable{}

	now := time.Now().Truncate(time.Second)
	stats := &TableStatistics{
		TableID:        1,
		Cardinality:    1000,
		PageCount:      50,
		AvgTupleSize:   128,
		LastUpdated:    now,
		DistinctValues: 800,
		NullCount:      10,
		MinValue:       "1",
		MaxValue:       "1000",
	}

	// Create tuple
	tuple := st.CreateTuple(stats)
	if tuple == nil {
		t.Fatal("CreateTuple returned nil")
	}

	// Parse it back
	parsed, err := st.Parse(tuple)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Verify fields
	if parsed.TableID != stats.TableID {
		t.Errorf("TableID mismatch: expected %d, got %d", stats.TableID, parsed.TableID)
	}
	if parsed.Cardinality != stats.Cardinality {
		t.Errorf("Cardinality mismatch: expected %d, got %d", stats.Cardinality, parsed.Cardinality)
	}
	if parsed.PageCount != stats.PageCount {
		t.Errorf("PageCount mismatch: expected %d, got %d", stats.PageCount, parsed.PageCount)
	}
	if parsed.AvgTupleSize != stats.AvgTupleSize {
		t.Errorf("AvgTupleSize mismatch: expected %d, got %d", stats.AvgTupleSize, parsed.AvgTupleSize)
	}
	if !parsed.LastUpdated.Equal(stats.LastUpdated) {
		t.Errorf("LastUpdated mismatch: expected %v, got %v", stats.LastUpdated, parsed.LastUpdated)
	}
	if parsed.DistinctValues != stats.DistinctValues {
		t.Errorf("DistinctValues mismatch: expected %d, got %d", stats.DistinctValues, parsed.DistinctValues)
	}
	if parsed.NullCount != stats.NullCount {
		t.Errorf("NullCount mismatch: expected %d, got %d", stats.NullCount, parsed.NullCount)
	}
	if parsed.MinValue != stats.MinValue {
		t.Errorf("MinValue mismatch: expected %s, got %s", stats.MinValue, parsed.MinValue)
	}
	if parsed.MaxValue != stats.MaxValue {
		t.Errorf("MaxValue mismatch: expected %s, got %s", stats.MaxValue, parsed.MaxValue)
	}
}

// TestColumnStatsTable_RoundTrip tests CreateTuple and Parse for ColumnStatsTable
func TestColumnStatsTable_RoundTrip(t *testing.T) {
	cst := &ColumnStatsTable{}

	now := time.Now().Truncate(time.Second)
	stats := &ColumnStatisticsRow{
		TableID:       1,
		ColumnName:    "email",
		ColumnIndex:   2,
		DistinctCount: 500,
		NullCount:     5,
		MinValue:      "a@example.com",
		MaxValue:      "z@example.com",
		AvgWidth:      32,
		LastUpdated:   now,
	}

	// Create tuple
	tuple := cst.CreateTuple(stats)
	if tuple == nil {
		t.Fatal("CreateTuple returned nil")
	}

	// Parse it back
	parsed, err := cst.Parse(tuple)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Verify fields
	if parsed.TableID != stats.TableID {
		t.Errorf("TableID mismatch: expected %d, got %d", stats.TableID, parsed.TableID)
	}
	if parsed.ColumnName != stats.ColumnName {
		t.Errorf("ColumnName mismatch: expected %s, got %s", stats.ColumnName, parsed.ColumnName)
	}
	if parsed.ColumnIndex != stats.ColumnIndex {
		t.Errorf("ColumnIndex mismatch: expected %d, got %d", stats.ColumnIndex, parsed.ColumnIndex)
	}
	if parsed.DistinctCount != stats.DistinctCount {
		t.Errorf("DistinctCount mismatch: expected %d, got %d", stats.DistinctCount, parsed.DistinctCount)
	}
	if parsed.NullCount != stats.NullCount {
		t.Errorf("NullCount mismatch: expected %d, got %d", stats.NullCount, parsed.NullCount)
	}
	if parsed.MinValue != stats.MinValue {
		t.Errorf("MinValue mismatch: expected %s, got %s", stats.MinValue, parsed.MinValue)
	}
	if parsed.MaxValue != stats.MaxValue {
		t.Errorf("MaxValue mismatch: expected %s, got %s", stats.MaxValue, parsed.MaxValue)
	}
	if parsed.AvgWidth != stats.AvgWidth {
		t.Errorf("AvgWidth mismatch: expected %d, got %d", stats.AvgWidth, parsed.AvgWidth)
	}
	if !parsed.LastUpdated.Equal(stats.LastUpdated) {
		t.Errorf("LastUpdated mismatch: expected %v, got %v", stats.LastUpdated, parsed.LastUpdated)
	}
}

// TestIndexStatsTable_RoundTrip tests CreateTuple and Parse for IndexStatsTable
func TestIndexStatsTable_RoundTrip(t *testing.T) {
	ist := &IndexStatsTable{}

	now := time.Now().Truncate(time.Second)
	stats := &IndexStatisticsRow{
		IndexID:          1,
		TableID:          1,
		IndexName:        "idx_users_email",
		IndexType:        index.BTreeIndex,
		ColumnName:       "email",
		NumEntries:       1000,
		NumPages:         20,
		BTreeHeight:      3,
		DistinctKeys:     800,
		ClusteringFactor: 0.75,
		AvgKeySize:       32,
		LastUpdated:      now,
	}

	// Create tuple
	tuple := ist.CreateTuple(stats)
	if tuple == nil {
		t.Fatal("CreateTuple returned nil")
	}

	// Parse it back
	parsed, err := ist.Parse(tuple)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Verify fields
	if parsed.IndexID != stats.IndexID {
		t.Errorf("IndexID mismatch: expected %d, got %d", stats.IndexID, parsed.IndexID)
	}
	if parsed.TableID != stats.TableID {
		t.Errorf("TableID mismatch: expected %d, got %d", stats.TableID, parsed.TableID)
	}
	if parsed.IndexName != stats.IndexName {
		t.Errorf("IndexName mismatch: expected %s, got %s", stats.IndexName, parsed.IndexName)
	}
	if parsed.IndexType != stats.IndexType {
		t.Errorf("IndexType mismatch: expected %v, got %v", stats.IndexType, parsed.IndexType)
	}
	if parsed.ColumnName != stats.ColumnName {
		t.Errorf("ColumnName mismatch: expected %s, got %s", stats.ColumnName, parsed.ColumnName)
	}
	if parsed.NumEntries != stats.NumEntries {
		t.Errorf("NumEntries mismatch: expected %d, got %d", stats.NumEntries, parsed.NumEntries)
	}
	if parsed.NumPages != stats.NumPages {
		t.Errorf("NumPages mismatch: expected %d, got %d", stats.NumPages, parsed.NumPages)
	}
	if parsed.BTreeHeight != stats.BTreeHeight {
		t.Errorf("BTreeHeight mismatch: expected %d, got %d", stats.BTreeHeight, parsed.BTreeHeight)
	}
	if parsed.DistinctKeys != stats.DistinctKeys {
		t.Errorf("DistinctKeys mismatch: expected %d, got %d", stats.DistinctKeys, parsed.DistinctKeys)
	}
	// Allow small floating point error for clustering factor
	diff := parsed.ClusteringFactor - stats.ClusteringFactor
	if diff < -0.000001 || diff > 0.000001 {
		t.Errorf("ClusteringFactor mismatch: expected %f, got %f", stats.ClusteringFactor, parsed.ClusteringFactor)
	}
	if parsed.AvgKeySize != stats.AvgKeySize {
		t.Errorf("AvgKeySize mismatch: expected %d, got %d", stats.AvgKeySize, parsed.AvgKeySize)
	}
	if !parsed.LastUpdated.Equal(stats.LastUpdated) {
		t.Errorf("LastUpdated mismatch: expected %v, got %v", stats.LastUpdated, parsed.LastUpdated)
	}
}

// TestAllTables_Schema tests that all tables have valid schemas
func TestAllTables_Schema(t *testing.T) {
	tables := []struct {
		name   string
		schema func() int
	}{
		{"TablesTable", func() int { return (&TablesTable{}).GetNumFields() }},
		{"ColumnsTable", func() int { return 7 }},
		{"IndexesTable", func() int { return (&IndexesTable{}).GetNumFields() }},
		{"StatsTable", func() int { return 9 }},
		{"ColumnStatsTable", func() int { return 9 }},
		{"IndexStatsTable", func() int { return 12 }},
	}

	for _, table := range tables {
		t.Run(table.name, func(t *testing.T) {
			numFields := table.schema()
			if numFields <= 0 {
				t.Errorf("%s has invalid number of fields: %d", table.name, numFields)
			}
		})
	}
}
