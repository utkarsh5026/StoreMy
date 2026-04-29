# StoreMy

> A relational database engine built from scratch in **Rust**

StoreMy is an educational database engine implementing core DBMS concepts including ACID transactions, query optimization, crash recovery, and concurrent access control—without embedding SQLite/PostgreSQL as the storage engine. The implementation lives in the Cargo workspace under [`db/`](db/). For workflows, CI, and contribution expectations, see [CONTRIBUTING.md](CONTRIBUTING.md).

![Rust](https://img.shields.io/badge/rust-1.88-orange?logo=rust)
![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Docker](https://img.shields.io/badge/docker-ready-blue?style=flat&logo=docker)

## 🚀 Quick Start with Docker (Recommended for Testing)

**Want to test StoreMy in 2 minutes?** Just run:

```bash
make quickstart
```

Or using Docker Compose directly:

```bash
docker compose up storemy
```

This builds the Rust image and starts the interactive REPL (see [db/src/main.rs](db/src/main.rs) for CLI modes). Data persists under the `storemy-data` volume at `/app/data` inside the container (`DATA_DIR`).

### Other Quick Start Options

```bash
# Run Rust integration tests in a container
make docker-test

# Benchmark profile (Criterion; copies reports under ./benchmark-results)
docker compose --profile benchmark up storemy-benchmark
```

## Key Features

### Storage Engine

- **Slotted Page Architecture** with 4KB pages for efficient variable-length tuple storage
- **B+Tree Indexes** with automatic rebalancing, split/merge operations, and range scan support
- **Hash Indexes** with separate chaining and overflow page management for fast equality lookups
- **Buffer Pool Manager** implementing LRU eviction policy with NO-STEAL/FORCE buffer management
- **Heap File Storage** with page-level locking and dirty page tracking

### Transaction Processing

- **ACID Guarantees** with full transaction support (begin, commit, abort)
- **Two-Phase Locking (2PL)** with page-level granularity for concurrency control
- **Deadlock Detection** using dependency graph analysis with automatic abort and retry
- **Write-Ahead Logging (WAL)** for durability and crash recovery
- **Before-Image Tracking** for transaction rollback support

### Query Execution

- **SQL Parser** supporting SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE
- **Cost-Based Query Optimization** with join algorithm selection and cardinality estimation
- **Multiple Join Algorithms**:
  - Block Nested Loop Join for general predicates
  - Hash Join for equi-joins with memory-efficient hash tables
  - Sort-Merge Join for pre-sorted inputs
- **Aggregate Functions**: COUNT, SUM, AVG, MIN, MAX with GROUP BY support
- **Iterator-Based Execution Model** for memory-efficient query processing

### System Catalog

- **Self-Describing Metadata** stored in system tables (CATALOG_TABLES, CATALOG_COLUMNS, CATALOG_STATISTICS)
- **Auto-Increment Columns** with persistent counter management
- **Table Statistics** for query optimization including cardinality, page counts, and distinct value estimates
- **Background Statistics Updater** maintaining fresh optimizer statistics

### User Interface

- **Terminal REPL** built with `ratatui` / `rustyline` (interactive SQL shell and one-shot mode)
- **Query history** persisted under the data directory

## Architecture

### Component Structure

```
StoreMy/
├── db/                     # `storemy` crate — storage, buffer pool, WAL, parser, execution, …
│   ├── src/
│   ├── benches/            # Criterion benchmarks
│   └── tests/integration*  # End-to-end tests against the public `Database` API
├── storemy-codec-derive/   # Proc-macros for on-disk codecs
├── Dockerfile              # Release multi-stage image (`storemy` + `metrics_exporter` binaries)
└── docker-compose.yml      # REPL, tests, benchmarks, optional monitoring stack
```

### Query Processing Pipeline

```
SQL Query
    ↓
Lexer (Tokenization)
    ↓
Parser (AST Generation)
    ↓
Query Planner (Logical Plan)
    ↓
Optimizer (Physical Plan + Join Order)
    ↓
Execution Engine (Operator Tree)
    ↓
Iterator Model (Tuple-at-a-time)
    ↓
Results
```

### Concurrency Control Architecture

- **Page-Level Locking**: Shared locks for reads, exclusive locks for writes
- **Lock Upgrade Protocol**: Automatic promotion from shared to exclusive when needed
- **Deadlock Detection**: Maintains wait-for graph with cycle detection
- **Lock Manager**: Coordinates lock acquisition, waiting queues, and grant decisions
- **Transaction Isolation**: Serializable isolation via strict two-phase locking

### Recovery Architecture

- **Write-Ahead Logging Protocol**: Log record forced to disk before page modification
- **Force-at-Commit**: COMMIT record synced before transaction acknowledgment
- **LSN Chaining**: Each log record links to previous for efficient traversal
- **Undo on Abort**: Restores before-images for all dirty pages
- **Log Record Types**: BEGIN, COMMIT, ABORT, INSERT, DELETE, UPDATE

## Getting Started

### Prerequisites

- Rust **1.88** (see [rust-toolchain.toml](rust-toolchain.toml); `rustup` will pick it up automatically)
- A terminal with UTF-8 support

### Installation

```bash
git clone https://github.com/utkarsh-priyadarshi/storemy.git
cd storemy

# Debug build (workspace root)
cargo build -p storemy

# Release binary
cargo build -p storemy --release
```

### Quick Start

```bash
# Interactive REPL (default data dir ./data unless DATA_DIR is set)
cargo run -p storemy -- repl

# One-shot SQL
cargo run -p storemy -- "SELECT 1;"
```

### Environment

| Variable    | Description                          | Default   |
| ----------- | ------------------------------------ | --------- |
| `DATA_DIR`  | Directory for WAL, catalog, history  | `./data`  |
| `RUST_LOG`  | `tracing` filter (see observability) | see crate |

## Usage Examples

### SQL Operations

```sql
-- Create a table
CREATE TABLE employees (
    id INT,
    name VARCHAR,
    department VARCHAR,
    salary FLOAT,
    hire_date VARCHAR
)

-- Insert data
INSERT INTO employees (id, name, department, salary, hire_date)
VALUES (1, 'Alice Johnson', 'Engineering', 95000.00, '2023-01-15')

-- Query with filtering
SELECT name, salary
FROM employees
WHERE salary > 80000

-- Join multiple tables
SELECT e.name, d.department_name, e.salary
FROM employees e
JOIN departments d ON e.department = d.id
WHERE e.salary > 70000

-- Aggregation with grouping
SELECT department, COUNT(*), AVG(salary)
FROM employees
GROUP BY department

-- Update records
UPDATE employees
SET salary = 100000.00
WHERE id = 1

-- Delete records
DELETE FROM employees
WHERE hire_date < '2020-01-01'

-- Drop table
DROP TABLE employees
```

### REPL

Run `cargo run -p storemy -- repl` and type `help` for the current key bindings and commands.

## Technical Highlights

### Buffer Pool Management

The PageStore implements sophisticated buffer management:

- **Maximum Capacity**: 1000 pages (configurable)
- **LRU Eviction**: Least Recently Used policy for clean pages
- **NO-STEAL Policy**: Dirty pages never evicted before commit
- **FORCE Policy**: All dirty pages flushed at commit time
- **Lock Integration**: Coordinates with LockManager for page-level 2PL

### Join Optimization

Cost-based optimizer selects optimal join algorithm:

| Algorithm                   | Best For                                 | Time Complexity                      | Space Complexity |
| --------------------------- | ---------------------------------------- | ------------------------------------ | ---------------- |
| **Block Nested Loop** | Non-equality predicates, small relations | O(\|R\| + (\|R\|/B) × \|S\|)        | O(B)             |
| **Hash Join**         | Equality predicates, sufficient memory   | O(\|R\| + \|S\|) avg                 | O(\|S\|)         |
| **Sort-Merge**        | Pre-sorted inputs, large relations       | O(\|R\| log \|R\| + \|S\| log \|S\|) | O(1) merge       |

Selection criteria based on:

- Predicate type (equality vs. range)
- Table cardinality from catalog statistics
- Available memory for hash tables
- Presence of indexes

### Index Structures

**B+Tree Implementation**:

- Internal nodes store separator keys and child pointers
- Leaf nodes contain (key, RID) pairs with sibling pointers for range scans
- Automatic split/merge operations maintain balance
- O(log n) point queries, O(log n + k) range queries

**Hash Index Implementation**:

- Fixed bucket count determined at creation
- FNV-1a hash function for key distribution
- Separate chaining with overflow pages
- O(1) average case lookups
- Cycle detection prevents infinite overflow chains

## Testing

```bash
# Full workspace (library + integration test crate)
cargo test --workspace

# Integration tests only (`db/tests/integration.rs`)
cargo test -p storemy --test integration

# CI-style (install cargo-nextest first)
cargo nextest run --workspace
```

### Test layout

- **Unit tests** live next to modules under `db/src/`.
- **Integration tests** under `db/tests/` exercise the public `Database` API end-to-end.

## Performance Characteristics

### Storage Layer

- **Page Size**: 4KB (standard database page size)
- **Buffer Pool**: 1000 pages = 4MB default cache
- **Index Lookup**: O(log n) for B+Tree, O(1) average for Hash

### Concurrency

- **Lock Granularity**: Page-level (fine-grained)
- **Deadlock Detection**: Immediate with automatic retry
- **Max Retry Attempts**: 100 with exponential backoff (1ms → 50ms)

### Query Execution

- **Iterator Model**: Memory-efficient tuple-at-a-time processing
- **Join Block Size**: 100 tuples (configurable)
- **Hash Table**: O(1) build and probe for equi-joins

## Project scope

The codebase is organized as a single primary crate (`storemy`) with focused submodules for storage, concurrency, parsing, execution, and recovery—see `db/src/` and [RUST_PORT_ROADMAP.md](RUST_PORT_ROADMAP.md).

## Design Philosophy

StoreMy demonstrates several key principles:

1. **Separation of Concerns**: Clear boundaries between storage, execution, and concurrency layers
2. **Iterator Pattern**: Unified interface for all query operators enabling composition
3. **Strategy Pattern**: Pluggable join algorithms selected by cost-based optimizer
4. **ACID Compliance**: Proper transaction semantics with 2PL and WAL
5. **Production Patterns**: Professional error handling, logging, and resource management

## Limitations & Future Enhancements

### Current Limitations

- Full crash recovery (redo phase) not yet implemented
- No multi-column indexes or composite keys
- Limited query optimizer (no join reordering)
- Single isolation level (serializable via strict 2PL)
- No support for subqueries, views, or stored procedures

### Potential Enhancements

- [ ] MVCC (Multi-Version Concurrency Control) for higher concurrency
- [ ] Query optimizer improvements (join reordering, cost models)
- [ ] Additional aggregate functions (MEDIAN, STDDEV)
- [ ] Full ARIES recovery protocol implementation
- [ ] Network protocol for client-server architecture
- [ ] Composite indexes and covering indexes
- [ ] Query result caching
- [ ] Parallel query execution
- [ ] Compression for pages and log records

## Contributing

This is an educational project demonstrating database internals. Contributions, suggestions, and discussions are welcome! Please feel free to:

- Open issues for bugs or feature requests
- Submit pull requests with improvements
- Share feedback on architecture decisions
- Use as reference for learning database systems

## License

This project is available under the MIT License. See LICENSE file for details.

## Acknowledgments

Built with inspiration from:

- Database System Concepts (Silberschatz, Korth, Sudarshan)
- Database Management Systems (Ramakrishnan, Gehrke)
- CMU 15-445/645 Database Systems course materials
- Modern database architectures (PostgreSQL, SQLite, MySQL)

---

**Built with passion for systems programming and database internals**
