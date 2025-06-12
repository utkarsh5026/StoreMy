"""
INDEX STORAGE ARCHITECTURE FOR SIMPLEDB-STYLE DATABASE

Key Design Principles:
1. **Page-Based**: Indexes use same page structure as heap files
2. **Separate Files**: Each index gets its own .idx file
3. **Buffer Pool Integration**: Indexes use same caching/locking
4. **Type-Specific Pages**: Different page types for different index structures
5. **Metadata in Catalog**: Index information stored in catalog

DIRECTORY STRUCTURE:
data/
├── students.dat # Heap file (table data)
├── students_name.idx      # B+ tree index on name field
├── students_age.idx       # Hash index on age field
└── courses_title.idx      # Another index file

FILE ORGANIZATION:
┌─────────────────────────────────────────────────────────┐
│                    HEAP FILE                            │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐        │
│  │ Page 0  │ │ Page 1  │ │ Page 2  │ │ Page 3  │ ...    │
│  │(Header) │ │(Data)   │ │(Data)   │ │(Data)   │        │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘        │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                   INDEX FILE                            │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐        │
│  │ Page 0  │ │ Page 1  │ │ Page 2  │ │ Page 3  │ ...    │
│  │(Meta)   │ │(Internal│ │(Leaf)   │ │(Leaf)   │        │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘        │
└─────────────────────────────────────────────────────────┘

PAGE TYPE HIERARCHY:
┌─────────────┐
│    Page     │ (Abstract base)
└─────────────┘
       │
    ┌──┴────────────────┐
    │                   │
┌─────────┐     ┌─────────────┐
│HeapPage │     │ IndexPage   │ (Abstract)
└─────────┘     └─────────────┘
                       │
              ┌────────┴────────┐
              │                 │
      ┌─────────────┐   ┌─────────────┐
      │BTreePage    │   │ HashPage    │
      └─────────────┘   └─────────────┘
              │
      ┌───────┴────────┐
      │                │
┌──────────┐  ┌──────────┐
│BTreeLeaf │  │BTreeInter│
└──────────┘  └──────────┘

INDEX ENTRY STRUCTURE:
- B+ Tree Leaf: [Key, RecordId] pairs
- B+ Tree Internal: [Key, ChildPageId] pairs
- Hash Bucket: [Key, RecordId] pairs with overflow chains

INTEGRATION POINTS:
1. BufferPool: Manages both heap and index pages
2. LockManager: Locks index pages during updates
3. Catalog: Stores index metadata
4. Query Optimizer: Uses indexes for efficient access
"""