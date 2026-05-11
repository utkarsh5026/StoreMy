//! Binding for schema-changing SQL (`CREATE` / `DROP` / `SHOW` on tables and indexes).
//!
//! Sits at the SQL → bound-plan seam for DDL: resolve object names against the catalog inside a
//! [`Transaction`](crate::transaction::Transaction), enforce static rules (duplicate columns,
//! unknown PK columns, index ownership), and hand the executor compact
//! [`Bound*`](crate::binder::Bound) values so it does not repeat lookups.
//!
//! ## Shape
//!
//! - [`BoundDrop`] — `DROP TABLE` / `DROP TABLE IF EXISTS`
//! - [`BoundCreateTable`] — `CREATE TABLE` / `CREATE TABLE IF NOT EXISTS`
//! - [`BoundCreateIndex`] — `CREATE INDEX` / `CREATE INDEX IF NOT EXISTS`
//! - [`BoundDropIndex`] — `DROP INDEX` / `DROP INDEX IF EXISTS`
//! - [`BoundShowIndexes`] — `SHOW INDEXES` / `SHOW INDEXES FROM <table>`
//!
//! ## How it works
//!
//! Single-phase binding (no iterator phases like a physical operator):
//!
//! - **Tables** — validate the column list, build a [`TupleSchema`] in declaration order, resolve
//!   primary key column names to [`ColumnId`](crate::primitives::ColumnId)s (or surface
//!   `AlreadyExists` under `IF NOT EXISTS`).
//! - **Indexes** — resolve the base table and each indexed column name to schema positions; reject
//!   duplicate names in the index column list; check index name collisions against the catalog.
//! - **Drop index** — resolve the index by name, then confirm it belongs to the named table (or
//!   treat as no-op / error per `IF EXISTS`).
//! - **Show indexes** — either list all indexes, or resolve `FROM <table>` to a
//!   [`FileId`](crate::FileId) for filtered listing.
//!
//! ## NULL semantics
//!
//! Does not evaluate scalar expressions. `NULL` only appears indirectly: each
//! [`ColumnDef`](crate::parser::statements::ColumnDef) contributes nullability to the built
//! [`TupleSchema`]; binding does not insert default values or run `CHECK` constraints.

use crate::{
    FileId, Value,
    binder::{BindError, check_table, require_column, resolve_column_ids},
    catalog::{CatalogError, TableInfo, manager::Catalog, systable::FkAction},
    index::IndexKind,
    parser::statements::{
        AlterAction, AlterTableStatement, ColumnDef, CreateIndexStatement, CreateTableStatement,
        DropIndexStatement, DropStatement, ReferentialAction, ShowIndexesStatement,
        TableConstraint, Uniqueness,
    },
    primitives::{ColumnId, NonEmptyString},
    transaction::Transaction,
    tuple::TupleSchema,
};

/// Bound outcome of `DROP TABLE` / `DROP TABLE IF EXISTS`.
///
/// Either resolves the table to a concrete [`FileId`] for the executor to remove, or records a
/// successful no-op when `IF EXISTS` is set and the name is unknown.
///
/// # SQL examples
///
/// ```sql
/// -- DROP TABLE users;
/// --
/// --   BoundDrop::Drop { name: "users", file_id: <heap FileId> }
/// ```
///
/// ```sql
/// -- DROP TABLE IF EXISTS ghost;
/// --
/// --   When `ghost` is missing:
/// --   BoundDrop::NoOp { name: "ghost" }
/// ```
pub enum BoundDrop {
    Drop { name: String, file_id: FileId },
    NoOp { name: String },
}

impl BoundDrop {
    /// Binds a parsed [`DropStatement`] against the
    /// catalog.
    ///
    /// # Errors
    ///
    /// - [`BindError::UnknownTable`] — table name not in the catalog and `IF EXISTS` is false.
    /// - [`BindError::Catalog`] — other catalog read failures while resolving the table.
    pub(in crate::binder) fn bind(
        stmt: &DropStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        match check_table(catalog, txn, &stmt.table_name, stmt.if_exists)? {
            Some(TableInfo { name, file_id, .. }) => Ok(Self::Drop {
                name: name.as_str().to_string(),
                file_id,
            }),
            None => Ok(Self::NoOp {
                name: stmt.table_name.as_str().to_string(),
            }),
        }
    }
}

/// Bound outcome of `CREATE TABLE` / `CREATE TABLE IF NOT EXISTS`.
///
/// On success either describes a **new** table (`schema` + optional composite primary key as
/// resolved [`ColumnId`]s), or reports **already exists** under
/// `IF NOT EXISTS` so the executor can skip allocation.
///
/// # SQL examples
///
/// Fixed reference schema for the snippets below: `users(id, name, age)` with binder-style indices
/// `id → 0`, `name → 1`, `age → 2` after [`TupleSchema`] is built in column list order.
///
/// ```sql
/// -- CREATE TABLE users (
/// --   id INT64 NOT NULL PRIMARY KEY,
/// --   name STRING,
/// --   age INT64
/// -- );
/// --
/// --   BoundCreateTable::New {
/// --       name: "users",
/// --       schema: TupleSchema { fields in order id, name, age },
/// --       primary_key: Some(vec![ColumnId(0)]),
/// --   }
/// ```
///
/// ```sql
/// -- CREATE TABLE IF NOT EXISTS users (...);
/// --
/// --   When `users` already exists:
/// --   BoundCreateTable::AlreadyExists { name: "users", file_id: <heap FileId> }
/// ```
pub enum BoundCreateTable {
    New {
        name: String,
        schema: TupleSchema,
        primary_key: Option<Vec<ColumnId>>,
        constraints: Vec<BoundTableConstraint>,
    },
    AlreadyExists {
        name: String,
        file_id: FileId,
    },
}

impl BoundCreateTable {
    /// Binds a parsed [`CreateTableStatement`].
    ///
    /// # Errors
    ///
    /// - [`BindError::TableAlreadyExists`] — name already taken and `IF NOT EXISTS` is false.
    /// - [`BindError::DuplicateColumn`] — duplicate column name in the `CREATE TABLE` column list.
    /// - [`BindError::PrimaryKeyNotInColumns`] — `PRIMARY KEY (...)` names a column not present in
    ///   the table definition.
    /// - [`BindError::Catalog`] — catalog read failure on the `IF NOT EXISTS` path when the table
    ///   already exists.
    pub(in crate::binder) fn bind(
        stmt: &CreateTableStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        let table_name = stmt.table_name.as_str();
        if catalog.table_exists(table_name) {
            if !stmt.if_not_exists {
                return Err(BindError::table_already_exists(table_name));
            }

            let table = check_table(catalog, txn, table_name, true)?
                .expect("if_exists=false should never yield None");
            return Ok(Self::AlreadyExists {
                name: table.name.as_str().to_string(),
                file_id: table.file_id,
            });
        }

        let schema = TupleSchema::from(stmt.columns.iter().collect::<Vec<&ColumnDef>>());
        resolve_column_ids(
            &schema,
            table_name,
            stmt.columns.iter().map(|c| c.name.as_str()),
        )?;
        let primary_key = Self::resolve_primary_key(
            stmt.columns.as_slice(),
            stmt.primary_key.as_slice(),
            &schema,
            table_name,
        )?;
        let mut constraints = Self::resolve_constraints(stmt, &schema, catalog, txn)?;
        Self::append_column_level_unique_constraints(
            table_name,
            &stmt.columns,
            &schema,
            &mut constraints,
        )?;

        Ok(Self::New {
            name: stmt.table_name.as_str().to_string(),
            schema,
            primary_key,
            constraints,
        })
    }

    /// Appends column-level unique constraints to the list of table constraints.
    ///
    /// For each column in `columns` marked as `UNIQUE`, this function checks if a unique constraint
    /// on that column already exists in `constraints`. If not, it creates a `BoundTableConstraint`
    /// with an auto-generated name (`{table}_unique_{col}`) and adds it to
    /// `constraints`.
    ///
    /// If a column marked as `UNIQUE` does not exist in the schema, returns
    /// `BindError::unknown_column`.
    ///
    /// # Arguments
    ///
    /// - `table_name`: The name of the table, used for error reporting and auto-naming constraints.
    /// - `columns`: Slice of all column definitions for the table.
    /// - `schema`: The schema for the table, used for resolving column IDs.
    /// - `constraints`: The mutable vector of constraints to which new unique constraints will be
    ///   appended.
    ///
    /// # Errors
    ///
    /// Returns `BindError::unknown_column` if a column marked as unique is not found in the schema.
    /// Returns `BindError::Catalog` on constraint name validation errors.
    fn append_column_level_unique_constraints(
        table_name: &str,
        columns: &[ColumnDef],
        schema: &TupleSchema,
        constraints: &mut Vec<BoundTableConstraint>,
    ) -> Result<(), BindError> {
        for col in columns {
            if col.unique != Uniqueness::Unique {
                continue;
            }
            let (col_id, _) = require_column(schema, table_name, col.name.as_str())?;
            let sorted = vec![col_id];

            if constraints.iter().any(|c| {
                matches!(
                    &c.body,
                    BoundConstraintBody::Unique { columns } if columns == &sorted
                )
            }) {
                continue;
            }

            constraints.push(BoundTableConstraint {
                name: None,
                body: BoundConstraintBody::Unique { columns: sorted },
            });
        }
        Ok(())
    }

    /// Resolves the primary key for a table during table creation.
    ///
    /// Primary keys can be specified in two ways:
    /// 1. Inline, by marking one or more column definitions as `primary_key`.
    /// 2. At the table-level (often via a `PRIMARY KEY (...)` constraint), passed as `table_pk`.
    ///
    /// If both exist, preference is given to the inline specification.
    ///
    /// Returns a vector of `ColumnId`s for the columns holding the primary key, or `None` if there
    /// is no primary key. Returns `BindError::PrimaryKeyNotInColumns` if a named primary key
    /// column does not exist in the schema.
    fn resolve_primary_key(
        columns: &[ColumnDef],
        table_pk: &[NonEmptyString],
        schema: &TupleSchema,
        _table_name: &str,
    ) -> Result<Option<Vec<ColumnId>>, BindError> {
        let pk_names = {
            let inline_pk_names = columns
                .iter()
                .filter_map(|c| c.primary_key.then_some(c.name.as_str()))
                .collect::<Vec<_>>();

            let table_pk_names = table_pk
                .iter()
                .map(NonEmptyString::as_str)
                .collect::<Vec<_>>();

            if inline_pk_names.is_empty() {
                table_pk_names
            } else {
                inline_pk_names
            }
        };

        let primary_key = if pk_names.is_empty() {
            None
        } else {
            let pk = pk_names
                .into_iter()
                .map(|name| {
                    schema.field_by_name(name).map_or_else(
                        || Err(BindError::PrimaryKeyNotInColumns(name.to_string())),
                        |(id, _)| Ok(id),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Some(pk)
        };

        Ok(primary_key)
    }

    /// Resolves and binds table constraints declared in a `CREATE TABLE` statement.
    ///
    /// Iterates over all named constraints and binds them against the current table schema,
    /// validating correctness and referencing other catalog entries when needed (e.g. for
    /// foreign keys).
    ///
    /// Returns a vector of bound constraints or a `BindError` if any constraint is invalid.
    fn resolve_constraints(
        stmt: &CreateTableStatement,
        schema: &TupleSchema,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Vec<BoundTableConstraint>, BindError> {
        stmt.constraints
            .iter()
            .map(|(name, constraint)| {
                bind_constraint_against_schema(
                    schema,
                    stmt.table_name.as_str(),
                    name.clone(),
                    constraint,
                    catalog,
                    txn,
                )
            })
            .collect()
    }
}

/// Bound outcome of `CREATE INDEX` / `CREATE INDEX IF NOT EXISTS`.
///
/// Resolves the base table and each indexed column **name** to a zero-based
/// [`ColumnId`](crate::primitives::ColumnId) (schema position) in index declaration order. The
/// executor persists names and uses these indices when registering the live index.
///
/// # SQL examples
///
/// Same reference table `users(id, name, age)` with `id → 0`, `name → 1`, `age → 2`.
///
/// ```sql
/// -- CREATE INDEX users_name_idx ON users (name) USING HASH;
/// --
/// --   BoundCreateIndex::New {
/// --       index_name: "users_name_idx",
/// --       table_name: "users",
/// --       table_file_id: <users heap FileId>,
/// --       column_indices: vec![ColumnId(1)],
/// --       kind: IndexKind::Hash,
/// --   }
/// ```
///
/// ```sql
/// -- CREATE INDEX IF NOT EXISTS users_name_idx ON users (name) USING HASH;
/// --
/// --   When that index name is already registered:
/// --   BoundCreateIndex::AlreadyExists { index_name: "users_name_idx" }
/// ```
#[derive(Debug)]
pub enum BoundCreateIndex {
    New {
        index_name: String,
        table_name: String,
        table_file_id: FileId,
        column_indices: Vec<ColumnId>,
        kind: IndexKind,
    },
    AlreadyExists {
        index_name: String,
    },
}

#[derive(Debug, Clone)]
pub enum BoundConstraintBody {
    Unique {
        columns: Vec<ColumnId>,
    },
    Check {
        expr: crate::parser::statements::Expr,
    },
    ForeignKey {
        local_columns: Vec<ColumnId>,
        ref_table_id: FileId,
        ref_columns: Vec<ColumnId>,
        on_delete: Option<FkAction>,
        on_update: Option<FkAction>,
    },
}

#[derive(Debug, Clone)]
pub struct BoundTableConstraint {
    pub name: Option<NonEmptyString>,
    pub body: BoundConstraintBody,
}

impl From<ReferentialAction> for FkAction {
    fn from(action: ReferentialAction) -> Self {
        match action {
            ReferentialAction::Cascade => FkAction::Cascade,
            ReferentialAction::SetNull => FkAction::SetNull,
            ReferentialAction::SetDefault => FkAction::SetDefault,
            ReferentialAction::Restrict => FkAction::Restrict,
        }
    }
}

impl BoundCreateIndex {
    /// Binds a parsed [`CreateIndexStatement`].
    ///
    /// # Errors
    ///
    /// - [`BindError::UnknownTable`] — `ON` table not found.
    /// - [`BindError::UnknownColumn`] — a name in the index column list is not a column of that
    ///   table.
    /// - [`BindError::DuplicateColumn`] — the same column name appears twice in the index column
    ///   list.
    /// - [`BindError::IndexAlreadyExists`] — index name is taken and `IF NOT EXISTS` is false.
    /// - [`BindError::Catalog`] — other catalog failures while loading table metadata or checking
    ///   the index registry.
    pub(in crate::binder) fn bind(
        stmt: CreateIndexStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        let table = check_table(catalog, txn, &stmt.table_name, false)?
            .expect("if_exists=false should never yield None");

        if catalog.get_index_by_name(&stmt.index_name).is_some() {
            if stmt.if_not_exists {
                return Ok(Self::AlreadyExists {
                    index_name: stmt.index_name.into_inner(),
                });
            }
            return Err(BindError::IndexAlreadyExists(stmt.index_name.into_inner()));
        }

        let column_indices = resolve_column_ids(
            &table.schema,
            table.name.as_str(),
            stmt.columns.iter().map(NonEmptyString::as_str),
        )?;
        Ok(Self::New {
            index_name: stmt.index_name.into_inner(),
            table_name: table.name.into_inner(),
            table_file_id: table.file_id,
            column_indices,
            kind: stmt.index_type,
        })
    }
}

/// Bound outcome of `SHOW INDEXES` / `SHOW INDEXES FROM <table>`.
///
/// Without `FROM`, the executor lists every index in the catalog. With `FROM`, resolves the table
/// once to `(name, file_id)` so listing can filter index metadata by `table_id` without a second
/// name lookup.
///
/// # SQL examples
///
/// ```sql
/// -- SHOW INDEXES;
/// --
/// --   BoundShowIndexes::AllTables
/// ```
///
/// ```sql
/// -- SHOW INDEXES FROM users;
/// --
/// --   BoundShowIndexes::OneTable { name: "users", file_id: <heap FileId> }
/// ```
#[derive(Debug)]
pub enum BoundShowIndexes {
    AllTables,
    OneTable { name: String, file_id: FileId },
}

impl BoundShowIndexes {
    /// Binds a parsed [`ShowIndexesStatement`].
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- SHOW INDEXES FROM users
    /// --   BoundShowIndexes::bind(&stmt, catalog, txn)
    /// --     -> Ok(BoundShowIndexes::OneTable { .. })
    /// ```
    ///
    /// # Errors
    ///
    /// - [`BindError::UnknownTable`] — `FROM` names a table that is not in the catalog.
    /// - [`BindError::Catalog`] — other catalog read failures.
    pub(in crate::binder) fn bind(
        stmt: &ShowIndexesStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        match &stmt.0 {
            None => Ok(Self::AllTables),
            Some(name) => {
                let TableInfo { name, file_id, .. } = check_table(catalog, txn, name, false)?
                    .expect("if_exists=false should never yield None");
                Ok(Self::OneTable {
                    name: name.as_str().to_string(),
                    file_id,
                })
            }
        }
    }
}

/// Bound outcome of `DROP INDEX <name> ON <table>` / `… IF EXISTS`.
///
/// After resolving the index by name, binding checks that it belongs to the named table. Either
/// returns a concrete drop for the executor, or a no-op when `IF EXISTS` allows ignoring a missing
/// or mismatched index.
///
/// # SQL examples
///
/// ```sql
/// -- DROP INDEX users_email_idx ON users;
/// --
/// --   BoundDropIndex::Drop { index_name: "users_email_idx" }
/// --   (only when that index exists on `users`)
/// ```
///
/// ```sql
/// -- DROP INDEX IF EXISTS ghost_idx ON users;
/// --
/// --   BoundDropIndex::NoOp { index_name: "ghost_idx" }
/// --   when the index is missing, or exists but not on `users`
/// ```
#[derive(Debug)]
pub enum BoundDropIndex {
    Drop { index_name: String },
    NoOp { index_name: String },
}

impl BoundDropIndex {
    /// Binds a parsed [`DropIndexStatement`].
    ///
    /// # Errors
    ///
    /// - [`BindError::UnknownIndex`] — index name not found, or index exists but not on the given
    ///   table, when `IF EXISTS` is false.
    /// - [`BindError::UnknownTable`] — `ON` table not found when the index exists and must be
    ///   checked for ownership.
    /// - [`BindError::Catalog`] — other catalog failures.
    pub(in crate::binder) fn bind(
        stmt: DropIndexStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        let index_name = stmt.index_name.into_inner();
        if let Some(index) = catalog.get_index_by_name(&index_name) {
            let Some(table_name) = stmt.table_name.as_ref() else {
                return Err(BindError::UnknownIndex(index_name));
            };
            let table = check_table(catalog, txn, table_name.as_str(), false)?
                .expect("if_exists=false should never yield None");

            if catalog.index_belongs_to_table(table.file_id, &index) {
                return Ok(Self::Drop { index_name });
            }

            if stmt.if_exists {
                return Ok(Self::NoOp {
                    index_name: index_name.to_string(),
                });
            }
            return Err(BindError::UnknownIndex(index_name));
        }

        if stmt.if_exists {
            return Ok(Self::NoOp { index_name });
        }
        Err(BindError::UnknownIndex(index_name))
    }
}

/// Bound outcome of `ALTER TABLE [IF EXISTS] <name> <action>`.
///
/// Each variant carries exactly the information the executor needs — no raw
/// string lookups remain.  The outer `IF EXISTS` (missing *table*) collapses
/// the whole statement into [`NoOp`](BoundAlterTable::NoOp).
///
/// # SQL examples
///
/// Reference table: `users(id INT64 NOT NULL, name STRING)` with `id → ColumnId(0)`,
/// `name → ColumnId(1)`.
///
/// ```sql
/// -- ALTER TABLE users ADD COLUMN age INT64;
/// --
/// --   BoundAlterTable::AddColumn {
/// --       table_name: "users", file_id: <FileId>,
/// --       column: ColumnDef { name: "age", col_type: Int64, … },
/// --   }
/// ```
///
/// ```sql
/// -- ALTER TABLE users DROP COLUMN name;
/// --
/// --   BoundAlterTable::DropColumn {
/// --       table_name: "users", file_id: <FileId>,
/// --       column_name: "name", column_id: ColumnId(1),
/// --   }
/// ```
///
/// ```sql
/// -- ALTER TABLE users RENAME COLUMN name TO full_name;
/// --
/// --   BoundAlterTable::RenameColumn {
/// --       table_name: "users", file_id: <FileId>,
/// --       old_name: "name", new_name: "full_name",
/// --   }
/// ```
///
/// ```sql
/// -- ALTER TABLE users RENAME TO accounts;
/// --
/// --   BoundAlterTable::RenameTable {
/// --       old_name: "users", new_name: "accounts", file_id: <FileId>,
/// --   }
/// ```
///
/// ```sql
/// -- ALTER TABLE IF EXISTS ghost ADD COLUMN x INT64;
/// --
/// --   BoundAlterTable::NoOp { table_name: "ghost" }
/// --   when `ghost` is not in the catalog.
/// ```
#[derive(Debug)]
pub enum BoundAlterTable {
    /// The table named in the statement was not found, but `IF EXISTS` was set,
    /// so this is a successful no-op rather than an error.
    NoOp { table_name: String },

    /// A real table mutation. Every executable ALTER action has a resolved table file id.
    Action {
        table_name: String,
        file_id: FileId,
        action: BoundAlterAction,
    },
}

#[derive(Debug)]
pub enum BoundAlterAction {
    /// `ADD COLUMN <col_def>` — append a new column to the table schema.
    ///
    /// The executor should insert a new row into `CATALOG_COLUMNS` and
    /// invalidate the cached [`TableInfo`] so the rebuilt schema includes the
    /// new field.
    AddColumn {
        /// The full column definition from the AST, including type, nullability,
        /// and any default. The executor owns this and writes it to the catalog.
        column: ColumnDef,
    },

    /// `DROP COLUMN [IF EXISTS] <name>` — remove an existing column.
    ///
    /// `column_id` is the zero-based position in the *current* schema. The
    /// executor uses it to delete the matching row from `CATALOG_COLUMNS` and
    /// to rebuild the schema after removal.
    DropColumn {
        column_name: String,
        column_id: ColumnId,
    },

    /// `RENAME COLUMN <old> TO <new>` — rename a column in the catalog.
    ///
    /// Catalog-metadata change only; the heap file is unaffected.
    RenameColumn {
        old_name: String,
        new_name: String,
    },

    /// `RENAME TO <new_name>` — give the table a different name.
    ///
    /// Catalog-metadata change only; the heap file path is unaffected.
    RenameTable {
        old_name: String,
        new_name: String,
    },

    /// `ALTER COLUMN <col> SET DEFAULT <value>` — set a column's default value.
    SetDefault {
        column: String,
        value: Value,
    },

    /// `ALTER COLUMN <col> DROP DEFAULT` — remove a column's default value.
    DropDefault {
        column: String,
    },

    /// `ALTER COLUMN <col> DROP NOT NULL` — relax a NOT NULL constraint to nullable.
    DropNotNull {
        column: String,
    },

    /// `ADD PRIMARY KEY (<cols>)` — set the table's primary key.
    ///
    /// `column_ids` are the resolved zero-based positions in the *current* schema,
    /// in the order the user listed them in the `ADD PRIMARY KEY` clause.
    AddPrimaryKey {
        column_ids: Vec<ColumnId>,
    },

    /// `DROP PRIMARY KEY` — remove the table's primary key.
    DropPrimaryKey,

    AddConstraint {
        constraint: BoundTableConstraint,
    },

    DropConstraint {
        name: NonEmptyString,
        if_exists: bool,
    },
}

impl BoundAlterTable {
    fn action(table_name: &str, file_id: FileId, action: BoundAlterAction) -> Self {
        Self::Action {
            table_name: table_name.to_string(),
            file_id,
            action,
        }
    }

    /// Binds a parsed [`AlterTableStatement`] against the catalog.
    ///
    /// # Errors
    ///
    /// - [`BindError::UnknownTable`] — table not found and `IF EXISTS` is false.
    /// - [`BindError::UnknownColumn`] — `DROP COLUMN` or `RENAME COLUMN` names a column that does
    ///   not exist in the current schema (and `IF EXISTS` is false for `DROP COLUMN`).
    /// - [`BindError::DuplicateColumn`] — `ADD COLUMN` names a column that already exists, or
    ///   `RENAME COLUMN` targets a name already taken.
    /// - [`BindError::TableAlreadyExists`] — `RENAME TO` targets a name already in the catalog.
    /// - [`BindError::Catalog`] — other catalog read failures.
    pub(in crate::binder) fn bind(
        stmt: AlterTableStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<Self, BindError> {
        let table_name = stmt.table_name.into_inner();
        let Some(table_info) = check_table(catalog, txn, &table_name, stmt.if_exists)? else {
            return Ok(Self::NoOp { table_name });
        };

        let TableInfo {
            file_id,
            schema,
            primary_key,
            ..
        } = &table_info;

        let action = match stmt.action {
            AlterAction::DropColumn { name, if_exists } => {
                let Some((column_id, _)) = schema.field_by_name(name.as_str()) else {
                    return if if_exists {
                        Ok(Self::NoOp {
                            table_name: table_name.clone(),
                        })
                    } else {
                        Err(BindError::unknown_column(&table_name, name.into_inner()))
                    };
                };

                BoundAlterAction::DropColumn {
                    column_name: name.into_inner(),
                    column_id,
                }
            }

            AlterAction::AddColumn(col_def) => {
                if schema.field_by_name(&col_def.name).is_some() {
                    return Err(BindError::duplicate_column(col_def.name.as_str()));
                }
                BoundAlterAction::AddColumn { column: col_def }
            }

            AlterAction::RenameColumn { from, to } => {
                require_column(schema, &table_name, from.as_str())?;
                if schema.field_by_name(to.as_str()).is_some() {
                    return Err(BindError::duplicate_column(to));
                }

                BoundAlterAction::RenameColumn {
                    old_name: from.into_inner(),
                    new_name: to.into_inner(),
                }
            }

            AlterAction::RenameTable { to } => {
                if catalog.table_exists(to.as_str()) {
                    return Err(BindError::table_already_exists(to.into_inner()));
                }

                BoundAlterAction::RenameTable {
                    old_name: table_name.to_string(),
                    new_name: to.into_inner(),
                }
            }

            AlterAction::SetDefault { column, value } => {
                require_column(schema, &table_name, column.as_str())?;
                BoundAlterAction::SetDefault {
                    column: column.into_inner(),
                    value,
                }
            }

            AlterAction::DropDefault { column } => {
                require_column(&table_info.schema, &table_info.name, column.as_str())?;
                BoundAlterAction::DropDefault {
                    column: column.to_string(),
                }
            }

            AlterAction::DropNotNull { column } => {
                require_column(&table_info.schema, &table_info.name, column.as_str())?;
                BoundAlterAction::DropNotNull {
                    column: column.to_string(),
                }
            }

            AlterAction::AddPrimaryKey { columns } => {
                if primary_key.is_some() {
                    return Err(BindError::primary_key_already_exists(&table_name));
                }

                let cols_iter = columns.iter().map(NonEmptyString::as_str);
                BoundAlterAction::AddPrimaryKey {
                    column_ids: resolve_column_ids(schema, &table_name, cols_iter)?,
                }
            }

            AlterAction::DropPrimaryKey => BoundAlterAction::DropPrimaryKey,

            AlterAction::DropConstraint { name, if_exists } => {
                BoundAlterAction::DropConstraint { name, if_exists }
            }

            AlterAction::AddConstraint { name, constraint } => BoundAlterAction::AddConstraint {
                constraint: bind_constraint_against_schema(
                    schema,
                    &table_name,
                    name,
                    &constraint,
                    catalog,
                    txn,
                )?,
            },
        };

        Ok(Self::action(&table_name, *file_id, action))
    }
}

/// Resolves a parsed table constraint against the current catalog snapshot.
///
/// `TableConstraint` is still parser-shaped: it contains column and table names
/// exactly as the user wrote them. This helper lowers that syntax into a
/// [`BoundTableConstraint`] by resolving local columns against `schema`, looking
/// up referenced tables for foreign keys, and converting referential actions
/// into catalog-facing [`FkAction`] values.
///
/// This is shared by `CREATE TABLE (..., <constraint>)` and
/// `ALTER TABLE ... ADD CONSTRAINT`, so both statements apply the same static
/// validation before the executor writes anything to the catalog.
///
/// # Errors
///
/// - [`BindError::DuplicateColumn`] when a UNIQUE/FK column list repeats a column.
/// - [`BindError::UnknownColumn`] when a local or referenced column name cannot be resolved.
/// - [`BindError::UnknownTable`] when a foreign key references a missing table.
/// - [`BindError::Unsupported`] when a foreign key has mismatched local and referenced column
///   counts.
/// - [`BindError::Catalog`] for other catalog lookup failures.
fn bind_constraint_against_schema(
    schema: &TupleSchema,
    table_name: &str,
    name: Option<NonEmptyString>,
    constraint: &TableConstraint,
    catalog: &Catalog,
    txn: &Transaction<'_>,
) -> Result<BoundTableConstraint, BindError> {
    let body = match constraint {
        TableConstraint::Unique { columns } => BoundConstraintBody::Unique {
            columns: resolve_column_ids(
                schema,
                table_name,
                columns.iter().map(NonEmptyString::as_str),
            )?,
        },
        TableConstraint::Check { expr } => BoundConstraintBody::Check { expr: expr.clone() },
        TableConstraint::ForeignKey {
            local_cols,
            ref_table,
            ref_cols,
            on_delete,
            on_update,
        } => {
            let local_columns = resolve_column_ids(
                schema,
                table_name,
                local_cols.iter().map(NonEmptyString::as_str),
            )?;
            let ref_table_info = check_table(catalog, txn, ref_table.as_str(), false)?
                .expect("if_exists=false should never yield None");
            let ref_columns = resolve_column_ids(
                &ref_table_info.schema,
                ref_table_info.name.as_str(),
                ref_cols.iter().map(NonEmptyString::as_str),
            )?;

            if local_columns.len() != ref_columns.len() {
                return Err(BindError::Unsupported(format!(
                    "foreign key column count mismatch: {} local columns, {} referenced columns",
                    local_columns.len(),
                    ref_columns.len()
                )));
            }

            BoundConstraintBody::ForeignKey {
                local_columns,
                ref_table_id: ref_table_info.file_id,
                ref_columns,
                on_delete: (*on_delete).map(FkAction::from),
                on_update: (*on_update).map(FkAction::from),
            }
        }
    };

    Ok(BoundTableConstraint { name, body })
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use super::*;
    use crate::{
        PAGE_SIZE, TransactionId, Type, Value,
        buffer_pool::page_store::PageStore,
        catalog::manager::Catalog,
        index::hash::HashIndex,
        parser::statements::{ColumnDef, CreateTableStatement, DropStatement, Uniqueness},
        primitives::NonEmptyString,
        transaction::TransactionManager,
        tuple::{Field, TupleSchema},
        wal::writer::Wal,
    };

    fn field(name: &str, field_type: Type) -> Field {
        Field::new(name, field_type).unwrap()
    }

    fn make_catalog_and_txn_mgr(dir: &Path) -> (Catalog, TransactionManager) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).expect("WAL creation failed"));
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        let catalog = Catalog::initialize(&bp, &wal, dir).expect("catalog init failed");
        let txn_mgr = TransactionManager::new(wal, bp);
        (catalog, txn_mgr)
    }

    /// Variant of [`make_catalog_and_txn_mgr`] that also exposes the underlying
    /// `PageStore` and `Wal` so index tests can build and register a real
    /// `HashIndex` against the catalog.
    fn make_full_infra(dir: &Path) -> (Catalog, TransactionManager, Arc<PageStore>, Arc<Wal>) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).expect("WAL creation failed"));
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        let catalog = Catalog::initialize(&bp, &wal, dir).expect("catalog init failed");
        let txn_mgr = TransactionManager::new(Arc::clone(&wal), Arc::clone(&bp));
        (catalog, txn_mgr, bp, wal)
    }

    /// Builds a small live `HashIndex` and registers it under `index_name` for
    /// `table_file_id`. The index itself is never queried by these tests — the
    /// binder only consults the registry — but the catalog API requires a real
    /// `AnyIndex`, so we stand one up over a freshly-created data file.
    fn register_test_index(
        catalog: &Catalog,
        bp: &Arc<PageStore>,
        wal: &Wal,
        dir: &Path,
        table_file_id: FileId,
        index_name: &str,
        idx_file_id: FileId,
    ) {
        const NUM_BUCKETS: u32 = 4;
        let path = dir.join(format!("idx_{}.dat", idx_file_id.0));
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.set_len(u64::from(NUM_BUCKETS) * PAGE_SIZE as u64)
            .unwrap();
        drop(f);
        bp.register_file(idx_file_id, &path).unwrap();

        let hash = HashIndex::new(
            idx_file_id,
            vec![Type::Int64],
            NUM_BUCKETS,
            Arc::clone(bp),
            NUM_BUCKETS,
        );
        let init_txn = TransactionId::new(idx_file_id.0);
        wal.log_begin(init_txn).unwrap();
        hash.init(init_txn).unwrap();
        bp.release_all(init_txn);

        catalog
            .register_index(index_name, table_file_id, hash.into(), vec![
                ColumnId::try_from(0usize).unwrap(),
            ])
            .unwrap();
    }

    fn create_index_stmt(
        index_name: &str,
        table_name: &str,
        columns: &[&str],
        kind: IndexKind,
        if_not_exists: bool,
    ) -> CreateIndexStatement {
        CreateIndexStatement {
            index_name: NonEmptyString::new(index_name).unwrap(),
            table_name: NonEmptyString::new(table_name).unwrap(),
            columns: columns
                .iter()
                .map(|c| NonEmptyString::new(*c).unwrap())
                .collect(),
            index_type: kind,
            if_not_exists,
        }
    }

    fn drop_index_stmt(
        index_name: &str,
        table_name: &str,
        if_exists: bool,
    ) -> crate::parser::statements::DropIndexStatement {
        crate::parser::statements::DropIndexStatement {
            table_name: Some(NonEmptyString::new(table_name).unwrap()),
            index_name: NonEmptyString::new(index_name).unwrap(),
            if_exists,
        }
    }

    fn col(name: &str, ty: Type, pk: bool) -> ColumnDef {
        ColumnDef {
            name: NonEmptyString::new(name).unwrap(),
            col_type: ty,
            nullable: true,
            primary_key: pk,
            auto_increment: false,
            default: None,
            unique: Uniqueness::NotUnique,
            check: None,
            references: None,
        }
    }

    fn create_stmt(
        name: &str,
        cols: Vec<ColumnDef>,
        if_not_exists: bool,
        table_pk: Option<&str>,
    ) -> CreateTableStatement {
        CreateTableStatement {
            table_name: NonEmptyString::new(name).unwrap(),
            if_not_exists,
            columns: cols,
            primary_key: table_pk
                .into_iter()
                .map(|s| NonEmptyString::new(s).unwrap())
                .collect(),
            constraints: vec![],
        }
    }

    fn drop_stmt(name: &str, if_exists: bool) -> DropStatement {
        DropStatement {
            table_name: NonEmptyString::new(name).unwrap(),
            if_exists,
        }
    }

    fn two_col_schema() -> TupleSchema {
        TupleSchema::new(vec![
            field("id", Type::Uint64).not_null(),
            field("name", Type::String).not_null(),
        ])
    }

    fn col_id(idx: usize) -> ColumnId {
        ColumnId::try_from(idx).unwrap()
    }

    fn unwrap_alter_action(bound: BoundAlterTable) -> (String, FileId, BoundAlterAction) {
        match bound {
            BoundAlterTable::Action {
                table_name,
                file_id,
                action,
            } => (table_name, file_id, action),
            other @ BoundAlterTable::NoOp { .. } => {
                panic!("expected AlterTable action, got {other:?}")
            }
        }
    }

    // Existing table is resolved to Drop carrying the canonical name + file_id.
    #[test]
    fn test_bind_drop_existing_table_returns_drop() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let bound = BoundDrop::bind(&drop_stmt("users", false), &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        match bound {
            BoundDrop::Drop { name, file_id: fid } => {
                assert_eq!(name, "users");
                assert_eq!(fid, file_id);
            }
            BoundDrop::NoOp { .. } => panic!("expected Drop, got NoOp"),
        }
    }

    // IF EXISTS on a missing table is a NoOp, not an error.
    #[test]
    fn test_bind_drop_missing_with_if_exists_is_noop() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let bound = BoundDrop::bind(&drop_stmt("ghost", true), &catalog, &txn).unwrap();
        txn.commit().unwrap();

        match bound {
            BoundDrop::NoOp { name } => assert_eq!(name, "ghost"),
            BoundDrop::Drop { .. } => panic!("expected NoOp, got Drop"),
        }
    }

    // Missing table without IF EXISTS surfaces as BindError::UnknownTable.
    #[test]
    fn test_bind_drop_missing_without_if_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let Err(err) = BoundDrop::bind(&drop_stmt("ghost", false), &catalog, &txn) else {
            panic!("expected error");
        };
        txn.commit().unwrap();

        assert!(
            matches!(err, BindError::UnknownTable(ref n) if n == "ghost"),
            "unexpected error: {err:?}"
        );
    }

    // New table with no PK produces BoundCreateTable::New with primary_key = None.
    #[test]
    fn test_bind_create_table_new_no_pk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt(
            "t",
            vec![col("a", Type::Int64, false), col("b", Type::Int64, false)],
            false,
            None,
        );
        let bound = BoundCreateTable::bind(&stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        match bound {
            BoundCreateTable::New {
                name,
                primary_key,
                schema,
                constraints,
            } => {
                assert_eq!(name, "t");
                assert!(primary_key.is_none());
                assert_eq!(schema.physical_num_fields(), 2);
                assert!(constraints.is_empty());
            }
            BoundCreateTable::AlreadyExists { .. } => panic!("expected New"),
        }
    }

    #[test]
    fn test_bind_create_table_unique_constraint_resolves_column_ids() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = CreateTableStatement {
            table_name: NonEmptyString::new("users").unwrap(),
            if_not_exists: false,
            columns: vec![
                col("id", Type::Int64, false),
                col("name", Type::String, false),
            ],
            primary_key: vec![],
            constraints: vec![(
                Some(NonEmptyString::new("users_name_key").unwrap()),
                TableConstraint::Unique {
                    columns: vec![NonEmptyString::new("name").unwrap()],
                },
            )],
        };
        let bound = BoundCreateTable::bind(&stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        match bound {
            BoundCreateTable::New { constraints, .. } => {
                assert_eq!(constraints.len(), 1);
                let c = &constraints[0];
                assert_eq!(
                    c.name.as_ref().map(NonEmptyString::as_str),
                    Some("users_name_key")
                );
                match &c.body {
                    BoundConstraintBody::Unique { columns } => {
                        assert_eq!(columns, &vec![col_id(1)]);
                    }
                    other => panic!("expected Unique, got {other:?}"),
                }
            }
            BoundCreateTable::AlreadyExists { .. } => panic!("expected New"),
        }
    }

    // Inline `primary_key: true` on a column resolves to that column's index.
    #[test]
    fn test_bind_create_table_inline_pk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt(
            "t",
            vec![
                col("a", Type::Int64, false),
                col("b", Type::Int64, true),
                col("c", Type::Int64, false),
            ],
            false,
            None,
        );
        let bound = BoundCreateTable::bind(&stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        match bound {
            BoundCreateTable::New { primary_key, .. } => {
                assert_eq!(primary_key, Some(vec![col_id(1)]));
            }
            BoundCreateTable::AlreadyExists { .. } => panic!("expected New"),
        }
    }

    // Table-level PK clause resolves to the named column's index.
    #[test]
    fn test_bind_create_table_table_level_pk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt(
            "t",
            vec![col("a", Type::Int64, false), col("b", Type::Int64, false)],
            false,
            Some("b"),
        );
        let bound = BoundCreateTable::bind(&stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        match bound {
            BoundCreateTable::New { primary_key, .. } => {
                assert_eq!(primary_key, Some(vec![col_id(1)]));
            }
            BoundCreateTable::AlreadyExists { .. } => panic!("expected New"),
        }
    }

    // When both inline and table-level PK are present, inline wins (find().or() order).
    #[test]
    fn test_bind_create_table_inline_pk_wins_over_table_level() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt(
            "t",
            vec![col("a", Type::Int64, true), col("b", Type::Int64, false)],
            false,
            Some("b"),
        );
        let bound = BoundCreateTable::bind(&stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        match bound {
            BoundCreateTable::New { primary_key, .. } => {
                assert_eq!(primary_key, Some(vec![col_id(0)]));
            }
            BoundCreateTable::AlreadyExists { .. } => panic!("expected New"),
        }
    }

    // ── edge cases: bind_create_table ─────────────────────────────────────

    // IF NOT EXISTS on an existing table returns AlreadyExists with that table's file_id.
    #[test]
    fn test_bind_create_table_if_not_exists_on_existing() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = create_stmt("t", vec![col("a", Type::Int64, false)], true, None);
        let bound = BoundCreateTable::bind(&stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        match bound {
            BoundCreateTable::AlreadyExists { name, file_id: fid } => {
                assert_eq!(name, "t");
                assert_eq!(fid, file_id);
            }
            BoundCreateTable::New { .. } => panic!("expected AlreadyExists"),
        }
    }

    // Empty column list still binds: ddl.rs itself doesn't enforce non-empty columns.
    #[test]
    fn test_bind_create_table_empty_columns() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt("t", vec![], false, None);
        let bound = BoundCreateTable::bind(&stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        match bound {
            BoundCreateTable::New {
                primary_key,
                schema,
                ..
            } => {
                assert!(primary_key.is_none());
                assert_eq!(schema.physical_num_fields(), 0);
            }
            BoundCreateTable::AlreadyExists { .. } => panic!("expected New"),
        }
    }

    // ── error paths: bind_create_table ────────────────────────────────────

    // Existing table without IF NOT EXISTS errors as TableAlreadyExists.
    #[test]
    fn test_bind_create_table_existing_without_if_not_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "t", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = create_stmt("t", vec![col("a", Type::Int64, false)], false, None);
        let Err(err) = BoundCreateTable::bind(&stmt, &catalog, &txn2) else {
            panic!("expected error");
        };
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::TableAlreadyExists(ref n) if n == "t"),
            "unexpected error: {err:?}"
        );
    }

    // Duplicate column names error before PK resolution runs.
    #[test]
    fn test_bind_create_table_duplicate_columns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt(
            "t",
            vec![col("a", Type::Int64, false), col("a", Type::Int64, false)],
            false,
            None,
        );
        let Err(err) = BoundCreateTable::bind(&stmt, &catalog, &txn) else {
            panic!("expected error");
        };
        txn.commit().unwrap();

        assert!(
            matches!(err, BindError::DuplicateColumn(ref n) if n == "a"),
            "unexpected error: {err:?}"
        );
    }

    // Duplicate-column check fires before PK-not-found, even if PK is also bogus.
    #[test]
    fn test_bind_create_table_duplicate_columns_takes_precedence_over_pk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt(
            "t",
            vec![col("a", Type::Int64, false), col("a", Type::Int64, false)],
            false,
            Some("missing"),
        );
        let Err(err) = BoundCreateTable::bind(&stmt, &catalog, &txn) else {
            panic!("expected error");
        };
        txn.commit().unwrap();

        assert!(matches!(err, BindError::DuplicateColumn(_)));
    }

    // Table-level PK referencing a missing column errors as PrimaryKeyNotInColumns.
    #[test]
    fn test_bind_create_table_pk_not_in_columns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt(
            "t",
            vec![col("a", Type::Int64, false), col("b", Type::Int64, false)],
            false,
            Some("c"),
        );
        let Err(err) = BoundCreateTable::bind(&stmt, &catalog, &txn) else {
            panic!("expected error");
        };
        txn.commit().unwrap();

        assert!(
            matches!(err, BindError::PrimaryKeyNotInColumns(ref n) if n == "c"),
            "unexpected error: {err:?}"
        );
    }

    // Column-name lookup is case-sensitive: PK "A" with column "a" should fail.
    #[test]
    fn test_bind_create_table_pk_lookup_is_case_sensitive() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_stmt("t", vec![col("a", Type::Int64, false)], false, Some("A"));
        let Err(err) = BoundCreateTable::bind(&stmt, &catalog, &txn) else {
            panic!("expected error");
        };
        txn.commit().unwrap();

        assert!(matches!(err, BindError::PrimaryKeyNotInColumns(_)));
    }

    // Resolves a single column name to its index in the table's schema and
    // preserves the access-method kind from the AST.
    #[test]
    fn test_bind_create_index_new_single_column() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = create_index_stmt("users_name_idx", "users", &["name"], IndexKind::Hash, false);
        let bound = BoundCreateIndex::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        match bound {
            BoundCreateIndex::New {
                index_name,
                table_name,
                table_file_id: tfid,
                column_indices,
                kind,
            } => {
                assert_eq!(index_name, "users_name_idx");
                assert_eq!(table_name, "users");
                assert_eq!(tfid, table_file_id);
                assert_eq!(column_indices, vec![col_id(1)]);
                assert_eq!(kind, IndexKind::Hash);
            }
            BoundCreateIndex::AlreadyExists { .. } => panic!("expected New"),
        }
    }

    // Composite indexes: each column resolves to its own offset, in declaration order.
    #[test]
    fn test_bind_create_index_composite_preserves_order() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let schema = TupleSchema::new(vec![
            field("a", Type::Int64).not_null(),
            field("b", Type::Int64).not_null(),
            field("c", Type::Int64).not_null(),
        ]);
        catalog.create_table(&txn, "t", schema, None).unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        // Order in the AST is (c, a) → indices (2, 0). Order is meaningful: it
        // determines the composite-key layout the index will use.
        let stmt = create_index_stmt("t_ca_idx", "t", &["c", "a"], IndexKind::Btree, false);
        let bound = BoundCreateIndex::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        match bound {
            BoundCreateIndex::New {
                column_indices,
                kind,
                ..
            } => {
                assert_eq!(column_indices, vec![col_id(2), col_id(0)]);
                assert_eq!(kind, IndexKind::Btree);
            }
            BoundCreateIndex::AlreadyExists { .. } => panic!("expected New"),
        }
    }

    // Unknown column inside the column list surfaces as BindError::UnknownColumn,
    // tagged with the table name the binder resolved.
    #[test]
    fn test_bind_create_index_unknown_column_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = create_index_stmt("ix", "users", &["ghost"], IndexKind::Hash, false);
        let err = BoundCreateIndex::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(
                err,
                BindError::UnknownColumn { ref table, ref column }
                    if table == "users" && column == "ghost"
            ),
            "unexpected error: {err:?}"
        );
    }

    // A column listed twice in CREATE INDEX (...) is a static error before any
    // schema lookup — this catches user typos like `(a, a)`.
    #[test]
    fn test_bind_create_index_duplicate_columns_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = create_index_stmt("ix", "users", &["id", "id"], IndexKind::Hash, false);
        let err = BoundCreateIndex::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::DuplicateColumn(ref n) if n == "id"),
            "unexpected error: {err:?}"
        );
    }

    // CREATE INDEX against a missing table is reshaped from CatalogError::TableNotFound
    // into BindError::UnknownTable so the binder layer's vocabulary stays clean.
    #[test]
    fn test_bind_create_index_unknown_table_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = create_index_stmt("ix", "ghost", &["x"], IndexKind::Hash, false);
        let err = BoundCreateIndex::bind(stmt, &catalog, &txn).unwrap_err();
        txn.commit().unwrap();

        assert!(
            matches!(err, BindError::UnknownTable(ref n) if n == "ghost"),
            "unexpected error: {err:?}"
        );
    }

    // An index name already in the registry collides — without IF NOT EXISTS this is
    // a hard error.
    #[test]
    fn test_bind_create_index_already_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, bp, wal) = make_full_infra(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        register_test_index(
            &catalog,
            &bp,
            &wal,
            dir.path(),
            table_file_id,
            "users_id_idx",
            FileId::new(1000),
        );

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = create_index_stmt("users_id_idx", "users", &["id"], IndexKind::Hash, false);
        let err = BoundCreateIndex::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::IndexAlreadyExists(ref n) if n == "users_id_idx"),
            "unexpected error: {err:?}"
        );
    }

    // With IF NOT EXISTS, a name collision binds as AlreadyExists (a no-op).
    #[test]
    fn test_bind_create_index_if_not_exists_on_existing() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, bp, wal) = make_full_infra(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        register_test_index(
            &catalog,
            &bp,
            &wal,
            dir.path(),
            table_file_id,
            "users_id_idx",
            FileId::new(1001),
        );

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = create_index_stmt("users_id_idx", "users", &["id"], IndexKind::Hash, true);
        let bound = BoundCreateIndex::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        match bound {
            BoundCreateIndex::AlreadyExists { index_name } => {
                assert_eq!(index_name, "users_id_idx");
            }
            BoundCreateIndex::New { .. } => panic!("expected AlreadyExists"),
        }
    }

    // A registered index name resolves to BoundDropIndex::Drop carrying that name.
    #[test]
    fn test_bind_drop_index_existing_returns_drop() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, bp, wal) = make_full_infra(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        register_test_index(
            &catalog,
            &bp,
            &wal,
            dir.path(),
            table_file_id,
            "users_id_idx",
            FileId::new(1002),
        );

        let txn2 = txn_mgr.begin().unwrap();
        let bound = BoundDropIndex::bind(
            drop_index_stmt("users_id_idx", "users", false),
            &catalog,
            &txn2,
        )
        .expect("drop should bind");
        txn2.commit().unwrap();

        match bound {
            BoundDropIndex::Drop { index_name } => assert_eq!(index_name, "users_id_idx"),
            BoundDropIndex::NoOp { .. } => panic!("expected Drop, got NoOp"),
        }
    }

    // DROP INDEX IF EXISTS on a missing index is a successful no-op.
    #[test]
    fn test_bind_drop_index_missing_with_if_exists_is_noop() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let bound =
            BoundDropIndex::bind(drop_index_stmt("ghost_idx", "users", true), &catalog, &txn)
                .expect("if-exists drop should bind");
        txn.commit().unwrap();

        match bound {
            BoundDropIndex::NoOp { index_name } => assert_eq!(index_name, "ghost_idx"),
            BoundDropIndex::Drop { .. } => panic!("expected NoOp, got Drop"),
        }
    }

    // Without IF EXISTS, a missing index is a hard BindError::UnknownIndex.
    #[test]
    fn test_bind_drop_index_missing_without_if_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let err =
            BoundDropIndex::bind(drop_index_stmt("ghost_idx", "users", false), &catalog, &txn)
                .unwrap_err();
        txn.commit().unwrap();

        assert!(
            matches!(err, BindError::UnknownIndex(ref n) if n == "ghost_idx"),
            "unexpected error: {err:?}"
        );
    }

    // Existing index + wrong table qualifier is treated as not found for that table.
    #[test]
    fn test_bind_drop_index_existing_wrong_table_errors_without_if_exists() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, bp, wal) = make_full_infra(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let users_file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        catalog
            .create_table(&txn, "orders", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        register_test_index(
            &catalog,
            &bp,
            &wal,
            dir.path(),
            users_file_id,
            "users_id_idx",
            FileId::new(1003),
        );

        let txn2 = txn_mgr.begin().unwrap();
        let err = BoundDropIndex::bind(
            drop_index_stmt("users_id_idx", "orders", false),
            &catalog,
            &txn2,
        )
        .unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::UnknownIndex(ref n) if n == "users_id_idx"),
            "unexpected error: {err:?}"
        );
    }

    // ── helpers: bind_alter_table ─────────────────────────────────────────

    fn alter_stmt(table_name: &str, if_exists: bool, action: AlterAction) -> AlterTableStatement {
        AlterTableStatement {
            table_name: NonEmptyString::new(table_name).unwrap(),
            if_exists,
            action,
        }
    }

    fn col_def(name: &str, ty: Type) -> ColumnDef {
        ColumnDef {
            name: NonEmptyString::new(name).unwrap(),
            col_type: ty,
            nullable: true,
            primary_key: false,
            auto_increment: false,
            default: None,
            unique: Uniqueness::NotUnique,
            check: None,
            references: None,
        }
    }

    // Adding a brand-new column resolves to AddColumn carrying the correct
    // table name, file_id, and column definition.
    #[test]
    fn test_bind_alter_add_column_new() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt(
            "users",
            false,
            AlterAction::AddColumn(col_def("age", Type::Int64)),
        );
        let bound = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        let (_, fid, action) = unwrap_alter_action(bound);
        assert_eq!(fid, file_id);
        match action {
            BoundAlterAction::AddColumn { column } => {
                assert_eq!(column.name, "age");
            }
            other => panic!("expected AddColumn, got {other:?}"),
        }
    }

    // Adding a column whose name already exists in the schema is a static
    // duplicate-column error; the binder catches it before the executor runs.
    #[test]
    fn test_bind_alter_add_column_duplicate_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt(
            "users",
            false,
            AlterAction::AddColumn(col_def("name", Type::String)),
        );
        let err = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::DuplicateColumn(ref n) if n == "name"),
            "unexpected error: {err:?}"
        );
    }

    // ALTER TABLE on a table that doesn't exist (no IF EXISTS) surfaces as
    // UnknownTable, regardless of which action was requested.
    #[test]
    fn test_bind_alter_add_column_unknown_table_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = alter_stmt(
            "ghost",
            false,
            AlterAction::AddColumn(col_def("x", Type::Int64)),
        );
        let err = BoundAlterTable::bind(stmt, &catalog, &txn).unwrap_err();
        txn.commit().unwrap();

        assert!(
            matches!(err, BindError::UnknownTable(ref n) if n == "ghost"),
            "unexpected error: {err:?}"
        );
    }

    // With IF EXISTS on the outer table clause, a missing table collapses to
    // NoOp instead of an error — regardless of which action was inside.
    #[test]
    fn test_bind_alter_if_exists_on_missing_table_is_noop() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = alter_stmt(
            "ghost",
            true,
            AlterAction::AddColumn(col_def("x", Type::Int64)),
        );
        let bound = BoundAlterTable::bind(stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        match bound {
            BoundAlterTable::NoOp { table_name } => assert_eq!(table_name, "ghost"),
            other @ BoundAlterTable::Action { .. } => panic!("expected NoOp, got {other:?}"),
        }
    }

    // Dropping an existing column resolves its zero-based position in the
    // schema (name → ColumnId) correctly.
    #[test]
    fn test_bind_alter_drop_column_existing() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::DropColumn {
            name: NonEmptyString::new("name").unwrap(),
            if_exists: false,
        });
        let bound = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        // two_col_schema() is (id → 0, name → 1); dropping "name" must yield ColumnId(1).
        let (_, fid, action) = unwrap_alter_action(bound);
        assert_eq!(fid, file_id);
        match action {
            BoundAlterAction::DropColumn {
                column_name,
                column_id,
            } => {
                assert_eq!(column_name, "name");
                assert_eq!(column_id, col_id(1));
            }
            other => panic!("expected DropColumn, got {other:?}"),
        }
    }

    // Dropping a column that doesn't exist (no inner IF EXISTS) is
    // UnknownColumn — the column name and table name both appear in the error.
    #[test]
    fn test_bind_alter_drop_column_unknown_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::DropColumn {
            name: NonEmptyString::new("ghost").unwrap(),
            if_exists: false,
        });
        let err = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(
                err,
                BindError::UnknownColumn { ref table, ref column }
                    if table == "users" && column == "ghost"
            ),
            "unexpected error: {err:?}"
        );
    }

    // DROP COLUMN IF EXISTS on a missing column is a per-column no-op; the
    // inner `if_exists` is independent of the outer table-level `if_exists`.
    #[test]
    fn test_bind_alter_drop_column_if_exists_on_missing_col_is_noop() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::DropColumn {
            name: NonEmptyString::new("ghost").unwrap(),
            if_exists: true,
        });
        let bound = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        match bound {
            BoundAlterTable::NoOp { table_name } => assert_eq!(table_name, "users"),
            other @ BoundAlterTable::Action { .. } => panic!("expected NoOp, got {other:?}"),
        }
    }

    // Renaming an existing column to a free name resolves correctly.
    #[test]
    fn test_bind_alter_rename_column_happy() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::RenameColumn {
            from: NonEmptyString::new("name").unwrap(),
            to: NonEmptyString::new("full_name").unwrap(),
        });
        let bound = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        let (_, fid, action) = unwrap_alter_action(bound);
        assert_eq!(fid, file_id);
        match action {
            BoundAlterAction::RenameColumn { old_name, new_name } => {
                assert_eq!(old_name, "name");
                assert_eq!(new_name, "full_name");
            }
            other => panic!("expected RenameColumn, got {other:?}"),
        }
    }

    // The `from` column must exist; otherwise UnknownColumn.
    #[test]
    fn test_bind_alter_rename_column_from_missing_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::RenameColumn {
            from: NonEmptyString::new("ghost").unwrap(),
            to: NonEmptyString::new("x").unwrap(),
        });
        let err = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(
                err,
                BindError::UnknownColumn { ref table, ref column }
                    if table == "users" && column == "ghost"
            ),
            "unexpected error: {err:?}"
        );
    }

    // The `to` name must not already be a column; otherwise DuplicateColumn.
    #[test]
    fn test_bind_alter_rename_column_to_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        // Renaming "id" to "name" would create a second "name" column.
        let stmt = alter_stmt("users", false, AlterAction::RenameColumn {
            from: NonEmptyString::new("id").unwrap(),
            to: NonEmptyString::new("name").unwrap(),
        });
        let err = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::DuplicateColumn(ref n) if n == "name"),
            "unexpected error: {err:?}"
        );
    }

    // Renaming a table to a free name resolves to RenameTable carrying both
    // old and new names plus the heap file_id.
    #[test]
    fn test_bind_alter_rename_table_happy() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::RenameTable {
            to: NonEmptyString::new("accounts").unwrap(),
        });
        let bound = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        let (_, fid, action) = unwrap_alter_action(bound);
        assert_eq!(fid, file_id);
        match action {
            BoundAlterAction::RenameTable { old_name, new_name } => {
                assert_eq!(old_name, "users");
                assert_eq!(new_name, "accounts");
            }
            other => panic!("expected RenameTable, got {other:?}"),
        }
    }

    // Renaming to a name already taken by another table is TableAlreadyExists.
    #[test]
    fn test_bind_alter_rename_table_target_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        catalog
            .create_table(&txn, "accounts", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::RenameTable {
            to: NonEmptyString::new("accounts").unwrap(),
        });
        let err = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::TableAlreadyExists(ref n) if n == "accounts"),
            "unexpected error: {err:?}"
        );
    }

    // Missing table without IF EXISTS is UnknownTable.
    #[test]
    fn test_bind_alter_rename_table_missing_without_if_exists_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = alter_stmt("ghost", false, AlterAction::RenameTable {
            to: NonEmptyString::new("x").unwrap(),
        });
        let err = BoundAlterTable::bind(stmt, &catalog, &txn).unwrap_err();
        txn.commit().unwrap();

        assert!(
            matches!(err, BindError::UnknownTable(ref n) if n == "ghost"),
            "unexpected error: {err:?}"
        );
    }

    // Missing table with IF EXISTS is a no-op.
    #[test]
    fn test_bind_alter_rename_table_missing_with_if_exists_is_noop() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();

        let stmt = alter_stmt("ghost", true, AlterAction::RenameTable {
            to: NonEmptyString::new("x").unwrap(),
        });
        let bound = BoundAlterTable::bind(stmt, &catalog, &txn).unwrap();
        txn.commit().unwrap();

        match bound {
            BoundAlterTable::NoOp { table_name } => assert_eq!(table_name, "ghost"),
            other @ BoundAlterTable::Action { .. } => panic!("expected NoOp, got {other:?}"),
        }
    }

    #[test]
    fn test_bind_alter_set_default_known_column_returns_set_default() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::SetDefault {
            column: NonEmptyString::new("name").unwrap(),
            value: Value::String("anon".to_string()),
        });
        let bound = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        let (_, fid, action) = unwrap_alter_action(bound);
        assert_eq!(fid, file_id);
        match action {
            BoundAlterAction::SetDefault { column, value } => {
                assert_eq!(column, "name");
                assert_eq!(value, Value::String("anon".to_string()));
            }
            other => panic!("expected SetDefault, got {other:?}"),
        }
    }

    #[test]
    fn test_bind_alter_set_default_unknown_column_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::SetDefault {
            column: NonEmptyString::new("ghost").unwrap(),
            value: Value::Int64(0),
        });
        let err = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::UnknownColumn { ref column, .. } if column == "ghost"),
            "unexpected error: {err:?}"
        );
    }

    // ── DROP DEFAULT ──────────────────────────────────────────────────────

    #[test]
    fn test_bind_alter_drop_default_known_column_returns_drop_default() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::DropDefault {
            column: NonEmptyString::new("name").unwrap(),
        });
        let bound = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        let (_, fid, action) = unwrap_alter_action(bound);
        assert_eq!(fid, file_id);
        match action {
            BoundAlterAction::DropDefault { column } => {
                assert_eq!(column, "name");
            }
            other => panic!("expected DropDefault, got {other:?}"),
        }
    }

    #[test]
    fn test_bind_alter_drop_default_unknown_column_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::DropDefault {
            column: NonEmptyString::new("ghost").unwrap(),
        });
        let err = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::UnknownColumn { ref column, .. } if column == "ghost"),
            "unexpected error: {err:?}"
        );
    }

    // ── DROP NOT NULL ─────────────────────────────────────────────────────

    #[test]
    fn test_bind_alter_drop_not_null_known_column_returns_drop_not_null() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::DropNotNull {
            column: NonEmptyString::new("id").unwrap(),
        });
        let bound = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        let (_, fid, action) = unwrap_alter_action(bound);
        assert_eq!(fid, file_id);
        match action {
            BoundAlterAction::DropNotNull { column } => {
                assert_eq!(column, "id");
            }
            other => panic!("expected DropNotNull, got {other:?}"),
        }
    }

    #[test]
    fn test_bind_alter_drop_not_null_unknown_column_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::DropNotNull {
            column: NonEmptyString::new("ghost").unwrap(),
        });
        let err = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::UnknownColumn { ref column, .. } if column == "ghost"),
            "unexpected error: {err:?}"
        );
    }

    // ── ADD PRIMARY KEY ───────────────────────────────────────────────────

    #[test]
    fn test_bind_alter_add_primary_key_single_column_resolves_column_id() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        // two_col_schema() is (id → 0, name → 1); no PK.
        let file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::AddPrimaryKey {
            columns: vec![NonEmptyString::new("name").unwrap()],
        });
        let bound = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        let (_, fid, action) = unwrap_alter_action(bound);
        assert_eq!(fid, file_id);
        match action {
            BoundAlterAction::AddPrimaryKey { column_ids } => {
                assert_eq!(column_ids, vec![col_id(1)]);
            }
            other => panic!("expected AddPrimaryKey, got {other:?}"),
        }
    }

    #[test]
    fn test_bind_alter_add_primary_key_composite_preserves_order() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let schema = TupleSchema::new(vec![
            field("a", Type::Int64).not_null(),
            field("b", Type::Int64).not_null(),
            field("c", Type::Int64).not_null(),
        ]);
        catalog.create_table(&txn, "t", schema, None).unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("t", false, AlterAction::AddPrimaryKey {
            columns: vec![
                NonEmptyString::new("c").unwrap(),
                NonEmptyString::new("a").unwrap(),
            ],
        });
        let bound = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        let (_, _, action) = unwrap_alter_action(bound);
        match action {
            BoundAlterAction::AddPrimaryKey { column_ids } => {
                // c → 2, a → 0, order preserved from the SQL clause.
                assert_eq!(column_ids, vec![col_id(2), col_id(0)]);
            }
            other => panic!("expected AddPrimaryKey, got {other:?}"),
        }
    }

    #[test]
    fn test_bind_alter_add_primary_key_existing_pk_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), Some(vec![col_id(0)]))
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::AddPrimaryKey {
            columns: vec![NonEmptyString::new("name").unwrap()],
        });
        let err = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::PrimaryKeyAlreadyExists(ref t) if t == "users"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_bind_alter_add_primary_key_unknown_column_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::AddPrimaryKey {
            columns: vec![NonEmptyString::new("ghost").unwrap()],
        });
        let err = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::UnknownColumn { ref column, .. } if column == "ghost"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_bind_alter_add_primary_key_duplicate_columns_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::AddPrimaryKey {
            columns: vec![
                NonEmptyString::new("id").unwrap(),
                NonEmptyString::new("id").unwrap(),
            ],
        });
        let err = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap_err();
        txn2.commit().unwrap();

        assert!(
            matches!(err, BindError::DuplicateColumn(ref c) if c == "id"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_bind_alter_add_fk_constraint_resolves_ids_and_actions() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let parent_id = catalog
            .create_table(&txn, "parents", two_col_schema(), None)
            .unwrap();
        catalog
            .create_table(&txn, "children", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("children", false, AlterAction::AddConstraint {
            name: Some(NonEmptyString::new("children_parent_fk").unwrap()),
            constraint: TableConstraint::ForeignKey {
                local_cols: vec![NonEmptyString::new("id").unwrap()],
                ref_table: NonEmptyString::new("parents").unwrap(),
                ref_cols: vec![NonEmptyString::new("id").unwrap()],
                on_delete: Some(ReferentialAction::Cascade),
                on_update: Some(ReferentialAction::Restrict),
            },
        });
        let bound = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        let (_, _, action) = unwrap_alter_action(bound);
        match action {
            BoundAlterAction::AddConstraint { constraint } => {
                assert_eq!(
                    constraint.name.as_ref().map(NonEmptyString::as_str),
                    Some("children_parent_fk")
                );
                match constraint.body {
                    BoundConstraintBody::ForeignKey {
                        local_columns,
                        ref_table_id,
                        ref_columns,
                        on_delete,
                        on_update,
                    } => {
                        assert_eq!(local_columns, vec![col_id(0)]);
                        assert_eq!(ref_columns, vec![col_id(0)]);
                        assert_eq!(ref_table_id, parent_id);
                        assert_eq!(on_delete, Some(FkAction::Cascade));
                        assert_eq!(on_update, Some(FkAction::Restrict));
                    }
                    other => panic!("expected ForeignKey, got {other:?}"),
                }
            }
            other => panic!("expected AddConstraint, got {other:?}"),
        }
    }

    #[test]
    fn test_bind_alter_drop_primary_key_returns_drop_primary_key() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn_mgr(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", two_col_schema(), None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let stmt = alter_stmt("users", false, AlterAction::DropPrimaryKey);
        let bound = BoundAlterTable::bind(stmt, &catalog, &txn2).unwrap();
        txn2.commit().unwrap();

        let (_, fid, action) = unwrap_alter_action(bound);
        assert_eq!(fid, file_id);
        match action {
            BoundAlterAction::DropPrimaryKey => {}
            other => panic!("expected DropPrimaryKey, got {other:?}"),
        }
    }
}
