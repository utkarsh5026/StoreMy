from .table_info import TableMetadata


class SchemaValidator:
    """🔍 Validates database schema consistency and constraints.
    
    🏗️ Ensures tables are created correctly
    ✅ Validates field names and types
    🔑 Checks primary key constraints  
    🔗 Verifies foreign key relationships
    """

    def __init__(self):
        """
        🎬 Initialize validator with empty error list.
        """
        self.validation_errors: list[str] = []

    def validate_table_creation(self, metadata: TableMetadata,
                                existing_tables: dict[str, TableMetadata]) -> bool:
        """
        🔍 Validate that a new table can be created.

        Args:
            metadata: Metadata for the table to create.
            existing_tables: Currently existing tables

        Returns:
            True if valid, False otherwise
        """
        self.validation_errors.clear()

        if metadata.table_name in existing_tables:
            self.validation_errors.append(f"Table '{metadata.table_name}' already exists")


        existing_ids = {t.table_id for t in existing_tables.values()}
        if metadata.table_id in existing_ids:
            self.validation_errors.append(f"Table ID {metadata.table_id} already in use")


        field_names = metadata.tuple_desc.field_names or []
        if len(field_names) != len(set(field_names)):
            self.validation_errors.append("Duplicate field names in table schema")

        # Validate primary key fields exist
        if metadata.primary_key_fields:
            schema_fields = set(field_names)
            pk_fields = set(metadata.primary_key_fields)
            missing_fields = pk_fields - schema_fields
            if missing_fields:
                self.validation_errors.append(
                    f"Primary key fields not in schema: {missing_fields}")

        self._validate_constraints(metadata, existing_tables)
        return len(self.validation_errors) == 0

    def validate_foreign_key_references(self,
                                        tables: dict[str, TableMetadata]) -> bool:
        """
        Validate all foreign key constraints are satisfied.

        Args:
            tables: All tables in the catalog

        Returns:
            True if all foreign keys are valid
        """
        self.validation_errors.clear()

        for table_name, table_meta in tables.items():
            for constraint in table_meta.get_foreign_key_constraints():
                if constraint.reference_table not in tables:
                    self.validation_errors.append(
                        f"Foreign key '{constraint.constraint_name}' in table "
                        f"'{table_name}' references non-existent table "
                        f"'{constraint.reference_table}'"
                    )
                    continue

                # Check referenced fields exist
                ref_table = tables[constraint.reference_table]
                ref_field_names = set(ref_table.tuple_desc.field_names or [])

                for ref_field in constraint.reference_fields or []:
                    if ref_field not in ref_field_names:
                        self.validation_errors.append(
                            f"Foreign key '{constraint.constraint_name}' references "
                            f"non-existent field '{ref_field}' in table "
                            f"'{constraint.reference_table}'"
                        )

        return len(self.validation_errors) == 0

    def _validate_constraints(self, metadata: TableMetadata,
                              existing_tables: dict[str, TableMetadata]) -> None:
        """Validate constraints for a table."""
        schema_fields = set(metadata.tuple_desc.field_names or [])

        for constraint in metadata.constraints.values():
            for field_name in constraint.field_names:
                if field_name not in schema_fields:
                    self.validation_errors.append(
                        f"Constraint '{constraint.constraint_name}' references "
                        f"non-existent field '{field_name}'"
                    )

            if constraint.constraint_type == "FOREIGN_KEY":
                if not constraint.reference_table:
                    self.validation_errors.append(
                        f"Foreign key constraint '{constraint.constraint_name}' "
                        f"missing reference table"
                    )
                elif constraint.reference_table in existing_tables:
                    ref_table = existing_tables[constraint.reference_table]
                    ref_fields = set(ref_table.tuple_desc.field_names or [])

                    for ref_field in constraint.reference_fields or []:
                        if ref_field not in ref_fields:
                            self.validation_errors.append(
                                f"Foreign key '{constraint.constraint_name}' references "
                                f"non-existent field '{ref_field}' in table "
                                f"'{constraint.reference_table}'"
                            )

    def get_validation_errors(self) -> list[str]:
        """Get a list of validation errors from last validation."""
        return self.validation_errors.copy()