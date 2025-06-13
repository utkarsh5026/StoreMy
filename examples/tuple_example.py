#!/usr/bin/env python3
"""
Comprehensive Tuple Example for StoreMy Database

This example demonstrates the complete working of tuples in the StoreMy database system:
- Creating tuple descriptors (schemas)
- Creating and manipulating tuples
- Using all field types (Int, String, Boolean, Float, Double)
- Record IDs for physical location tracking
- Serialization and deserialization
- Tuple combination for joins
- Error handling and validation

Run with: python examples/tuple_example.py
"""

from app.storage.heap.heap_page_id import HeapPageId
from app.core.types import (
    FieldType, IntField, StringField, BoolField,
    FloatField, DoubleField
)
from app.core.tuple import Tuple, TupleDesc, RecordId
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.rule import Rule
from rich import box
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


# Initialize Rich console
console = Console()


def print_header(title: str, subtitle: str = ""):
    """Print a beautiful header using Rich"""
    if subtitle:
        full_title = f"[bold blue]{title}[/bold blue]\n[dim]{subtitle}[/dim]"
    else:
        full_title = f"[bold blue]{title}[/bold blue]"

    console.print(Panel(
        full_title,
        style="bright_blue",
        box=box.DOUBLE,
        padding=(1, 2)
    ))


def print_step(step_num: int, title: str, description: str = ""):
    """Print a step header"""
    step_text = f"[bold yellow]Step {step_num}: {title}[/bold yellow]"
    if description:
        step_text += f"\n[dim italic]{description}[/dim italic]"
    console.print(step_text)
    console.print()


def print_success(message: str):
    """Print a success message"""
    console.print(f"[bold green]✓[/bold green] {message}")


def print_info(message: str):
    """Print an info message"""
    console.print(f"[bold cyan]ℹ[/bold cyan] {message}")


def print_warning(message: str):
    """Print a warning message"""
    console.print(f"[bold yellow]⚠[/bold yellow] {message}")


def print_error(message: str):
    """Print an error message"""
    console.print(f"[bold red]✗[/bold red] {message}")


def ask_continue(step_name: str = "next step") -> bool:
    """Ask user if they want to continue to the next step"""
    console.print()
    console.print(Rule("[dim]Waiting for user input[/dim]"))
    console.print()

    while True:
        try:
            response = input(
                f"🤔 Do you want to continue to the {step_name}? ([bold green]y[/bold green]/[bold red]n[/bold red]/[bold yellow]q[/bold yellow] to quit): ").lower().strip()

            if response in ['y', 'yes', '']:  # Default to yes
                console.print("[bold green]✓[/bold green] Continuing...")
                console.print()
                return True
            elif response in ['n', 'no']:
                console.print(
                    "[bold yellow]⏸[/bold yellow] Skipping this step...")
                console.print()
                return False
            elif response in ['q', 'quit', 'exit']:
                console.print(
                    "[bold red]👋[/bold red] Exiting demonstration. Thanks for exploring StoreMy tuples!")
                sys.exit(0)
            else:
                console.print(
                    "[bold red]❌[/bold red] Please enter 'y' for yes, 'n' for no, or 'q' to quit.")
        except KeyboardInterrupt:
            console.print(
                "\n[bold red]👋[/bold red] Demonstration interrupted. Goodbye!")
            sys.exit(0)
        except EOFError:
            console.print(
                "\n[bold red]👋[/bold red] Input stream ended. Goodbye!")
            sys.exit(0)


def create_field_type_table():
    """Create a table showing all supported field types"""
    table = Table(title="Supported Field Types", box=box.ROUNDED)
    table.add_column("Type", style="cyan", no_wrap=True)
    table.add_column("Python Type", style="magenta")
    table.add_column("Size (bytes)", style="green", justify="right")
    table.add_column("Description", style="white")

    table.add_row("INT", "int", "4", "32-bit signed integer")
    table.add_row("STRING", "str", "132",
                  "Variable-length string (max 128 chars + length)")
    table.add_row("BOOLEAN", "bool", "1", "True/False value")
    table.add_row("FLOAT", "float", "4", "32-bit floating point")
    table.add_row("DOUBLE", "float", "8", "64-bit floating point")

    return table


def demonstrate_tuple_desc():
    """Demonstrate TupleDesc creation and usage"""
    print_step(1, "Creating Tuple Descriptors (Schemas)",
               "TupleDesc defines the structure of a tuple - like a table schema")

    # Show supported field types
    console.print(create_field_type_table())
    console.print()

    # Create simple tuple descriptor
    print_info("Creating a simple schema: (INT, STRING)")
    simple_desc = TupleDesc([FieldType.INT, FieldType.STRING])
    console.print(f"Schema: {simple_desc}")
    console.print(f"Number of fields: {simple_desc.num_fields()}")
    console.print(f"Total size: {simple_desc.get_size()} bytes")
    print_success("Simple schema created successfully!")
    console.print()

    # Create complex tuple descriptor with names
    print_info("Creating a complex schema with field names")
    field_types = [FieldType.INT, FieldType.STRING,
                   FieldType.BOOLEAN, FieldType.FLOAT, FieldType.DOUBLE]
    field_names = ["id", "name", "active", "score", "average"]
    complex_desc = TupleDesc(field_types, field_names)

    console.print(f"Schema: {complex_desc}")
    console.print(f"Total size: {complex_desc.get_size()} bytes")

    # Show field details
    table = Table(title="Field Details", box=box.SIMPLE)
    table.add_column("Index", style="cyan", justify="right")
    table.add_column("Name", style="green")
    table.add_column("Type", style="magenta")
    table.add_column("Size", style="yellow", justify="right")

    for i in range(complex_desc.num_fields()):
        table.add_row(
            str(i),
            complex_desc.get_field_name(i),
            complex_desc.get_field_type(i).value,
            f"{complex_desc.get_field_type(i).get_length()} bytes"
        )

    console.print(table)
    print_success("Complex schema created successfully!")
    console.print()

    return simple_desc, complex_desc


def demonstrate_field_creation():
    """Demonstrate creation of all field types"""
    print_step(2, "Creating Fields",
               "Fields are the individual values that go into tuples")

    # Create examples of each field type
    fields = {}

    print_info("Creating IntField...")
    fields['int'] = IntField(42)
    console.print(
        f"IntField: {fields['int']} (type: {fields['int'].get_type()})")
    console.print(
        f"Value: {fields['int'].get_value()}, Size: {fields['int'].get_size()} bytes")
    console.print()

    print_info("Creating StringField...")
    fields['string'] = StringField("Hello, Database!")
    console.print(
        f"StringField: {fields['string']} (type: {fields['string'].get_type()})")
    console.print(
        f"Value: '{fields['string'].get_value()}', Size: {fields['string'].get_size()} bytes")
    console.print()

    print_info("Creating BoolField...")
    fields['bool'] = BoolField(True)
    console.print(
        f"BoolField: {fields['bool']} (type: {fields['bool'].get_type()})")
    console.print(
        f"Value: {fields['bool'].get_value()}, Size: {fields['bool'].get_size()} bytes")
    console.print()

    print_info("Creating FloatField...")
    fields['float'] = FloatField(3.14159)
    console.print(
        f"FloatField: {fields['float']} (type: {fields['float'].get_type()})")
    console.print(
        f"Value: {fields['float'].get_value()}, Size: {fields['float'].get_size()} bytes")
    console.print()

    print_info("Creating DoubleField...")
    fields['double'] = DoubleField(2.718281828459045)
    console.print(
        f"DoubleField: {fields['double']} (type: {fields['double'].get_type()})")
    console.print(
        f"Value: {fields['double'].get_value()}, Size: {fields['double'].get_size()} bytes")
    console.print()

    print_success("All field types created successfully!")
    console.print()

    return fields


def demonstrate_tuple_creation_and_manipulation(complex_desc, fields):
    """Demonstrate tuple creation and field manipulation"""
    print_step(3, "Creating and Manipulating Tuples",
               "Tuples are rows of data that conform to a schema")

    # Create tuple
    print_info("Creating a new tuple with complex schema...")
    tuple_obj = Tuple(complex_desc)
    console.print(f"Initial tuple: {tuple_obj}")
    console.print(f"Is complete: {tuple_obj.is_complete()}")
    console.print()

    # Set fields one by one
    print_info("Setting fields in the tuple...")
    tuple_obj.set_field(0, fields['int'])  # id
    console.print(f"After setting field 0 (id): {tuple_obj}")

    tuple_obj.set_field(1, fields['string'])  # name
    console.print(f"After setting field 1 (name): {tuple_obj}")

    tuple_obj.set_field(2, fields['bool'])  # active
    console.print(f"After setting field 2 (active): {tuple_obj}")

    tuple_obj.set_field(3, fields['float'])  # score
    console.print(f"After setting field 3 (score): {tuple_obj}")

    tuple_obj.set_field(4, fields['double'])  # average
    console.print(f"After setting field 4 (average): {tuple_obj}")
    console.print()

    console.print(f"Is complete now: {tuple_obj.is_complete()}")
    print_success("Tuple created and populated successfully!")
    console.print()

    # Display tuple in table format
    table = Table(title="Complete Tuple", box=box.ROUNDED)
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="green")
    table.add_column("Type", style="magenta")

    for i in range(complex_desc.num_fields()):
        field_name = complex_desc.get_field_name(i)
        field_value = tuple_obj.get_field(i)
        field_type = field_value.get_type().value
        table.add_row(field_name, str(field_value.get_value()), field_type)

    console.print(table)
    console.print()

    return tuple_obj


def demonstrate_record_id():
    """Demonstrate RecordId creation and usage"""
    print_step(4, "Working with Record IDs",
               "RecordIds track the physical location of tuples in storage")

    print_info("Creating a HeapPageId...")
    page_id = HeapPageId(table_id=1, page_number=42)
    console.print(f"Page ID: {page_id}")
    console.print()

    print_info("Creating RecordId...")
    record_id = RecordId(page_id, tuple_number=5)
    console.print(f"Record ID: {record_id}")
    console.print(f"Page ID: {record_id.get_page_id()}")
    console.print(f"Tuple number: {record_id.get_tuple_number()}")
    console.print()

    print_success("RecordId created successfully!")
    return record_id


def demonstrate_tuple_with_record_id(tuple_obj, record_id):
    """Demonstrate assigning RecordId to tuple"""
    print_step(5, "Assigning Record ID to Tuple",
               "Linking logical tuple with its physical storage location")

    print_info("Assigning RecordId to tuple...")
    tuple_obj.set_record_id(record_id)
    console.print(f"Tuple with Record ID: {tuple_obj}")
    console.print(f"Record ID: {tuple_obj.get_record_id()}")
    print_success("RecordId assigned successfully!")
    console.print()


def demonstrate_serialization_deserialization(tuple_obj, complex_desc):
    """Demonstrate tuple serialization and deserialization"""
    print_step(6, "Serialization & Deserialization",
               "Converting tuples to/from bytes for storage")

    print_info("Serializing tuple to bytes...")
    serialized_data = tuple_obj.serialize()
    console.print(f"Serialized size: {len(serialized_data)} bytes")
    console.print(f"Serialized data (hex): {serialized_data.hex()[:64]}...")
    console.print()

    print_info("Deserializing tuple from bytes...")
    deserialized_tuple = Tuple.deserialize(serialized_data, complex_desc)
    console.print(f"Deserialized tuple: {deserialized_tuple}")
    console.print()

    # Verify equality
    print_info("Verifying data integrity...")
    if tuple_obj == deserialized_tuple:
        print_success("✓ Original and deserialized tuples are equal!")
    else:
        print_error("✗ Data integrity check failed!")

    console.print()
    return deserialized_tuple


def demonstrate_tuple_combination(complex_desc):
    """Demonstrate tuple combination (for joins)"""
    print_step(7, "Tuple Combination",
               "Combining tuples for join operations")

    # Create two tuples to combine
    print_info("Creating first tuple (Person)...")
    person_desc = TupleDesc([FieldType.INT, FieldType.STRING], [
                            "person_id", "name"])
    person_tuple = Tuple(person_desc)
    person_tuple.set_field(0, IntField(1))
    person_tuple.set_field(1, StringField("Alice"))
    console.print(f"Person tuple: {person_tuple}")
    console.print()

    print_info("Creating second tuple (Score)...")
    score_desc = TupleDesc([FieldType.INT, FieldType.FLOAT], [
                           "person_id", "score"])
    score_tuple = Tuple(score_desc)
    score_tuple.set_field(0, IntField(1))
    score_tuple.set_field(1, FloatField(95.5))
    console.print(f"Score tuple: {score_tuple}")
    console.print()

    print_info("Combining tuples...")
    combined_tuple = Tuple.combine(person_tuple, score_tuple)
    console.print(f"Combined tuple: {combined_tuple}")
    console.print(f"Combined schema: {combined_tuple.get_tuple_desc()}")
    console.print()

    # Display combined tuple in table
    table = Table(title="Combined Tuple (Join Result)", box=box.ROUNDED)
    table.add_column("Field Index", style="cyan", justify="right")
    table.add_column("Value", style="green")
    table.add_column("Type", style="magenta")
    table.add_column("Source", style="yellow")

    combined_desc = combined_tuple.get_tuple_desc()
    for i in range(combined_desc.num_fields()):
        field_value = combined_tuple.get_field(i)
        source = "Person" if i < person_desc.num_fields() else "Score"
        table.add_row(
            str(i),
            str(field_value.get_value()),
            field_value.get_type().value,
            source
        )

    console.print(table)
    print_success("Tuples combined successfully for join operation!")
    console.print()


def demonstrate_error_handling():
    """Demonstrate error handling and validation"""
    print_step(8, "Error Handling & Validation",
               "Showing how the system handles invalid operations")

    # Invalid field index
    print_info("Testing invalid field index access...")
    try:
        desc = TupleDesc([FieldType.INT])
        tuple_obj = Tuple(desc)
        tuple_obj.get_field(5)  # Invalid index
    except IndexError as e:
        print_warning(f"Caught expected error: {e}")
    console.print()

    # Type mismatch
    print_info("Testing field type mismatch...")
    try:
        desc = TupleDesc([FieldType.INT])
        tuple_obj = Tuple(desc)
        tuple_obj.set_field(0, StringField("wrong type"))  # Wrong type
    except TypeError as e:
        print_warning(f"Caught expected error: {e}")
    console.print()

    # Incomplete tuple serialization
    print_info("Testing incomplete tuple serialization...")
    try:
        desc = TupleDesc([FieldType.INT, FieldType.STRING])
        tuple_obj = Tuple(desc)
        tuple_obj.set_field(0, IntField(42))  # Only set one field
        tuple_obj.serialize()  # Should fail
    except ValueError as e:
        print_warning(f"Caught expected error: {e}")
    console.print()

    print_success("Error handling demonstration complete!")
    console.print()


def demonstrate_advanced_features():
    """Demonstrate advanced tuple features"""
    print_step(9, "Advanced Features",
               "Exploring advanced tuple functionality")

    # Field name lookup
    print_info("Testing field name lookup...")
    desc = TupleDesc([FieldType.INT, FieldType.STRING], ["id", "name"])
    console.print(f"Index of 'name' field: {desc.name_to_index('name')}")
    console.print(f"Index of 'id' field: {desc.name_to_index('id')}")
    console.print()

    # Qualified field names (table.field)
    print_info("Testing qualified field names...")
    try:
        index = desc.name_to_index("users.name")  # Qualified name
        console.print(f"Index of 'users.name' field: {index}")
    except ValueError as e:
        console.print(f"Field not found: {e}")
    console.print()

    # Tuple equality
    print_info("Testing tuple equality...")
    tuple1 = Tuple(desc)
    tuple1.set_field(0, IntField(1))
    tuple1.set_field(1, StringField("test"))

    tuple2 = Tuple(desc)
    tuple2.set_field(0, IntField(1))
    tuple2.set_field(1, StringField("test"))

    tuple3 = Tuple(desc)
    tuple3.set_field(0, IntField(2))
    tuple3.set_field(1, StringField("different"))

    console.print(f"tuple1 == tuple2: {tuple1 == tuple2}")
    console.print(f"tuple1 == tuple3: {tuple1 == tuple3}")
    console.print()

    # Tuple hashing (for sets/dicts)
    print_info("Testing tuple hashing...")
    tuple_set = {tuple1, tuple2, tuple3}
    console.print(f"Unique tuples in set: {len(tuple_set)}")
    console.print(f"tuple1 hash: {hash(tuple1)}")
    console.print(f"tuple2 hash: {hash(tuple2)}")
    console.print()

    print_success("Advanced features demonstration complete!")
    console.print()


def main():
    """Main demonstration function"""
    # Print beautiful header
    print_header(
        "StoreMy Database - Tuple System Demonstration",
        "A comprehensive walkthrough of tuple functionality"
    )
    console.print()

    console.print(Panel(
        "[bold cyan]Welcome to the Interactive Tuple Demonstration![/bold cyan]\n\n" +
        "This demonstration will walk you through 9 comprehensive steps:\n" +
        "1. Creating Tuple Descriptors (Schemas)\n" +
        "2. Creating Fields\n" +
        "3. Creating and Manipulating Tuples\n" +
        "4. Working with Record IDs\n" +
        "5. Assigning Record ID to Tuple\n" +
        "6. Serialization & Deserialization\n" +
        "7. Tuple Combination\n" +
        "8. Error Handling & Validation\n" +
        "9. Advanced Features\n\n" +
        "[dim]You can skip any step by entering 'n', or quit anytime with 'q'.[/dim]",
        style="bright_cyan",
        box=box.ROUNDED,
        title="📋 Overview",
        padding=(1, 2)
    ))

    try:
        # Step 1: Tuple Descriptors
        simple_desc, complex_desc = demonstrate_tuple_desc()

        if not ask_continue("Field Creation step"):
            fields = {}  # Empty fields for skipped step
        else:
            console.print(
                Rule("[bold blue]Moving to Field Creation[/bold blue]"))
            console.print()
            # Step 2: Field Creation
            fields = demonstrate_field_creation()

        if not ask_continue("Tuple Operations step"):
            # Create minimal tuple for later steps
            tuple_obj = Tuple(complex_desc)
            for i in range(complex_desc.num_fields()):
                if complex_desc.get_field_type(i) == FieldType.INT:
                    tuple_obj.set_field(i, IntField(0))
                elif complex_desc.get_field_type(i) == FieldType.STRING:
                    tuple_obj.set_field(i, StringField(""))
                elif complex_desc.get_field_type(i) == FieldType.BOOLEAN:
                    tuple_obj.set_field(i, BoolField(False))
                elif complex_desc.get_field_type(i) == FieldType.FLOAT:
                    tuple_obj.set_field(i, FloatField(0.0))
                elif complex_desc.get_field_type(i) == FieldType.DOUBLE:
                    tuple_obj.set_field(i, DoubleField(0.0))
        else:
            console.print(
                Rule("[bold blue]Moving to Tuple Operations[/bold blue]"))
            console.print()
            # Step 3: Tuple Creation
            tuple_obj = demonstrate_tuple_creation_and_manipulation(
                complex_desc, fields)

        if not ask_continue("Record ID Operations step"):
            record_id = RecordId(HeapPageId(1, 1), 1)  # Default record ID
        else:
            console.print(
                Rule("[bold blue]Moving to Record ID Operations[/bold blue]"))
            console.print()
            # Step 4: Record IDs
            record_id = demonstrate_record_id()

        if ask_continue("Tuple-Storage linking step"):
            console.print(
                Rule("[bold blue]Linking Tuple with Storage[/bold blue]"))
            console.print()
            # Step 5: Tuple with Record ID
            demonstrate_tuple_with_record_id(tuple_obj, record_id)

        if ask_continue("Persistence testing step"):
            console.print(Rule("[bold blue]Testing Persistence[/bold blue]"))
            console.print()
            # Step 6: Serialization/Deserialization
            deserialized_tuple = demonstrate_serialization_deserialization(
                tuple_obj, complex_desc)

        if ask_continue("Join Operations step"):
            console.print(
                Rule("[bold blue]Testing Join Operations[/bold blue]"))
            console.print()
            # Step 7: Tuple Combination
            demonstrate_tuple_combination(complex_desc)

        if ask_continue("Error Handling step"):
            console.print(
                Rule("[bold blue]Testing Error Conditions[/bold blue]"))
            console.print()
            # Step 8: Error Handling
            demonstrate_error_handling()

        if ask_continue("Advanced Features step"):
            console.print(
                Rule("[bold blue]Exploring Advanced Features[/bold blue]"))
            console.print()
            # Step 9: Advanced Features
            demonstrate_advanced_features()

        # Final summary
        console.print(Rule("[bold green]Demonstration Complete![/bold green]"))
        console.print()

        summary_panel = Panel(
            "[bold green]🎉 All tuple operations completed successfully![/bold green]\n\n" +
            "You've seen:\n" +
            "• Tuple schema definition and validation\n" +
            "• All supported field types (INT, STRING, BOOLEAN, FLOAT, DOUBLE)\n" +
            "• Tuple creation and field manipulation\n" +
            "• Physical location tracking with RecordIds\n" +
            "• Serialization for persistent storage\n" +
            "• Tuple combination for join operations\n" +
            "• Comprehensive error handling\n" +
            "• Advanced features like field lookup and hashing\n\n" +
            "[dim]The tuple system is the foundation of the StoreMy database engine![/dim]",
            style="bright_green",
            box=box.DOUBLE,
            title="Summary",
            padding=(1, 2)
        )
        console.print(summary_panel)

        # Final user interaction
        console.print()
        console.print(
            Rule("[dim]Thank you for exploring StoreMy tuples![/dim]"))
        console.print()

        try:
            input(
                "Press [bold green]Enter[/bold green] to exit the demonstration... ")
            console.print(
                "[bold green]👋[/bold green] Thank you for exploring the StoreMy tuple system!")
        except (KeyboardInterrupt, EOFError):
            console.print("\n[bold green]👋[/bold green] Goodbye!")

    except Exception as e:
        console.print(
            f"\n[bold red]An unexpected error occurred: {e}[/bold red]")
        console.print(
            "[dim]This might indicate a problem with the tuple system.[/dim]")
        raise


if __name__ == "__main__":
    main()
