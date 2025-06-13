#!/usr/bin/env python3
"""
Documentation build script for StoreMy project.
This script builds the Sphinx documentation and outputs it to docs/_build/html/
"""

import os
import sys
import shutil
import subprocess
from pathlib import Path


def main():
    """Main function to build documentation."""
    print("Building StoreMy documentation...")

    # Get the docs directory (where this script is located)
    docs_dir = Path(__file__).parent.absolute()
    project_root = docs_dir.parent

    # Change to docs directory
    os.chdir(docs_dir)

    # Clean previous builds
    build_dir = docs_dir / "_build"
    if build_dir.exists():
        print("Cleaning previous build...")
        shutil.rmtree(build_dir)

    # Create _static directory if it doesn't exist
    static_dir = docs_dir / "_static"
    static_dir.mkdir(exist_ok=True)

    # Build the documentation
    print("Building HTML documentation...")
    try:
        result = subprocess.run([
            sys.executable, "-m", "sphinx",
            "-b", "html",
            ".",  # source directory
            "_build/html"  # output directory
        ], check=True, capture_output=True, text=True)

        print("Documentation built successfully!")
        print(f"Output directory: {docs_dir / '_build' / 'html'}")
        print(
            f"Open {docs_dir / '_build' / 'html' / 'index.html'} in your browser to view the documentation.")

    except subprocess.CalledProcessError as e:
        print(f"Error building documentation: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        sys.exit(1)
    except FileNotFoundError:
        print("Error: Sphinx not found. Make sure you have installed the dev dependencies:")
        print("poetry install --with dev")
        sys.exit(1)


if __name__ == "__main__":
    main()
