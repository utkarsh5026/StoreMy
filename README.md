# App

A Python project created with Poetry.

## Installation

1. Make sure you have Poetry installed
2. Clone this repository
3. Install dependencies:
   ```bash
   poetry install
   ```

## Usage

Run the application:
```bash
poetry run app
```

Or activate the virtual environment and run:
```bash
poetry shell
python -m app.main
```

## Development

Run tests:
```bash
poetry run pytest
```

Format code:
```bash
poetry run black .
```

Lint code:
```bash
poetry run flake8 .
```

Type check:
```bash
poetry run mypy .
```

## Project Structure

```
├── app/                 # Main application package
│   ├── __init__.py
│   └── main.py         # Main application entry point
├── tests/              # Test files
│   ├── __init__.py
│   └── test_main.py
├── pyproject.toml      # Poetry configuration
└── README.md          # This file
``` 