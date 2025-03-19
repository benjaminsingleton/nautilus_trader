# NautilusTrader Commands and Style Guide

## Build, Lint, and Test Commands
- **Install dependencies:** `make install-debug`
- **Build project:** `make build` (release) or `make build-debug` (debug)
- **Run all tests:** `make pytest` or `poetry run pytest`
- **Run single test:** `poetry run pytest tests/path/to/test_file.py::TestClass::test_function -v`
- **Lint code:** `make ruff` or `ruff check . --fix`
- **Format code:** `make format` (Rust) and `black .` (Python)
- **Run pre-commit checks:** `make pre-commit`
- **Clean build artifacts:** `make clean`

## Code Style Guidelines
- **Formatting:** Follow Black for Python and cargo fmt for Rust
- **Line length:** 100 characters
- **Imports:** Single line per import, trailing commas for multi-line arguments
- **Types:** Use static typing, explicit returns in functions
- **Docstrings:** Follow NumPy docstring spec
- **Comments:** Keep comments to a minimum
- **Naming:** PEP-8 for Python (snake_case for variables/functions, PascalCase for classes)
- **Error handling:** Use specific exceptions with contextual messages, prefer `is None` over truthiness
- **Structure:** Keep functions small and focused, prefer composition over inheritance
- **Tests:** Write comprehensive unit tests, include edge cases
