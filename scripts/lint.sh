#!/bin/bash
# Linting script for hajimi-king project
# Run this before committing to catch issues early

echo "🔍 Running linting checks..."

# Run ruff linter with auto-fix
echo "📋 Running ruff linter..."
uv run ruff check . --fix

# Run ruff formatter
echo "✨ Running ruff formatter..."
uv run ruff format .

# Run pre-commit checks (this will auto-fix trailing whitespace, etc.)
echo "🔧 Running pre-commit checks..."
uv run pre-commit run --all-files

echo "✅ Linting complete!"
