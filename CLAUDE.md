# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Essential Development Commands

### Running the Application
```bash
# Install dependencies using uv
uv pip install -r pyproject.toml

# Run the main application
python app/hajimi_king.py

# Create required data directory
mkdir -p data
```

### Environment Setup
```bash
# Copy configuration templates
cp env.example .env
cp queries.example queries.txt

# Edit .env file to configure GitHub tokens (required)
# Edit queries.txt to customize search patterns
```

### Code Quality & Linting
```bash
# Install development dependencies (includes ruff, pylint, pre-commit)
uv sync --group dev

# Install pre-commit hooks (run once)
uv run pre-commit install

# Manual linting (fix issues automatically)
uv run ruff check . --fix
uv run ruff format .

# Run all pre-commit checks manually
uv run pre-commit run --all-files

# Quick lint script
./scripts/lint.sh
```

### Docker Commands
```bash
# Build and run with Docker Compose
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

## Project Architecture

### Core Components

**Main Application (`app/hajimi_king.py`)**
- Entry point that orchestrates the entire key scanning process
- Implements the main scanning loop with GitHub API search
- Handles key validation using Google Gemini API
- Manages incremental scanning with checkpoint system
- Coordinates with external sync services

**Configuration Management (`common/config.py`)**
- Centralizes all environment variable parsing and validation
- Supports proxy rotation and multi-token GitHub authentication
- Manages file path prefixes and filtering rules
- Handles boolean parsing for various feature flags

**GitHub Integration (`utils/github_client.py`)**
- Implements GitHub Code Search API with token rotation
- Handles rate limiting and retry logic with exponential backoff
- Downloads file content via both base64 and direct URL methods
- Supports proxy rotation for API requests

**File Management (`utils/file_manager.py`)**
- Manages output file organization with timestamped filenames
- Implements checkpoint system for incremental scanning
- Handles different file types: valid keys, rate-limited keys, logs
- Saves search queries and maintains scan history

**Sync Utilities (`utils/sync_utils.py`)**
- Integrates with external services (Gemini-Balancer, GPT-Load)
- Manages async key synchronization queues
- Handles authentication for external API endpoints

### Key Workflows

**Search Process:**
1. Load search queries from `queries.txt`
2. Normalize queries and check against processed history
3. Execute GitHub code search with pagination
4. Filter results based on file age, path blacklist, and duplicates
5. Download file contents and extract API keys using regex
6. Validate each key against Google Gemini API
7. Save valid/rate-limited keys to categorized files
8. Update checkpoint data for incremental processing

**Validation Logic:**
- Uses configurable Gemini model (default: gemini-2.5-flash)
- Implements random delays to avoid rate limiting
- Categorizes keys as: valid, rate-limited, disabled, or invalid
- Supports proxy rotation during validation

**Data Management:**
- Organizes output in `data/` directory with subdirectories for different file types
- Implements dynamic filename generation with timestamps
- Maintains scan history to avoid reprocessing files
- Supports resume functionality through checkpoint files

### Configuration Patterns

The application uses environment variables extensively with fallback defaults. Critical settings include:
- `GITHUB_TOKENS`: Required comma-separated list of GitHub API tokens
- `PROXY`: Optional comma-separated proxy URLs for API requests
- `QUERIES_FILE`: Path to search query definitions
- `DATE_RANGE_DAYS`: Age filter for repositories (default 730 days)
- `FILE_PATH_BLACKLIST`: Patterns to skip documentation/example files

External sync services are optional and controlled by feature flags:
- Gemini-Balancer integration for key management
- GPT-Load integration for load balancing

### Security Considerations

This tool is designed for defensive security research to identify exposed API keys. It implements:
- Filtering logic to avoid processing documentation and example files
- Configurable delays to respect API rate limits
- Token rotation to distribute API usage across multiple GitHub tokens
- Proxy support to avoid IP-based restrictions
