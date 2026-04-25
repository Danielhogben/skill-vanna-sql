# Vanna SQL

Natural language to SQL conversion using Vanna.ai. Train on your database schema and documentation, then ask questions in plain English to get production-ready SQL queries.

## What It Does

- **Train** -- Feed DDL statements, documentation, and example SQL queries to build a knowledge base
- **Ask** -- Ask natural language questions and get SQL queries back with explanations
- **Connect** -- Connect to PostgreSQL, MySQL, or SQLite databases for live query execution
- **Run** -- Generate SQL and execute it against a connected database in one step
- **History** -- Track all generated queries with their natural language prompts
- **Optimize** -- Analyze generated queries and suggest indexes, rewrites, and performance improvements

## Usage

```bash
# Train on your schema
python3 vanna_sql.py train --ddl "CREATE TABLE users (id INT, name TEXT, email TEXT)"
python3 vanna_sql.py train --doc "Users table stores customer accounts with email as unique identifier"
python3 vanna_sql.py train --sql "SELECT COUNT(*) FROM users WHERE created_at > '2024-01-01'"

# Ask a question
python3 vanna_sql.py ask "How many users signed up last month?"

# Connect to a database
python3 vanna_sql.py connect --db-type sqlite --database mydb.sqlite
python3 vanna_sql.py connect --db-type postgres --host localhost --database mydb --user postgres

# Generate and execute
python3 vanna_sql.py run "Show me the top 10 customers by total order value"

# View query history
python3 vanna_sql.py history

# Optimize a query
python3 vanna_sql.py optimize --sql "SELECT * FROM orders JOIN customers ON ..."
```

## Requirements

- Python 3.10+
- `pip install vanna` (core library)
- Database drivers as needed: `pip install psycopg2-binary mysql-connector-python`

## Training Data Storage

Training data (DDL, documentation, SQL examples) is stored in `~/.vanna/training/` as JSON. Query history is stored in `~/.vanna/history.json`.
