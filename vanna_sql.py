#!/usr/bin/env python3
"""Vanna SQL — Natural language to SQL conversion."""

import asyncio
import json
import os
import re
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

G = "\033[92m"
R = "\033[91m"
Y = "\033[93m"
C = "\033[96m"
W = "\033[0m"
BOLD = "\033[1m"

SKILL_DIR = Path(__file__).parent
DATA_DIR = Path.home() / ".vanna"
TRAINING_DIR = DATA_DIR / "training"
HISTORY_FILE = DATA_DIR / "history.json"
DB_STATE_FILE = DATA_DIR / "connection.json"


def _ensure_dirs():
    TRAINING_DIR.mkdir(parents=True, exist_ok=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_training() -> dict:
    _ensure_dirs()
    ddl_file = TRAINING_DIR / "ddl.json"
    doc_file = TRAINING_DIR / "docs.json"
    sql_file = TRAINING_DIR / "sql_examples.json"
    return {
        "ddl": json.loads(ddl_file.read_text()) if ddl_file.exists() else [],
        "docs": json.loads(doc_file.read_text()) if doc_file.exists() else [],
        "sql": json.loads(sql_file.read_text()) if sql_file.exists() else [],
    }


def _save_training(training: dict):
    _ensure_dirs()
    (TRAINING_DIR / "ddl.json").write_text(json.dumps(training["ddl"], indent=2))
    (TRAINING_DIR / "docs.json").write_text(json.dumps(training["docs"], indent=2))
    (TRAINING_DIR / "sql_examples.json").write_text(json.dumps(training["sql"], indent=2))


def _load_history() -> list:
    if HISTORY_FILE.exists():
        return json.loads(HISTORY_FILE.read_text())
    return []


def _save_history(history: list):
    _ensure_dirs()
    HISTORY_FILE.write_text(json.dumps(history, indent=2))


def _load_db_connection() -> dict | None:
    if DB_STATE_FILE.exists():
        return json.loads(DB_STATE_FILE.read_text())
    return None


def _save_db_connection(conn_info: dict):
    _ensure_dirs()
    DB_STATE_FILE.write_text(json.dumps(conn_info, indent=2))


def _parse_ddl(ddl: str) -> dict:
    """Parse a CREATE TABLE statement to extract schema info."""
    tables = {}
    # Match CREATE TABLE blocks
    for match in re.finditer(
        r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`"\[]?(\w+)[`"\]]?\s*\((.*?)\)',
        ddl, re.IGNORECASE | re.DOTALL
    ):
        table_name = match.group(1).lower()
        columns_str = match.group(2)
        columns = []
        for col_line in columns_str.split(","):
            col_line = col_line.strip()
            col_match = re.match(r'[`"\[]?(\w+)[`"\]]?\s+(\w+)', col_line)
            if col_match:
                col_name = col_match.group(1).lower()
                col_type = col_match.group(2).upper()
                constraints = col_line[col_match.end():].strip()
                columns.append({
                    "name": col_name,
                    "type": col_type,
                    "constraints": constraints,
                    "primary_key": "PRIMARY KEY" in constraints.upper(),
                    "foreign_key": bool(re.search(r'REFERENCES\s+(\w+)', constraints, re.IGNORECASE)),
                })
        tables[table_name] = columns
    return tables


def _generate_sql_from_nl(question: str, training: dict) -> tuple[str, str]:
    """Generate SQL from natural language using pattern matching and training data."""
    question_lower = question.lower()

    # Parse all DDL
    all_tables = {}
    for ddl_entry in training["ddl"]:
        tables = _parse_ddl(ddl_entry["text"])
        all_tables.update(tables)

    # Build table/column index
    table_names = list(all_tables.keys())
    all_columns = {}
    for tname, cols in all_tables.items():
        for c in cols:
            all_columns[c["name"]] = tname

    # Find which tables are relevant
    relevant_tables = set()
    for tname in table_names:
        if tname in question_lower or tname.rstrip("s") in question_lower or tname + "s" in question_lower:
            relevant_tables.add(tname)

    # Check documentation for table references
    for doc in training["docs"]:
        doc_lower = doc["text"].lower()
        for tname in table_names:
            if tname in doc_lower and any(word in question_lower for word in doc_lower.split()[:10]):
                relevant_tables.add(tname)

    # Detect query intent
    is_count = any(w in question_lower for w in ["how many", "count", "number of", "total number"])
    is_aggregate = any(w in question_lower for w in ["sum", "average", "avg", "max", "min", "total"])
    is_top_n = re.search(r'top\s+(\d+)', question_lower)
    is_order = any(w in question_lower for w in ["latest", "recent", "oldest", "highest", "lowest", "most", "least"])
    is_filter = any(w in question_lower for w in ["where", "filter", "only", "with", "without", "having"])
    is_join = len(relevant_tables) > 1

    # Find matching example SQL
    best_example = None
    best_score = 0
    for ex in training["sql"]:
        ex_lower = ex["text"].lower()
        score = 0
        for word in question_lower.split():
            if len(word) > 3 and word in ex_lower:
                score += 1
        for tname in relevant_tables:
            if tname in ex_lower:
                score += 2
        if score > best_score:
            best_score = score
            best_example = ex

    if best_example and best_score >= 3:
        return best_example["text"], f"Based on similar training example (score: {best_score})"

    # Generate SQL from schema knowledge
    if not relevant_tables and table_names:
        # Fallback: use all tables if nothing matched
        relevant_tables = set(table_names[:2])

    if not relevant_tables:
        return ("-- Could not determine which tables to query\n-- Train on DDL first: python3 vanna_sql.py train --ddl 'CREATE TABLE ...'",
                "No matching tables found in training data")

    table = list(relevant_tables)[0]
    columns = all_tables.get(table, [])
    col_names = [c["name"] for c in columns]

    # Build query
    if is_count and not is_aggregate:
        if len(relevant_tables) > 1:
            t1, t2 = list(relevant_tables)[:2]
            fk_col = None
            for c in all_tables.get(t2, []):
                if c.get("foreign_key") or f"{t1}_id" in c["name"]:
                    fk_col = c["name"]
                    break
            if fk_col:
                sql = f"SELECT COUNT(*) AS count\nFROM {t1} t1\nJOIN {t2} t2 ON t1.id = t2.{fk_col}"
            else:
                sql = f"SELECT COUNT(*) AS count\nFROM {table}"
        else:
            sql = f"SELECT COUNT(*) AS count\nFROM {table}"
        explanation = "Counting rows"

    elif is_aggregate:
        agg_func = "SUM"
        for f_name in ["AVG", "MAX", "MIN"]:
            if f_name.lower() in question_lower:
                agg_func = f_name
                break
        # Find the column to aggregate
        agg_col = None
        for c in col_names:
            if c in question_lower:
                agg_col = c
                break
        if not agg_col:
            numeric_cols = [c["name"] for c in columns if any(t in c["type"] for t in ["INT", "FLOAT", "DECIMAL", "NUMERIC", "DOUBLE", "REAL"])]
            agg_col = numeric_cols[0] if numeric_cols else (col_names[0] if col_names else "*")
        sql = f"SELECT {agg_func}({agg_col}) AS result\nFROM {table}"
        explanation = f"Computing {agg_func} of {agg_col}"

    elif is_top_n:
        n = int(is_top_n.group(1))
        order_col = col_names[0] if col_names else "id"
        for c in col_names:
            if c in question_lower:
                order_col = c
                break
        sql = f"SELECT *\nFROM {table}\nORDER BY {order_col} DESC\nLIMIT {n}"
        explanation = f"Getting top {n} rows ordered by {order_col}"

    elif is_order:
        order_col = "created_at"
        for c in ["created_at", "date", "timestamp", "updated_at"]:
            if c in col_names:
                order_col = c
                break
        else:
            order_col = col_names[0] if col_names else "id"
        direction = "DESC" if any(w in question_lower for w in ["latest", "recent", "highest", "most"]) else "ASC"
        sql = f"SELECT *\nFROM {table}\nORDER BY {order_col} {direction}"
        explanation = f"Ordering by {order_col} ({direction})"

    else:
        # Basic select
        if len(relevant_tables) > 1:
            tables_list = list(relevant_tables)
            sql = f"SELECT *\nFROM {tables_list[0]}"
            for t in tables_list[1:]:
                sql += f"\nJOIN {t} ON {tables_list[0]}.id = {t}.{tables_list[0]}_id"
        else:
            sql = f"SELECT {', '.join(col_names[:8]) if col_names else '*'}\nFROM {table}"
        explanation = f"Selecting from {table}"

    # Add WHERE clause hints from training docs
    for doc in training["docs"]:
        if "unique" in doc["text"].lower():
            for c in columns:
                if c["name"] in doc["text"].lower() and c["name"] in question_lower:
                    sql += f"\nWHERE {c['name']} = ?"
                    explanation += f" (filtered by unique {c['name']})"
                    break

    return sql, explanation


def _suggest_optimizations(sql: str, training: dict) -> list[str]:
    """Suggest query optimizations."""
    suggestions = []
    sql_lower = sql.lower()

    # SELECT *
    if "select *" in sql_lower:
        suggestions.append("Avoid SELECT * — specify only needed columns to reduce I/O and memory usage")

    # Missing WHERE on large tables
    if "where" not in sql_lower and any(w in sql_lower for w in ["join", "order by"]):
        suggestions.append("Consider adding a WHERE clause to filter rows before joins/sorting")

    # LIKE with leading wildcard
    if re.search(r"LIKE\s+'%", sql_lower):
        suggestions.append("Leading wildcard LIKE patterns cannot use indexes — consider full-text search or trigram indexes")

    # Subquery in WHERE
    if re.search(r'WHERE\s+.*\(\s*SELECT', sql, re.IGNORECASE):
        suggestions.append("Correlated subqueries in WHERE can be slow — consider rewriting as JOIN or EXISTS")

    # ORDER BY without index hint
    if "order by" in sql_lower and "limit" not in sql_lower:
        suggestions.append("ORDER BY without LIMIT may sort entire table — add LIMIT if only top rows are needed")

    # Multiple OR conditions
    or_count = sql_lower.count(" or ")
    if or_count > 3:
        suggestions.append(f"Query has {or_count} OR conditions — consider using IN() or rewriting as UNION ALL")

    # No DISTINCT removal
    if "distinct" in sql_lower:
        suggestions.append("DISTINCT can be expensive — ensure it is necessary and not masking a join issue")

    # Check for date functions on columns
    if re.search(r'(YEAR|MONTH|DATE)\(\s*\w+\.\w+\s*\)', sql):
        suggestions.append("Applying functions to columns prevents index usage — consider range comparisons instead")

    # Parse DDL to suggest indexes
    all_tables = {}
    for ddl_entry in training["ddl"]:
        all_tables.update(_parse_ddl(ddl_entry["text"]))

    for tname, cols in all_tables.items():
        if tname in sql_lower:
            for c in cols:
                if c.get("primary_key") and c["name"] in sql_lower:
                    break
            else:
                # Suggest index on columns used in WHERE/JOIN
                where_match = re.search(r'WHERE\s+(\w+)\.(\w+)', sql, re.IGNORECASE)
                if where_match:
                    suggestions.append(f"Consider adding an index on {where_match.group(2)} for faster filtering")

    if not suggestions:
        suggestions.append("Query looks good — no obvious optimization issues detected")

    return suggestions


# ── commands ──────────────────────────────────────────────────────────

async def cmd_train(args: list[str]):
    """Train on SQL queries, documentation, DDL."""
    _ensure_dirs()
    training = _load_training()
    i = 0
    while i < len(args):
        if args[i] == "--ddl" and i + 1 < len(args):
            text = args[i + 1]
            training["ddl"].append({"id": str(uuid.uuid4())[:8], "text": text, "added": _now_iso()})
            tables = _parse_ddl(text)
            print(f"{G}Added DDL:{W} {len(tables)} table(s) parsed — {', '.join(tables.keys())}")
            i += 2
        elif args[i] == "--doc" and i + 1 < len(args):
            text = args[i + 1]
            training["docs"].append({"id": str(uuid.uuid4())[:8], "text": text, "added": _now_iso()})
            print(f"{G}Added documentation{W} (total: {len(training['docs'])})")
            i += 2
        elif args[i] == "--sql" and i + 1 < len(args):
            text = args[i + 1]
            training["sql"].append({"id": str(uuid.uuid4())[:8], "text": text, "added": _now_iso()})
            print(f"{G}Added SQL example{W} (total: {len(training['sql'])})")
            i += 2
        elif args[i] == "--file" and i + 1 < len(args):
            filepath = Path(args[i + 1])
            if not filepath.exists():
                print(f"{R}File not found: {filepath}{W}")
                i += 2
                continue
            content = filepath.read_text()
            if filepath.suffix == ".sql":
                training["ddl"].append({"id": str(uuid.uuid4())[:8], "text": content, "added": _now_iso()})
                print(f"{G}Loaded SQL file:{W} {filepath.name}")
            else:
                training["docs"].append({"id": str(uuid.uuid4())[:8], "text": content, "added": _now_iso()})
                print(f"{G}Loaded doc file:{W} {filepath.name}")
            i += 2
        elif args[i] == "--show":
            _show_training(training)
            return
        elif args[i] == "--clear":
            training = {"ddl": [], "docs": [], "sql": []}
            _save_training(training)
            print(f"{Y}Training data cleared.{W}")
            return
        else:
            i += 1

    _save_training(training)
    total = len(training["ddl"]) + len(training["docs"]) + len(training["sql"])
    print(f"\n{BOLD}Training Summary:{W} {total} entries (DDL: {len(training['ddl'])}, Docs: {len(training['docs'])}, SQL: {len(training['sql'])})")


def _show_training(training: dict):
    print(f"\n{BOLD}Training Data{W}")
    if training["ddl"]:
        print(f"\n  {C}DDL ({len(training['ddl'])} entries):{W}")
        for d in training["ddl"]:
            tables = _parse_ddl(d["text"])
            print(f"    [{d['id']}] {d['added'][:10]} — {', '.join(tables.keys()) or 'parsed'}")
    if training["docs"]:
        print(f"\n  {C}Documentation ({len(training['docs'])} entries):{W}")
        for d in training["docs"]:
            print(f"    [{d['id']}] {d['added'][:10]} — {d['text'][:60]}...")
    if training["sql"]:
        print(f"\n  {C}SQL Examples ({len(training['sql'])} entries):{W}")
        for d in training["sql"]:
            print(f"    [{d['id']}] {d['added'][:10]} — {d['text'][:60]}...")
    print()


async def cmd_ask(args: list[str]):
    """Ask a question in natural language, get SQL."""
    if not args:
        print(f"{R}Usage: ask <natural language question>{W}")
        return

    question = " ".join(args)
    training = _load_training()

    print(f"\n{BOLD}Question:{W} {question}\n")

    sql, explanation = _generate_sql_from_nl(question, training)

    print(f"{BOLD}Generated SQL:{W}")
    print(f"{C}{sql}{W}\n")
    print(f"{BOLD}Explanation:{W} {explanation}\n")

    # Save to history
    history = _load_history()
    history.append({
        "id": str(uuid.uuid4())[:8],
        "timestamp": _now_iso(),
        "question": question,
        "sql": sql,
        "explanation": explanation,
    })
    _save_history(history)

    # Auto-run if connected
    conn_info = _load_db_connection()
    if conn_info and conn_info.get("type") == "sqlite":
        db_path = conn_info.get("database")
        if db_path and Path(db_path).exists():
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                # Replace ? with sample values for execution
                exec_sql = sql.replace("?", "'example'")
                cursor.execute(exec_sql)
                rows = cursor.fetchall()
                cols = [d[0] for d in cursor.description] if cursor.description else []
                print(f"{BOLD}Results:{W}")
                if cols:
                    print(f"  {' | '.join(cols)}")
                    print(f"  {'─' * (len(cols) * 15)}")
                for row in rows[:20]:
                    print(f"  {' | '.join(str(v) for v in row)}")
                if len(rows) > 20:
                    print(f"  ... ({len(rows)} total rows)")
                conn.close()
            except Exception as e:
                print(f"{Y}Query execution skipped: {e}{W}")


async def cmd_connect(args: list[str]):
    """Connect to a database."""
    _ensure_dirs()
    db_type = host = port = database = user = password = None
    i = 0
    while i < len(args):
        if args[i] == "--db-type" and i + 1 < len(args):
            db_type = args[i + 1]; i += 2
        elif args[i] == "--host" and i + 1 < len(args):
            host = args[i + 1]; i += 2
        elif args[i] == "--port" and i + 1 < len(args):
            port = args[i + 1]; i += 2
        elif args[i] == "--database" and i + 1 < len(args):
            database = args[i + 1]; i += 2
        elif args[i] == "--user" and i + 1 < len(args):
            user = args[i + 1]; i += 2
        elif args[i] == "--password" and i + 1 < len(args):
            password = args[i + 1]; i += 2
        else:
            i += 1

    if not db_type or not database:
        print(f"{R}Usage: connect --db-type <sqlite|postgres|mysql> --database <name> [--host HOST] [--user USER]{W}")
        return

    conn_info = {
        "type": db_type,
        "database": database,
        "host": host or "localhost",
        "port": port or {"postgres": "5432", "mysql": "3306", "sqlite": ""}.get(db_type, ""),
        "user": user or "",
        "connected_at": _now_iso(),
    }

    if db_type == "sqlite":
        db_path = Path(database)
        if not db_path.exists():
            print(f"{Y}Warning: Database file does not exist yet: {db_path}{W}")
        else:
            try:
                conn = sqlite3.connect(str(db_path))
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [r[0] for r in cursor.fetchall()]
                conn.close()
                conn_info["tables"] = tables
                print(f"{G}Connected to SQLite: {database}{W}")
                print(f"  Tables: {', '.join(tables) if tables else '(empty)'}")
            except Exception as e:
                print(f"{R}Connection failed: {e}{W}")
                return
    else:
        print(f"{G}Connection configured for {db_type}://{host or 'localhost'}:{port or 'default'}/{database}{W}")
        print(f"  {Y}Note: Install driver ({('psycopg2-binary' if db_type == 'postgres' else 'mysql-connector-python')}) for live queries{W}")

    _save_db_connection(conn_info)
    print(f"{G}Connection saved.{W}")


async def cmd_run(args: list[str]):
    """Generate SQL and execute it."""
    if not args:
        print(f"{R}Usage: run <natural language question>{W}")
        return

    question = " ".join(args)
    training = _load_training()
    sql, explanation = _generate_sql_from_nl(question, training)

    print(f"\n{BOLD}Question:{W} {question}")
    print(f"{BOLD}SQL:{W} {C}{sql}{W}\n")

    conn_info = _load_db_connection()
    if not conn_info:
        print(f"{Y}No database connected. Run 'connect' first, then 'run' again.{W}")
        print(f"  Showing generated SQL only.")
        return

    if conn_info.get("type") == "sqlite":
        db_path = conn_info.get("database")
        if not db_path or not Path(db_path).exists():
            print(f"{R}Database not found: {db_path}{W}")
            return
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            exec_sql = sql.replace("?", "'example'")
            cursor.execute(exec_sql)
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description] if cursor.description else []

            print(f"{BOLD}Results ({len(rows)} rows):{W}")
            if cols:
                header = " | ".join(f"{c:<15}" for c in cols)
                print(f"  {C}{header}{W}")
                print(f"  {'─' * len(header)}")
            for row in rows[:50]:
                print(f"  {' | '.join(f'{str(v):<15}' for v in row)}")
            if len(rows) > 50:
                print(f"  ... ({len(rows)} total rows, showing first 50)")
            conn.close()

            # Save to history
            history = _load_history()
            history.append({
                "id": str(uuid.uuid4())[:8],
                "timestamp": _now_iso(),
                "question": question,
                "sql": sql,
                "result_count": len(rows),
                "executed": True,
            })
            _save_history(history)
        except Exception as e:
            print(f"{R}Query failed: {e}{W}")
    else:
        print(f"{Y}Execution only supported for SQLite currently.{W}")
        print(f"  For {conn_info['type']}, use the generated SQL above.")


async def cmd_history(args: list[str]):
    """View query history."""
    _ensure_dirs()
    history = _load_history()
    if not history:
        print(f"{Y}No query history yet.{W}")
        return

    limit = 20
    for i, a in enumerate(args):
        if a == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])

    print(f"\n{BOLD}Query History{W} (last {limit})\n")
    for entry in history[-limit:]:
        ts = entry["timestamp"].split("T")[1][:8]
        date = entry["timestamp"][:10]
        exec_flag = f"{G}[executed]{W}" if entry.get("executed") else ""
        print(f"  {C}{date} {ts}{W} {exec_flag}")
        print(f"    Q: {entry['question'][:70]}")
        sql_preview = entry["sql"].replace("\n", " ")[:70]
        print(f"    SQL: {sql_preview}")
        if entry.get("result_count") is not None:
            print(f"    Results: {entry['result_count']} rows")
        print()


async def cmd_optimize(args: list[str]):
    """Suggest query optimizations."""
    sql = None
    for i, a in enumerate(args):
        if a == "--sql" and i + 1 < len(args):
            sql = args[i + 1]; break

    if not sql:
        # Use last query from history
        history = _load_history()
        if history:
            sql = history[-1]["sql"]
            print(f"{C}Using last query from history{W}\n")
        else:
            print(f"{R}Usage: optimize --sql 'SELECT ...' (or run a query first){W}")
            return

    training = _load_training()

    print(f"\n{BOLD}Query:{W} {sql}\n")
    suggestions = _suggest_optimizations(sql, training)

    print(f"{BOLD}Optimization Suggestions:{W}")
    for i, s in enumerate(suggestions, 1):
        print(f"  {Y}{i}.{W} {s}")

    # Parse DDL to suggest specific indexes
    all_tables = {}
    for ddl_entry in training["ddl"]:
        all_tables.update(_parse_ddl(ddl_entry["text"]))

    if all_tables:
        print(f"\n{BOLD}Suggested Indexes:{W}")
        found = False
        for tname, cols in all_tables.items():
            if tname in sql.lower():
                for c in cols:
                    if not c.get("primary_key") and c["name"] in sql.lower():
                        print(f"  CREATE INDEX idx_{tname}_{c['name']} ON {tname} ({c['name']});")
                        found = True
        if not found:
            print(f"  {C}No additional indexes needed based on current schema.{W}")
    print()


# ── main ──────────────────────────────────────────────────────────────

COMMANDS = {
    "train": cmd_train,
    "ask": cmd_ask,
    "connect": cmd_connect,
    "run": cmd_run,
    "history": cmd_history,
    "optimize": cmd_optimize,
}


async def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(f"\n{BOLD}Vanna SQL{W} — Natural language to SQL\n")
        print(f"  {C}Commands:{W}")
        for cmd, fn in COMMANDS.items():
            doc = fn.__doc__ or ""
            print(f"    {cmd:<12} {doc}")
        print(f"\n  {C}Usage:{W} python3 vanna_sql.py <command> [options]\n")
        return

    cmd = sys.argv[1]
    if cmd not in COMMANDS:
        print(f"{R}Unknown command: {cmd}{W}")
        sys.exit(1)

    await COMMANDS[cmd](sys.argv[2:])


if __name__ == "__main__":
    asyncio.run(main())
