#!/usr/bin/env python3
"""nous-memory: persistent memory CLI for AI agents."""

import argparse
import datetime as dt
import json
import math
import shutil
import sqlite3
import sys
import subprocess
import os
import urllib.request
import urllib.parse
import urllib.error
import tempfile
import textwrap
from pathlib import Path

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_NOT_FOUND = 2

MEMORY_TYPES = ("decision", "preference", "fact", "observation", "failure", "pattern")
TASK_STATUSES = ("pending", "active", "done", "cancelled")
TASK_PRIORITIES = ("low", "medium", "high", "critical")
ENTITY_TYPES = ("project", "provider", "tool", "person", "repo")
SCHEMA_VERSION = 6

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS memories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    scope TEXT DEFAULT 'global',
    content TEXT NOT NULL,
    headline TEXT,
    tags TEXT,
    source TEXT,
    metadata JSON,
    topic_key TEXT,
    deleted_at DATETIME,
    revision_count INTEGER DEFAULT 1,
    duplicate_count INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    expires_at DATETIME,
    valid_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    invalid_at DATETIME,
    superseded_by INTEGER REFERENCES memories(id)
);

CREATE TABLE IF NOT EXISTS entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    name TEXT NOT NULL UNIQUE,
    metadata JSON,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    description TEXT,
    status TEXT DEFAULT 'pending',
    priority TEXT DEFAULT 'medium',
    due_date TEXT,
    repeat_rule TEXT,
    entity_id INTEGER REFERENCES entities(id),
    tags TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    completed_at DATETIME
);

CREATE TABLE IF NOT EXISTS session_refs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    summary TEXT,
    files_modified TEXT,
    decisions_made TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value JSON NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS recall_trace (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    command TEXT NOT NULL,
    scope TEXT,
    query TEXT,
    tier INTEGER,
    budget_chars INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS recall_trace_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES recall_trace(id) ON DELETE CASCADE,
    step TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS memory_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    src_memory_id INTEGER NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
    relation TEXT NOT NULL,
    dst_memory_id INTEGER NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
    note TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_memories_type ON memories(type);
CREATE INDEX IF NOT EXISTS idx_memories_scope ON memories(scope);
CREATE INDEX IF NOT EXISTS idx_memories_tags ON memories(tags);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(due_date);
CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type);
CREATE INDEX IF NOT EXISTS idx_session_refs_session ON session_refs(session_id);
CREATE INDEX IF NOT EXISTS idx_trace_events_run ON recall_trace_events(run_id, id);
CREATE INDEX IF NOT EXISTS idx_links_src ON memory_links(src_memory_id, relation);
CREATE INDEX IF NOT EXISTS idx_links_dst ON memory_links(dst_memory_id, relation);

CREATE TABLE IF NOT EXISTS episodes (
    id TEXT PRIMARY KEY,
    scope TEXT NOT NULL,
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at DATETIME,
    intent TEXT,
    summary TEXT,
    status TEXT CHECK(status IN ('active','paused','done')) NOT NULL DEFAULT 'active',
    parent_episode_id TEXT REFERENCES episodes(id)
);

CREATE TABLE IF NOT EXISTS episode_memories (
    episode_id TEXT NOT NULL REFERENCES episodes(id),
    memory_id INTEGER NOT NULL REFERENCES memories(id),
    relation TEXT NOT NULL CHECK(relation IN ('created_in','referenced_in','supersedes','contradicts')),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (episode_id, memory_id, relation)
);

CREATE TABLE IF NOT EXISTS memory_access_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    memory_id INTEGER NOT NULL REFERENCES memories(id),
    episode_id TEXT REFERENCES episodes(id),
    accessed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    access_kind TEXT CHECK(access_kind IN ('recall','bootstrap','dream','pin')),
    query TEXT
);

CREATE TABLE IF NOT EXISTS memory_access_stats (
    memory_id INTEGER PRIMARY KEY REFERENCES memories(id),
    access_count INTEGER NOT NULL DEFAULT 0,
    last_accessed_at DATETIME,
    access_score REAL NOT NULL DEFAULT 0.0
);

CREATE INDEX IF NOT EXISTS idx_episodes_scope ON episodes(scope);
CREATE INDEX IF NOT EXISTS idx_episodes_status ON episodes(status);
CREATE INDEX IF NOT EXISTS idx_episode_memories_memory ON episode_memories(memory_id);
CREATE INDEX IF NOT EXISTS idx_access_events_memory ON memory_access_events(memory_id);
CREATE INDEX IF NOT EXISTS idx_access_events_episode ON memory_access_events(episode_id);
"""


def parse_dt(value):
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    candidates = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d",
    ]
    for pattern in candidates:
        try:
            parsed = dt.datetime.strptime(text, pattern)
            if pattern == "%Y-%m-%d":
                return dt.datetime.combine(parsed.date(), dt.time.min)
            return parsed
        except ValueError:
            continue
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError:
        return None


def is_recent(date_value, now=None):
    parsed = parse_dt(date_value)
    if parsed is None:
        return False
    now = now or dt.datetime.now()
    return abs((now - parsed).days) <= 7


def humanize_datetime(value, now=None):
    parsed = parse_dt(value)
    if parsed is None:
        return str(value) if value is not None else "-"
    now = now or dt.datetime.now()
    delta = now - parsed
    seconds = int(delta.total_seconds())
    if seconds < 0:
        seconds = abs(seconds)
        if seconds < 60:
            return "in <1m"
        if seconds < 3600:
            return f"in {seconds // 60}m"
        if seconds < 86400:
            return f"in {seconds // 3600}h"
        if seconds < 172800:
            return "tomorrow"
        return f"in {seconds // 86400}d"
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 86400:
        return f"{seconds // 3600}h ago"
    if seconds < 172800:
        return "yesterday"
    if seconds < 604800:
        return f"{seconds // 86400}d ago"
    return parsed.isoformat(sep=" ", timespec="seconds")


def normalize_tags(tags):
    if not tags:
        return None
    bits = [item.strip() for item in tags.split(",") if item.strip()]
    if not bits:
        return None
    unique = []
    seen = set()
    for tag in bits:
        lowered = tag.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        unique.append(tag)
    return ",".join(unique)


def extract_headline(content: str, max_len: int = 120) -> str:
    text = (content or "").strip()
    if not text:
        return ""
    split_at = len(text)
    markers = (". ", ".\n", "\n")
    for marker in markers:
        idx = text.find(marker)
        if idx != -1:
            end = idx + (1 if marker.startswith(".") else 0)
            split_at = min(split_at, end)
    headline = text[:split_at].strip()
    if len(headline) > max_len:
        headline = headline[: max_len - 3].rstrip() + "..."
    return headline


def split_tags(tags):
    if not tags:
        return []
    return [item.strip() for item in str(tags).split(",") if item.strip()]


def decode_json_value(value):
    if isinstance(value, (dict, list, int, float, bool)) or value is None:
        return value
    return json.loads(value)


def supports_color():
    return sys.stdout.isatty()


def color(text, code):
    if not supports_color():
        return text
    return f"\033[{code}m{text}\033[0m"


def wrap_block(prefix, content, width=80):
    wrap_width = max(20, width - len(prefix))
    lines = textwrap.wrap(content, width=wrap_width) or [""]
    output = [f"{prefix}{lines[0]}"]
    pad = " " * len(prefix)
    for line in lines[1:]:
        output.append(f"{pad}{line}")
    return "\n".join(output)


def resolve_db_path(explicit_path=None):
    """Resolve database path with precedence: explicit > env > XDG default."""
    if explicit_path:
        return Path(explicit_path)
    env_path = os.environ.get("NOUS_MEMORY_DB")
    if env_path:
        return Path(env_path)
    xdg_data = os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local" / "share"))
    default = Path(xdg_data) / "nous-memory" / "state.db"
    default.parent.mkdir(parents=True, exist_ok=True)
    return default


def make_default_db_path():
    """Backward-compatible alias for DB resolution."""
    return resolve_db_path()

def resolve_workspace(workspace=None):
    """Resolve workspace with precedence: explicit > env > cwd."""
    base = workspace or os.environ.get("NOUS_MEMORY_WORKSPACE") or os.getcwd()
    return Path(base).expanduser().resolve()


def log_verbose(args, message):
    if getattr(args, "verbose", False):
        print(color(f"[debug] {message}", "2"), file=sys.stderr)


def fail(message, code=EXIT_ERROR):
    print(color(f"Error: {message}", "31"), file=sys.stderr)
    return code


def connect_db(db_path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def has_column(conn, table, column):
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row["name"] == column for row in rows)


def table_exists(conn, table_name):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def ensure_backup_for_migration(db_path):
    if not db_path.exists() or db_path.name != "state.db":
        return None
    backups_dir = db_path.parent / "backups"
    backups_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = backups_dir / f"state.db.pre-fts5-{stamp}"
    shutil.copy2(db_path, backup_path)
    return backup_path


def ensure_fts(conn, force_rebuild=False):
    conn.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
            content, tags, type, scope,
            content=memories, content_rowid=id,
            tokenize='porter unicode61'
        )
        """
    )
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS memories_fts_ai AFTER INSERT ON memories BEGIN
            INSERT INTO memories_fts(rowid, content, tags, type, scope)
            VALUES (new.id, new.content, new.tags, new.type, new.scope);
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS memories_fts_ad AFTER DELETE ON memories BEGIN
            INSERT INTO memories_fts(memories_fts, rowid, content, tags, type, scope)
            VALUES ('delete', old.id, old.content, old.tags, old.type, old.scope);
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS memories_fts_au AFTER UPDATE ON memories BEGIN
            INSERT INTO memories_fts(memories_fts, rowid, content, tags, type, scope)
            VALUES ('delete', old.id, old.content, old.tags, old.type, old.scope);
            INSERT INTO memories_fts(rowid, content, tags, type, scope)
            VALUES (new.id, new.content, new.tags, new.type, new.scope);
        END
        """
    )

    fts_count = conn.execute("SELECT COUNT(*) AS c FROM memories_fts").fetchone()["c"]
    if fts_count == 0:
        conn.execute(
            """
            INSERT INTO memories_fts(rowid, content, tags, type, scope)
            SELECT id, content, tags, type, scope FROM memories
            """
        )
        conn.execute("INSERT INTO memories_fts(memories_fts) VALUES ('rebuild')")
    elif force_rebuild:
        conn.execute("INSERT INTO memories_fts(memories_fts) VALUES ('rebuild')")


def ensure_schema(conn, db_path):
    conn.executescript(SCHEMA_SQL)

    version_row = conn.execute("SELECT value FROM kv WHERE key = 'schema_version'").fetchone()
    schema_version = 0
    if version_row is not None:
        try:
            decoded_version = decode_json_value(version_row["value"])
            if isinstance(decoded_version, (int, float, str, bool)):
                schema_version = int(decoded_version)
            else:
                schema_version = 0
        except (ValueError, TypeError, json.JSONDecodeError):
            schema_version = 0
    schema_changed = schema_version < SCHEMA_VERSION

    if schema_version < 1:
        needs_backup = (
            table_exists(conn, "memories")
            and conn.execute("SELECT COUNT(*) AS c FROM memories").fetchone()["c"] > 0
            and not has_column(conn, "memories", "topic_key")
        )
        if needs_backup:
            ensure_backup_for_migration(db_path)

        if not has_column(conn, "memories", "topic_key"):
            conn.execute("ALTER TABLE memories ADD COLUMN topic_key TEXT")
        if not has_column(conn, "memories", "deleted_at"):
            conn.execute("ALTER TABLE memories ADD COLUMN deleted_at DATETIME")
        if not has_column(conn, "memories", "revision_count"):
            conn.execute("ALTER TABLE memories ADD COLUMN revision_count INTEGER DEFAULT 1")
        if not has_column(conn, "memories", "duplicate_count"):
            conn.execute("ALTER TABLE memories ADD COLUMN duplicate_count INTEGER DEFAULT 0")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_memories_topic_key ON memories(topic_key)")
        conn.execute("UPDATE memories SET revision_count = 1 WHERE revision_count IS NULL")
        conn.execute("UPDATE memories SET duplicate_count = 0 WHERE duplicate_count IS NULL")

        conn.execute(
            """
            INSERT INTO kv(key, value, updated_at)
            VALUES ('schema_version', ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """,
            (json.dumps(SCHEMA_VERSION),),
        )

    if schema_version < 2:
        if not has_column(conn, 'memories', 'valid_at'):
            conn.execute('ALTER TABLE memories ADD COLUMN valid_at DATETIME')
        if not has_column(conn, 'memories', 'invalid_at'):
            conn.execute('ALTER TABLE memories ADD COLUMN invalid_at DATETIME')
        # Backfill: set valid_at = created_at for existing memories
        conn.execute('UPDATE memories SET valid_at = created_at WHERE valid_at IS NULL')
        conn.execute(
            """
            INSERT INTO kv(key, value, updated_at)
            VALUES ('schema_version', ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """,
            (json.dumps(SCHEMA_VERSION),),
        )


    if schema_version < 3:
        # V3: Episodes, access tracking, staleness fields
        # New tables are created by SCHEMA_SQL executescript above.
        # Here we only add columns to existing tables.
        if not has_column(conn, 'memories', 'verified_at'):
            conn.execute('ALTER TABLE memories ADD COLUMN verified_at DATETIME')
        if not has_column(conn, 'memories', 'ttl_days'):
            conn.execute('ALTER TABLE memories ADD COLUMN ttl_days INTEGER')
        if not has_column(conn, 'memories', 'staleness_policy'):
            conn.execute("ALTER TABLE memories ADD COLUMN staleness_policy TEXT DEFAULT 'ttl'")
        # Backfill staleness policies by type (no IS NULL check — ALTER DEFAULT fills 'ttl')
        conn.execute("UPDATE memories SET staleness_policy = 'none' WHERE type = 'preference'")
        conn.execute("UPDATE memories SET staleness_policy = 'ttl', ttl_days = 60 WHERE type IN ('observation', 'fact', 'pattern')")
        conn.execute("UPDATE memories SET staleness_policy = 'ttl', ttl_days = 90 WHERE type = 'decision'")
        conn.execute("UPDATE memories SET staleness_policy = 'half_life' WHERE type = 'failure'")
        conn.execute(
            """
            INSERT INTO kv(key, value, updated_at)
            VALUES ('schema_version', ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """,
            (json.dumps(SCHEMA_VERSION),),
        )

    if schema_version < 4:
        if not has_column(conn, 'memories', 'headline'):
            conn.execute('ALTER TABLE memories ADD COLUMN headline TEXT')
        conn.execute(
            """
            UPDATE memories
            SET headline = substr(
                content,
                1,
                CASE
                    WHEN instr(content, '.') > 0 AND instr(content, '.') <= 120 THEN instr(content, '.')
                    ELSE MIN(length(content), 120)
                END
            )
            WHERE headline IS NULL
            """
        )
        conn.execute(
            """
            INSERT INTO kv(key, value, updated_at)
            VALUES ('schema_version', ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """,
            (json.dumps(SCHEMA_VERSION),),
        )

    if schema_version < 5:
        if not has_column(conn, 'memories', 'metadata'):
            conn.execute('ALTER TABLE memories ADD COLUMN metadata JSON')
        conn.execute(
            """
            INSERT INTO kv(key, value, updated_at)
            VALUES ('schema_version', ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """,
            (json.dumps(5),),
        )

    if schema_version < 6:
        # memory_links table is created by SCHEMA_SQL executescript above
        # Just bump version
        conn.execute(
            """
            INSERT INTO kv(key, value, updated_at)
            VALUES ('schema_version', ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """,
            (json.dumps(6),),
        )

    ensure_fts(conn, force_rebuild=schema_changed)

    conn.commit()


def row_to_dict(row):
    return {key: row[key] for key in row.keys()}


def parse_relative_date(text):
    lowered = text.strip().lower()
    now = dt.datetime.now()
    if lowered == "tomorrow":
        return (now + dt.timedelta(days=1)).date().isoformat()
    if lowered.startswith("in "):
        parts = lowered.split()
        if len(parts) == 3 and parts[1].isdigit():
            amount = int(parts[1])
            unit = parts[2]
            if unit in {"day", "days"}:
                return (now + dt.timedelta(days=amount)).date().isoformat()
            if unit in {"hour", "hours"}:
                return (now + dt.timedelta(hours=amount)).isoformat(timespec="seconds")
    if lowered.startswith("next "):
        target = lowered[5:].strip()
        weekdays = {
            "monday": 0,
            "tuesday": 1,
            "wednesday": 2,
            "thursday": 3,
            "friday": 4,
            "saturday": 5,
            "sunday": 6,
        }
        if target in weekdays:
            current = now.weekday()
            wanted = weekdays[target]
            days_ahead = (wanted - current + 7) % 7
            if days_ahead == 0:
                days_ahead = 7
            return (now + dt.timedelta(days=days_ahead)).date().isoformat()
    parsed = parse_dt(text)
    if parsed is None:
        return None
    if len(text.strip()) <= 10:
        return parsed.date().isoformat()
    return parsed.isoformat(timespec="seconds")


def memory_to_json(row):
    data = row_to_dict(row)
    data["tags"] = split_tags(data.get("tags"))
    data["headline"] = data.get("headline")
    return data


def build_memory_filters(args, table_alias=""):
    prefix = f"{table_alias}." if table_alias else ""
    clauses = [f"{prefix}deleted_at IS NULL"]
    values = []

    if getattr(args, "type", None):
        clauses.append(f"{prefix}type = ?")
        values.append(args.type)
    if getattr(args, "scope", None):
        clauses.append(f"{prefix}scope = ?")
        values.append(args.scope)
    if getattr(args, "tags", None):
        for tag in split_tags(args.tags):
            clauses.append(f"(',' || {prefix}tags || ',' LIKE ?)")
            values.append(f"%,{tag.strip()},%")
    if getattr(args, "active", False):
        clauses.append(f"{prefix}superseded_by IS NULL")
        clauses.append(f"({prefix}expires_at IS NULL OR {prefix}expires_at > CURRENT_TIMESTAMP)")

    return clauses, values


def word_set(text):
    """Extract normalized word set from text for overlap comparison."""
    import re
    return set(re.sub(r'[^a-z0-9]', ' ', text.lower()).split())


def word_overlap_ratio(text_a, text_b):
    """Return Jaccard similarity (0.0–1.0) between two texts' word sets."""
    set_a = word_set(text_a)
    set_b = word_set(text_b)
    if not set_a or not set_b:
        return 0.0
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union)


def find_similar_memories(conn, content, mem_type, scope, exclude_id=None, limit=5):
    """Search FTS5 for memories similar to content (same type+scope)."""
    # Build a short query from the most distinctive words
    words = word_set(content)
    if not words:
        return []
    # Use up to 8 words for FTS5 query (OR-joined for broad matching)
    query_words = sorted(words, key=len, reverse=True)[:8]
    fts_query = ' OR '.join(query_words)
    try:
        rows = conn.execute(
            """
            SELECT m.*, bm25(memories_fts) AS rank
            FROM memories_fts
            JOIN memories m ON m.id = memories_fts.rowid
            WHERE memories_fts MATCH ?
              AND m.type = ?
              AND m.scope = ?
              AND m.deleted_at IS NULL
              AND m.superseded_by IS NULL
            ORDER BY rank
            LIMIT ?
            """,
            (fts_query, mem_type, scope, limit),
        ).fetchall()
    except sqlite3.Error:
        return []
    if exclude_id is not None:
        rows = [r for r in rows if r['id'] != exclude_id]
    return rows


def cmd_capture(args, conn):
    tags = normalize_tags(args.tags)
    headline = args.headline or extract_headline(args.content)
    expires_at = None
    if args.expires:
        parsed = parse_relative_date(args.expires)
        if not parsed:
            return fail(f"could not parse expires value '{args.expires}'")
        expires_at = parsed
    no_synthesis = getattr(args, 'no_synthesis', False)
    metadata_json = None
    raw_metadata = getattr(args, 'metadata', None)
    if raw_metadata:
        try:
            parsed_meta = json.loads(raw_metadata) if isinstance(raw_metadata, str) else raw_metadata
            if not isinstance(parsed_meta, dict):
                return fail('--metadata must be a JSON object')
            metadata_json = json.dumps(parsed_meta)
        except json.JSONDecodeError as e:
            return fail(f'invalid --metadata JSON: {e}')
    memory_id = None
    synthesis_action = None  # None | 'duplicate' | 'related'
    synthesis_match_id = None
    if args.topic_key:
        # Topic-key upsert path (existing behavior, no synthesis needed)
        existing = conn.execute(
            """
            SELECT id
            FROM memories
            WHERE topic_key = ?
              AND scope = ?
              AND deleted_at IS NULL
            ORDER BY id DESC
            LIMIT 1
            """,
            (args.topic_key, args.scope),
        ).fetchone()
        if existing is not None:
            memory_id = existing["id"]
            conn.execute(
                """
                UPDATE memories
                SET type = ?,
                    content = ?,
                    headline = ?,
                    tags = ?,
                    source = ?,
                    expires_at = ?,
                    metadata = COALESCE(?, metadata),
                    revision_count = COALESCE(revision_count, 1) + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (args.type, args.content, headline, tags, args.source, expires_at, metadata_json, memory_id),
            )
        else:
            cur = conn.execute(
                """
                INSERT INTO memories(type, scope, content, headline, tags, source, expires_at, topic_key, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (args.type, args.scope, args.content, headline, tags, args.source, expires_at, args.topic_key, metadata_json),
            )
            memory_id = cur.lastrowid
    else:
        # Check for similar memories before inserting (online synthesis)
        if not no_synthesis:
            similar = find_similar_memories(conn, args.content, args.type, args.scope)
            for sim_row in similar:
                ratio = word_overlap_ratio(args.content, sim_row['content'])
                if ratio >= 0.80:
                    # Near-duplicate: bump duplicate_count, skip insert
                    synthesis_action = 'duplicate'
                    synthesis_match_id = sim_row['id']
                    memory_id = sim_row['id']
                    conn.execute(
                        """
                        UPDATE memories
                        SET duplicate_count = COALESCE(duplicate_count, 0) + 1,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (memory_id,),
                    )
                    break
                elif ratio >= 0.50:
                    # Related but distinct — insert, note relationship
                    synthesis_action = 'related'
                    synthesis_match_id = sim_row['id']
                    break

        if memory_id is None:
            cur = conn.execute(
                """
                INSERT INTO memories(type, scope, content, headline, tags, source, expires_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (args.type, args.scope, args.content, headline, tags, args.source, expires_at, metadata_json),
            )
            memory_id = cur.lastrowid
    conn.commit()
    row = conn.execute("SELECT * FROM memories WHERE id = ?", (memory_id,)).fetchone()
    if args.json:
        data = memory_to_json(row)
        if synthesis_action:
            data['synthesis'] = {'action': synthesis_action, 'match_id': synthesis_match_id}
        print(json.dumps(data, indent=2))
    else:
        if synthesis_action == 'duplicate':
            print(color(f"Duplicate of [{synthesis_match_id}] (dup_count +1, skipped insert)", "33"))
        elif synthesis_action == 'related':
            print(color(f"Captured memory [{memory_id}]", "32") + f"  related_to=[{synthesis_match_id}]")
        else:
            print(color(f"Captured memory [{memory_id}]", "32"))
    return EXIT_OK


def _record_recall_access(conn, args, rows):
    if not rows:
        return
    active_map = _active_episode_map(conn)
    now = dt.datetime.now()
    tau_seconds = 20 * 86400

    for row in rows:
        scope = row["scope"] or "global"
        episode_id = None
        if getattr(args, "scope", None):
            episode_id = active_map.get(args.scope)
        if not episode_id:
            episode_id = active_map.get(scope)

        conn.execute(
            """
            INSERT INTO memory_access_events(memory_id, episode_id, accessed_at, access_kind, query)
            VALUES (?, ?, CURRENT_TIMESTAMP, 'recall', ?)
            """,
            (row["id"], episode_id, args.query),
        )

        existing = conn.execute(
            "SELECT access_score, last_accessed_at FROM memory_access_stats WHERE memory_id = ?",
            (row["id"],),
        ).fetchone()
        if existing is None:
            new_score = 1.0
        else:
            old_score = float(existing["access_score"] or 0.0)
            last_dt = parse_dt(existing["last_accessed_at"])
            if last_dt is None:
                delta_seconds = 0.0
            else:
                delta_seconds = max(0.0, (now - last_dt).total_seconds())
            new_score = old_score * math.exp(-delta_seconds / tau_seconds) + 1.0

        conn.execute(
            """
            INSERT INTO memory_access_stats(memory_id, access_count, last_accessed_at, access_score)
            VALUES (?, 1, CURRENT_TIMESTAMP, ?)
            ON CONFLICT(memory_id) DO UPDATE SET
                access_count = memory_access_stats.access_count + 1,
                last_accessed_at = CURRENT_TIMESTAMP,
                access_score = excluded.access_score
            """,
            (row["id"], new_score),
        )
    conn.commit()


def _compute_staleness(row, now):
    """Compute staleness score (0.0 = fresh, 1.0 = maximally stale)."""
    policy = row['staleness_policy'] if 'staleness_policy' in row.keys() else 'ttl'
    if not policy or policy == 'none':
        return 0.0
    verified = parse_dt(row['verified_at']) if 'verified_at' in row.keys() else None
    created = parse_dt(row['created_at'])
    ref_time = verified or created or now
    age_days = max(0.0, (now - ref_time).total_seconds() / 86400)
    if policy == 'ttl':
        ttl = int(row['ttl_days']) if ('ttl_days' in row.keys() and row['ttl_days']) else 60
        grace = 14
        if age_days <= ttl:
            return 0.0
        return min(1.0, (age_days - ttl) / grace)
    if policy == 'half_life':
        half_life = 30  # days
        return 1.0 - math.exp(-math.log(2) * age_days / half_life)
    return 0.0


def _rerank_with_scores(conn, rows, now):
    """Rerank recall results using composite score: relevance * frequency boost * staleness penalty."""
    if not rows:
        return []
    # Fetch access stats for all memory ids in one query
    ids = [row['id'] for row in rows]
    placeholders = ','.join('?' for _ in ids)
    stats_rows = conn.execute(
        f'SELECT memory_id, access_score FROM memory_access_stats WHERE memory_id IN ({placeholders})',
        ids,
    ).fetchall()
    stats_map = {r['memory_id']: float(r['access_score'] or 0.0) for r in stats_rows}
    scored = []
    for i, row in enumerate(rows):
        # Base relevance: inverse of original position (first = highest)
        base_relevance = 1.0 / (1 + i)
        access_score = stats_map.get(row['id'], 0.0)
        freq_boost = 1.0 + 0.2 * math.log1p(access_score)
        staleness = _compute_staleness(row, now)
        staleness_penalty = 1.0 - 0.3 * staleness
        composite = base_relevance * freq_boost * staleness_penalty
        scored.append((composite, staleness, row))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored


def _activation_signal_count(row, args, now):
    signals = 0
    query_tags = split_tags(getattr(args, 'tags', None) or '')
    query_scope = getattr(args, 'scope', None)
    has_query = bool(getattr(args, 'query', None))

    if has_query:
        signals += 1

    row_tags = split_tags(row['tags'] if 'tags' in row.keys() else None)
    query_tag_set = {t.lower() for t in query_tags}
    row_tag_set = {t.lower() for t in row_tags}
    if query_tag_set and (query_tag_set & row_tag_set):
        signals += 1

    if query_scope and row['scope'] == query_scope:
        signals += 1

    created = parse_dt(row['created_at'])
    if created:
        age_days = (now - created).total_seconds() / 86400
        if age_days <= 7:
            signals += 1

    return signals


def _rerank_with_activation(conn, rows, args, now, threshold=2):
    if not rows:
        return []

    ids = [row['id'] for row in rows]
    placeholders = ','.join('?' for _ in ids)
    stats_rows = conn.execute(
        f'SELECT memory_id, access_score FROM memory_access_stats WHERE memory_id IN ({placeholders})',
        ids,
    ).fetchall()
    stats_map = {r['memory_id']: float(r['access_score'] or 0.0) for r in stats_rows}

    query_tags = split_tags(getattr(args, 'tags', None) or '')
    query_scope = getattr(args, 'scope', None)
    has_query = bool(getattr(args, 'query', None))
    query_tag_set = {t.lower() for t in query_tags}

    scored = []
    for i, row in enumerate(rows):
        signals = 0
        signal_weights = 0.0

        if has_query:
            signals += 1
            base_relevance = 1.0 / (1 + i)
            signal_weights += 0.3 * base_relevance

        row_tags = split_tags(row['tags'] if 'tags' in row.keys() else None)
        row_tag_set = {t.lower() for t in row_tags}
        tag_overlap = len(query_tag_set & row_tag_set)
        if tag_overlap > 0:
            signals += 1
            signal_weights += 0.25 * min(1.0, tag_overlap / max(len(query_tag_set), 1))

        if query_scope and row['scope'] == query_scope:
            signals += 1
            signal_weights += 0.25

        created = parse_dt(row['created_at'])
        if created:
            age_days = (now - created).total_seconds() / 86400
            if age_days <= 7:
                signals += 1
                signal_weights += 0.2 * (1.0 - age_days / 7)

        access_score = stats_map.get(row['id'], 0.0)
        freq_boost = 1.0 + 0.2 * math.log1p(access_score)

        metadata = {}
        if 'metadata' in row.keys() and row['metadata']:
            try:
                metadata = json.loads(row['metadata']) if isinstance(row['metadata'], str) else (row['metadata'] or {})
            except (json.JSONDecodeError, TypeError):
                metadata = {}
        importance_weight = float(metadata.get('importance_weight', 1.0)) if isinstance(metadata, dict) else 1.0

        staleness = _compute_staleness(row, now)
        staleness_penalty = 1.0 - 0.3 * staleness

        if signals >= threshold:
            composite = (signal_weights + 0.5) * importance_weight * staleness_penalty
        else:
            composite = signal_weights * 0.1 * importance_weight * staleness_penalty
        composite *= freq_boost

        scored.append((composite, staleness, row, signals))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [(composite, staleness, row) for composite, staleness, row, _signals in scored]


def cmd_recall(args, conn):
    # Semantic mode — call daemon API instead of SQLite
    if getattr(args, 'semantic', False):
        return _recall_semantic(args)
    trace_enabled = getattr(args, 'trace', False)
    trace_run_id = None
    if trace_enabled:
        cur = conn.execute(
            """
            INSERT INTO recall_trace(command, scope, query, tier, budget_chars)
            VALUES ('recall', ?, ?, NULL, NULL)
            """,
            (args.scope, args.query),
        )
        trace_run_id = cur.lastrowid

    def trace_event(step, payload):
        if not trace_enabled or trace_run_id is None:
            return
        conn.execute(
            """
            INSERT INTO recall_trace_events(run_id, step, payload)
            VALUES (?, ?, ?)
            """,
            (trace_run_id, step, json.dumps(payload)),
        )

    clauses, values = build_memory_filters(args, table_alias='m')
    # Over-fetch 3x for reranking with frequency + staleness
    fetch_limit = args.limit * 3
    rows = []
    if args.query:
        fts_sql = f"""
            SELECT m.*, bm25(memories_fts) AS rank
            FROM memories_fts
            JOIN memories m ON m.id = memories_fts.rowid
            WHERE memories_fts MATCH ? AND {' AND '.join(clauses)}
            ORDER BY rank, m.created_at DESC, m.id DESC
            LIMIT ?
        """
        try:
            rows = conn.execute(fts_sql, (args.query, *values, fetch_limit)).fetchall()
        except sqlite3.Error:
            fallback_clauses = [clause.replace('m.', '') for clause in clauses]
            fallback_values = list(values)
            fallback_clauses.append("(content LIKE ? OR (',' || tags || ',' LIKE ?))")
            query_term = f'%{args.query}%'
            fallback_values.extend([query_term, f"%,{args.query.strip()},%"])
            where_sql = 'WHERE ' + ' AND '.join(fallback_clauses)
            sql = f"""
                SELECT *, NULL AS rank
                FROM memories
                {where_sql}
                ORDER BY created_at DESC, id DESC
                LIMIT ?
            """
            fallback_values.append(fetch_limit)
            rows = conn.execute(sql, tuple(fallback_values)).fetchall()
    else:
        where_sql = 'WHERE ' + ' AND '.join(clause.replace('m.', '') for clause in clauses)
        sql = f"""
            SELECT *, NULL AS rank
            FROM memories
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT ?
        """
        rows = conn.execute(sql, (*values, fetch_limit)).fetchall()

    trace_event('candidates_fts', {
        'count': len(rows),
        'ids': [row['id'] for row in rows],
    })

    # Rerank with frequency + staleness scoring
    now = dt.datetime.now()
    use_v2 = getattr(args, 'v2', False)
    if use_v2:
        scored = _rerank_with_activation(conn, rows, args, now)
    else:
        scored = _rerank_with_scores(conn, rows, now)
    trace_event('reranked', {
        'count': len(scored),
        'results': [
            {
                'id': row['id'],
                'composite': composite,
                'staleness': staleness,
                **({'signals': _activation_signal_count(row, args, now)} if use_v2 else {}),
            }
            for composite, staleness, row in scored
        ],
    })
    # Truncate to requested limit
    scored = scored[:args.limit]
    final_rows = [entry[2] for entry in scored]
    trace_event('selected', {
        'count': len(final_rows),
        'ids': [row['id'] for row in final_rows],
    })
    if args.json:
        print(json.dumps([memory_to_json(row) for row in final_rows], indent=2))
        _record_recall_access(conn, args, final_rows)
        if trace_enabled:
            conn.commit()
        return EXIT_OK
    if not scored:
        print('No memories found.')
        if trace_enabled:
            conn.commit()
        return EXIT_OK
    for composite, staleness, row in scored:
        memory_id = row['id']
        title = f'[{memory_id}] {row["type"]}'
        scope = row['scope'] or 'global'
        created = humanize_datetime(row['created_at'])
        stale_flag = ''
        if staleness >= 0.7:
            stale_flag = color(' [STALE]', '33')
        elif staleness >= 0.4:
            stale_flag = color(' [aging]', '33')
        activation_flag = ''
        if use_v2:
            activation_flag = f"  signals={_activation_signal_count(row, args, now)}"
        print(color(title, '36') + f'  scope={scope}  created={created}' + stale_flag + activation_flag)
        headline = row['headline'] if 'headline' in row.keys() else None
        if headline and not str(row['content']).startswith(headline):
            print(f'  headline: {headline}')
        print(wrap_block('  content: ', row['content'], width=80))
        tags = ', '.join(split_tags(row['tags'])) if row['tags'] else '-'
        print(f'  tags: {tags}')
        if row['expires_at']:
            print(f'  expires: {humanize_datetime(row["expires_at"])}')
        if row['superseded_by']:
            print(f'  superseded_by: {row["superseded_by"]}')
        if row['revision_count'] and row['revision_count'] > 1:
            print(f'  revision_count: {row["revision_count"]}')
        if row['topic_key']:
            print(f'  topic_key: {row["topic_key"]}')
        if staleness >= 0.7:
            verified = row['verified_at'] if 'verified_at' in row.keys() else None
            last_check = humanize_datetime(verified) if verified else humanize_datetime(row['created_at'])
            print(color(f'  \u26a0 May be outdated (last verified: {last_check}). Re-validate?', '33'))
        print()
    _record_recall_access(conn, args, final_rows)
    if trace_enabled:
        conn.commit()
    return EXIT_OK


def _recall_semantic(args):
    """Recall memories via daemon's semantic search API."""
    if not args.query:
        return fail("--semantic requires a search query")

    daemon_url = (
        args.daemon_url
        or os.environ.get("NOUS_DAEMON_URL")
        or "http://localhost:8080"
    )

    # Build query params
    params = {"q": args.query, "limit": str(args.limit)}
    if getattr(args, 'type', None):
        params["type"] = args.type
    if getattr(args, 'scope', None):
        params["scope"] = args.scope
    if getattr(args, 'tags', None):
        params["tags"] = args.tags

    url = f"{daemon_url.rstrip('/')}/v1/recall?{urllib.parse.urlencode(params)}"

    try:
        req = urllib.request.Request(url)
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.URLError as e:
        return fail(f"daemon unreachable at {daemon_url}: {e}")
    except json.JSONDecodeError:
        return fail("invalid JSON response from daemon")

    memories = data.get("memories", [])
    method = data.get("method", "unknown")

    if args.json:
        print(json.dumps(data, indent=2))
        return EXIT_OK

    if not memories:
        print(f"No memories found. (method={method})")
        return EXIT_OK

    print(color(f"Semantic recall ({method}):", "33"))
    print()
    for m in memories:
        memory_id = m.get("id", "?")
        mtype = m.get("type", "?")
        scope = m.get("scope", "global")
        created = m.get("created_at", "")
        title = f"[{memory_id}] {mtype}"
        print(color(title, "36") + f"  scope={scope}  created={created}")
        print(wrap_block("  content: ", m.get("content", ""), width=80))
        tags = m.get("tags", "")
        print(f"  tags: {tags or '-'}")
        source = m.get("source", "")
        if source:
            print(f"  source: {source}")
        print()
    return EXIT_OK


def cmd_search(args, conn):
    clauses, values = build_memory_filters(args, table_alias="m")
    sql = f"""
        SELECT m.*, bm25(memories_fts) AS rank
        FROM memories_fts
        JOIN memories m ON m.id = memories_fts.rowid
        WHERE memories_fts MATCH ? AND {' AND '.join(clauses)}
        ORDER BY rank, m.created_at DESC, m.id DESC
        LIMIT ?
    """
    try:
        rows = conn.execute(sql, (args.query, *values, args.limit)).fetchall()
    except sqlite3.Error:
        query_term = f"%{args.query}%"
        fallback_clauses = [clause.replace("m.", "") for clause in clauses]
        fallback_clauses.append("(content LIKE ? OR (',' || tags || ',' LIKE ?))")
        fallback_values = [*values, query_term, f"%,{args.query.strip()},%", args.limit]
        where_sql = "WHERE " + " AND ".join(fallback_clauses)
        rows = conn.execute(
            f"""
            SELECT *, NULL AS rank
            FROM memories
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            tuple(fallback_values),
        ).fetchall()

    if args.json:
        payload = []
        for row in rows:
            snippet = row["content"][:80]
            payload.append({
                "id": row["id"],
                "type": row["type"],
                "scope": row["scope"],
                "rank": row["rank"],
                "snippet": snippet,
                "tags": split_tags(row["tags"]),
            })
        print(json.dumps(payload, indent=2))
        return EXIT_OK

    if not rows:
        print("No memories found.")
        return EXIT_OK

    for row in rows:
        rank = row["rank"]
        rank_text = f"{rank:.4f}" if rank is not None else "-"
        print(f"[{row['id']}] {row['type']}  scope={row['scope'] or 'global'}  rank={rank_text}")
        print(f"  {row['content'][:80]}")
        print(f"  tags: {', '.join(split_tags(row['tags'])) if row['tags'] else '-'}")
    return EXIT_OK


def cmd_get(args, conn):
    row = conn.execute("SELECT * FROM memories WHERE id = ?", (args.id,)).fetchone()
    if row is None:
        return fail(f"memory {args.id} not found", code=EXIT_NOT_FOUND)

    if args.json:
        print(json.dumps(memory_to_json(row), indent=2))
        return EXIT_OK

    for key in row.keys():
        value = row[key]
        if key == "tags":
            value = ", ".join(split_tags(value)) if value else "-"
        if value is None:
            value = "-"
        print(f"{key}: {value}")
    return EXIT_OK


def cmd_timeline(args, conn):
    target = conn.execute("SELECT * FROM memories WHERE id = ?", (args.id,)).fetchone()
    if target is None:
        return fail(f"memory {args.id} not found", code=EXIT_NOT_FOUND)

    rows = conn.execute(
        """
        SELECT *
        FROM memories
        WHERE scope = ?
          AND deleted_at IS NULL
          AND created_at BETWEEN datetime(?, '-24 hours') AND datetime(?, '+24 hours')
        ORDER BY created_at ASC, id ASC
        """,
        (target["scope"], target["created_at"], target["created_at"]),
    ).fetchall()

    if not any(row["id"] == target["id"] for row in rows):
        rows = list(rows) + [target]
        rows.sort(key=lambda item: (item["created_at"], item["id"]))

    if args.json:
        print(json.dumps({
            "target": memory_to_json(target),
            "timeline": [memory_to_json(row) for row in rows],
        }, indent=2))
        return EXIT_OK

    if not rows:
        print("No timeline context found.")
        return EXIT_OK

    for row in rows:
        marker = ">" if row["id"] == target["id"] else " "
        print(
            f"{marker} [{row['id']}] {humanize_datetime(row['created_at'])}  {row['type']}  "
            f"scope={row['scope'] or 'global'}"
        )
        print(f"  {row['content'][:100]}")
    return EXIT_OK


def cmd_update(args, conn):
    old = conn.execute("SELECT * FROM memories WHERE id = ? AND deleted_at IS NULL", (args.id,)).fetchone()
    if old is None:
        return fail(f"memory {args.id} not found", code=EXIT_NOT_FOUND)
    if old["superseded_by"]:
        return fail(f"memory {args.id} is already superseded", code=EXIT_ERROR)

    cur = conn.execute(
        """
        INSERT INTO memories(type, scope, content, tags, source, expires_at, topic_key, revision_count, duplicate_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            old["type"],
            old["scope"],
            args.content,
            old["tags"],
            old["source"],
            old["expires_at"],
            old["topic_key"],
            old["revision_count"],
            old["duplicate_count"],
        ),
    )
    new_id = cur.lastrowid
    conn.execute(
        "UPDATE memories SET superseded_by = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (new_id, args.id),
    )
    conn.commit()

    if args.json:
        row = conn.execute("SELECT * FROM memories WHERE id = ?", (new_id,)).fetchone()
        print(json.dumps({"old_id": args.id, "new": memory_to_json(row)}, indent=2))
    else:
        print(color(f"Updated memory [{args.id}] -> [{new_id}]", "32"))
    return EXIT_OK


def cmd_forget(args, conn):
    row = conn.execute("SELECT * FROM memories WHERE id = ?", (args.id,)).fetchone()
    if row is None:
        return fail(f"memory {args.id} not found", code=EXIT_NOT_FOUND)

    if args.hard:
        conn.execute("DELETE FROM memories WHERE id = ?", (args.id,))
        conn.commit()
        if args.json:
            print(json.dumps({"deleted": args.id, "hard": True}, indent=2))
        else:
            print(color(f"Permanently deleted memory [{args.id}]", "33"))
        return EXIT_OK

    conn.execute(
        "UPDATE memories SET deleted_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (args.id,),
    )
    conn.commit()
    if args.json:
        print(json.dumps({"deleted": args.id, "hard": False}, indent=2))
    else:
        print(color(f"Soft-deleted memory [{args.id}]", "33"))
    return EXIT_OK


def cmd_verify(args, conn):
    """Verify an auto-extracted memory — sets verified_at, removes expires_at."""
    row = conn.execute("SELECT * FROM memories WHERE id = ? AND deleted_at IS NULL", (args.id,)).fetchone()
    if row is None:
        return fail(f"memory {args.id} not found", code=EXIT_NOT_FOUND)
    if row['verified_at']:
        if args.json:
            print(json.dumps({"id": args.id, "already_verified": True, "verified_at": row['verified_at']}, indent=2))
        else:
            print(color(f"Memory [{args.id}] already verified at {row['verified_at']}", "33"))
        return EXIT_OK
    conn.execute(
        """
        UPDATE memories
        SET verified_at = CURRENT_TIMESTAMP,
            expires_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (args.id,),
    )
    conn.commit()
    updated = conn.execute("SELECT * FROM memories WHERE id = ?", (args.id,)).fetchone()
    if args.json:
        print(json.dumps(memory_to_json(updated), indent=2))
    else:
        print(color(f"Verified memory [{args.id}] — now permanent, included in bootstrap", "32"))
    return EXIT_OK

def task_to_json(row):
    return row_to_dict(row)


def cmd_tasks(args, conn):
    if args.tasks_command == "add":
        due_date = None
        if args.due:
            due_date = parse_relative_date(args.due)
            if not due_date:
                return fail(f"could not parse due date '{args.due}'")
        entity_id = args.entity_id
        if args.entity_name:
            found = conn.execute("SELECT id FROM entities WHERE name = ?", (args.entity_name,)).fetchone()
            if not found:
                return fail(f"entity '{args.entity_name}' not found", code=EXIT_NOT_FOUND)
            entity_id = found["id"]
        tags = normalize_tags(args.tags)
        cur = conn.execute(
            """
            INSERT INTO tasks(title, description, status, priority, due_date, repeat_rule, entity_id, tags)
            VALUES (?, ?, 'pending', ?, ?, ?, ?, ?)
            """,
            (args.title, args.description, args.priority, due_date, args.repeat_rule, entity_id, tags),
        )
        conn.commit()
        created = conn.execute("SELECT * FROM tasks WHERE id = ?", (cur.lastrowid,)).fetchone()
        if args.json:
            print(json.dumps(task_to_json(created), indent=2))
        else:
            print(color(f"Created task [{created['id']}]", "32"))
        return EXIT_OK

    if args.tasks_command in {"done", "cancel"}:
        status = "done" if args.tasks_command == "done" else "cancelled"
        row = conn.execute("SELECT * FROM tasks WHERE id = ?", (args.id,)).fetchone()
        if row is None:
            return fail(f"task {args.id} not found", code=EXIT_NOT_FOUND)
        completed_at = "CURRENT_TIMESTAMP" if status == "done" else "NULL"
        sql = f"UPDATE tasks SET status = ?, completed_at = {completed_at} WHERE id = ?"
        conn.execute(sql, (status, args.id))
        conn.commit()
        if args.json:
            print(json.dumps({"id": args.id, "status": status}, indent=2))
        else:
            print(color(f"Task [{args.id}] -> {status}", "32"))
        return EXIT_OK

    clauses = []
    values = []
    if args.all:
        pass
    elif args.due:
        clauses.append("status IN ('pending','active')")
        clauses.append("due_date IS NOT NULL")
        limit = (dt.datetime.now() + dt.timedelta(hours=24)).isoformat(timespec="seconds")
        clauses.append("due_date <= ?")
        values.append(limit)
    else:
        clauses.append("status = 'pending'")

    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT t.*, e.name AS entity_name
        FROM tasks t
        LEFT JOIN entities e ON e.id = t.entity_id
        {where_sql}
        ORDER BY (due_date IS NULL), due_date ASC, created_at DESC
    """
    rows = conn.execute(sql, tuple(values)).fetchall()

    if args.json:
        print(json.dumps([row_to_dict(row) for row in rows], indent=2))
        return EXIT_OK

    if not rows:
        print("No tasks found.")
        return EXIT_OK

    now = dt.datetime.now()
    for row in rows:
        badge = color(f"[{row['id']}]", "36")
        due_label = "-"
        if row["due_date"]:
            due_parsed = parse_dt(row["due_date"])
            if due_parsed and due_parsed < now and row["status"] in {"pending", "active"}:
                due_label = color(f"{humanize_datetime(row['due_date'])} (overdue)", "31")
            else:
                due_label = humanize_datetime(row["due_date"])
        print(f"{badge} {row['title']}  status={row['status']}  priority={row['priority']}  due={due_label}")
        if row["description"]:
            print(wrap_block("  desc: ", row["description"], width=80))
        if row["entity_name"]:
            print(f"  entity: {row['entity_name']}")
        if row["tags"]:
            print(f"  tags: {row['tags']}")
        print()
    return EXIT_OK


def cmd_remind(args, conn):
    due = parse_relative_date(args.when)
    if not due:
        return fail(f"could not parse date expression '{args.when}'")
    cur = conn.execute(
        "INSERT INTO tasks(title, status, priority, due_date) VALUES (?, 'pending', 'medium', ?)",
        (args.title, due),
    )
    conn.commit()
    task_id = cur.lastrowid
    if args.json:
        row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        print(json.dumps(row_to_dict(row), indent=2))
    else:
        print(color(f"Reminder task [{task_id}] due {due}", "32"))
    return EXIT_OK


def cmd_entities(args, conn):
    if args.entities_command == "add":
        metadata = None
        if args.metadata:
            try:
                metadata = json.dumps(json.loads(args.metadata), separators=(",", ":"))
            except json.JSONDecodeError as exc:
                return fail(f"invalid metadata JSON: {exc}")
        try:
            cur = conn.execute(
                "INSERT INTO entities(type, name, metadata) VALUES (?, ?, ?)",
                (args.type, args.name, metadata),
            )
        except sqlite3.IntegrityError:
            return fail(f"entity '{args.name}' already exists")
        conn.commit()
        if args.json:
            row = conn.execute("SELECT * FROM entities WHERE id = ?", (cur.lastrowid,)).fetchone()
            payload = row_to_dict(row)
            if payload.get("metadata"):
                payload["metadata"] = json.loads(payload["metadata"])
            print(json.dumps(payload, indent=2))
        else:
            print(color(f"Created entity [{cur.lastrowid}] {args.name}", "32"))
        return EXIT_OK

    if args.entities_command == "show":
        entity = conn.execute("SELECT * FROM entities WHERE name = ?", (args.name,)).fetchone()
        if entity is None:
            return fail(f"entity '{args.name}' not found", code=EXIT_NOT_FOUND)
        memories = conn.execute(
            """
            SELECT * FROM memories
            WHERE deleted_at IS NULL
              AND (scope = ? OR (',' || tags || ',' LIKE ?))
            ORDER BY created_at DESC
            LIMIT 50
            """,
            (args.name, f"%,{args.name.strip()},%"),
        ).fetchall()
        entity_payload = row_to_dict(entity)
        if entity_payload.get("metadata"):
            entity_payload["metadata"] = json.loads(entity_payload["metadata"])
        payload = {
            "entity": entity_payload,
            "memories": [memory_to_json(row) for row in memories],
        }
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            print(color(f"[{entity['id']}] {entity['name']} ({entity['type']})", "36"))
            metadata = entity_payload.get("metadata")
            if metadata is not None:
                print(f"  metadata: {json.dumps(metadata, ensure_ascii=True)}")
            print(f"  created: {humanize_datetime(entity['created_at'])}")
            if memories:
                print("  linked memories:")
                for row in memories:
                    print(f"    [{row['id']}] {row['type']}: {row['content']}")
            else:
                print("  linked memories: none")
        return EXIT_OK

    if args.entities_command == "update":
        entity = conn.execute("SELECT * FROM entities WHERE name = ?", (args.name,)).fetchone()
        if entity is None:
            return fail(f"entity '{args.name}' not found", code=EXIT_NOT_FOUND)
        updates = []
        values = []
        if args.type:
            updates.append("type = ?")
            values.append(args.type)
        if args.metadata is not None:
            try:
                metadata = json.dumps(json.loads(args.metadata), separators=(",", ":"))
            except json.JSONDecodeError as exc:
                return fail(f"invalid metadata JSON: {exc}")
            updates.append("metadata = ?")
            values.append(metadata)
        if not updates:
            return fail("nothing to update")
        updates.append("updated_at = CURRENT_TIMESTAMP")
        sql = "UPDATE entities SET " + ", ".join(updates) + " WHERE id = ?"
        values.append(entity["id"])
        conn.execute(sql, tuple(values))
        conn.commit()
        updated = conn.execute("SELECT * FROM entities WHERE id = ?", (entity["id"],)).fetchone()
        payload = row_to_dict(updated)
        if payload.get("metadata"):
            payload["metadata"] = json.loads(payload["metadata"])
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            print(color(f"Updated entity [{updated['id']}] {updated['name']}", "32"))
        return EXIT_OK

    rows = conn.execute("SELECT * FROM entities ORDER BY name ASC").fetchall()
    if args.json:
        payload = []
        for row in rows:
            item = row_to_dict(row)
            if item.get("metadata"):
                item["metadata"] = json.loads(item["metadata"])
            payload.append(item)
        print(json.dumps(payload, indent=2))
        return EXIT_OK

    if not rows:
        print("No entities found.")
        return EXIT_OK

    for row in rows:
        meta = ""
        if row["metadata"]:
            meta = f"  metadata={row['metadata']}"
        print(f"[{row['id']}] {row['name']}  type={row['type']}{meta}")
    return EXIT_OK


def cmd_kv(args, conn):
    if args.kv_command == "set":
        try:
            parsed = json.loads(args.value)
        except json.JSONDecodeError:
            # Treat bare strings as plain string values
            parsed = args.value
        encoded = json.dumps(parsed, separators=(",", ":"))
        conn.execute(
            """
            INSERT INTO kv(key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """,
            (args.key, encoded),
        )
        conn.commit()
        if args.json:
            print(json.dumps({"key": args.key, "value": parsed}, indent=2))
        else:
            print(color(f"Set kv '{args.key}'", "32"))
        return EXIT_OK

    if args.kv_command == "get":
        row = conn.execute("SELECT * FROM kv WHERE key = ?", (args.key,)).fetchone()
        if row is None:
            return fail(f"key '{args.key}' not found", code=EXIT_NOT_FOUND)
        parsed = decode_json_value(row["value"])
        if args.json:
            print(json.dumps({"key": row["key"], "value": parsed, "updated_at": row["updated_at"]}, indent=2))
        else:
            print(f"{row['key']} = {json.dumps(parsed, ensure_ascii=True)}")
        return EXIT_OK

    if args.kv_command == "delete":
        cur = conn.execute("DELETE FROM kv WHERE key = ?", (args.key,))
        conn.commit()
        if cur.rowcount == 0:
            return fail(f"key '{args.key}' not found", code=EXIT_NOT_FOUND)
        if args.json:
            print(json.dumps({"deleted": args.key}, indent=2))
        else:
            print(color(f"Deleted kv '{args.key}'", "33"))
        return EXIT_OK

    rows = conn.execute("SELECT * FROM kv ORDER BY key ASC").fetchall()
    payload = []
    for row in rows:
        payload.append({
            "key": row["key"],
            "value": decode_json_value(row["value"]),
            "updated_at": row["updated_at"],
        })
    if args.json:
        print(json.dumps(payload, indent=2))
        return EXIT_OK

    if not payload:
        print("No kv entries found.")
        return EXIT_OK
    for item in payload:
        print(f"{item['key']} = {json.dumps(item['value'], ensure_ascii=True)}")
    return EXIT_OK


def cmd_session(args, conn):
    if args.session_command == "log":
        files_modified = None
        decisions_made = None
        if args.files:
            try:
                parsed_files = json.loads(args.files)
            except json.JSONDecodeError as exc:
                return fail(f"invalid files JSON: {exc}")
            if not isinstance(parsed_files, list):
                return fail("--files must be a JSON array")
            files_modified = json.dumps(parsed_files, separators=(",", ":"))
        if args.decisions:
            try:
                parsed_decisions = json.loads(args.decisions)
            except json.JSONDecodeError as exc:
                return fail(f"invalid decisions JSON: {exc}")
            if not isinstance(parsed_decisions, list):
                return fail("--decisions must be a JSON array")
            decisions_made = json.dumps(parsed_decisions, separators=(",", ":"))
        cur = conn.execute(
            """
            INSERT INTO session_refs(session_id, summary, files_modified, decisions_made)
            VALUES (?, ?, ?, ?)
            """,
            (args.id, args.summary, files_modified, decisions_made),
        )
        conn.commit()
        if args.json:
            row = conn.execute("SELECT * FROM session_refs WHERE id = ?", (cur.lastrowid,)).fetchone()
            data = row_to_dict(row)
            for field in ("files_modified", "decisions_made"):
                if data[field]:
                    data[field] = json.loads(data[field])
            print(json.dumps(data, indent=2))
        else:
            print(color(f"Logged session ref [{cur.lastrowid}] for {args.id}", "32"))
        # Auto-run pattern analysis on session log
        recurring = find_recurring_patterns(conn, threshold=3)
        if recurring:
            # Check for unproposed patterns
            proposed_key = 'patterns_proposed_tags'
            row = conn.execute("SELECT value FROM kv WHERE key = ?", (proposed_key,)).fetchone()
            already_proposed = set()
            if row:
                try:
                    already_proposed = set(json.loads(row['value']))
                except (json.JSONDecodeError, TypeError):
                    pass
            new_patterns = {t: m for t, m in recurring.items() if t not in already_proposed}
            if new_patterns:
                tags_str = ', '.join(f'{t} ({len(m)})' for t, m in new_patterns.items())
                print(color(f"  \u26a0 Recurring patterns detected: {tags_str}", '33'))
                print(f"  Run: nous-memory patterns suggest")
        return EXIT_OK

    if args.session_command == "show":
        row = conn.execute(
            "SELECT * FROM session_refs WHERE session_id = ? ORDER BY created_at DESC LIMIT 1",
            (args.session_id,),
        ).fetchone()
        if row is None:
            return fail(f"session '{args.session_id}' not found", code=EXIT_NOT_FOUND)
        data = row_to_dict(row)
        for field in ("files_modified", "decisions_made"):
            if data[field]:
                data[field] = json.loads(data[field])
            else:
                data[field] = []
        if args.json:
            print(json.dumps(data, indent=2))
        else:
            print(color(f"[{row['id']}] session={row['session_id']}", "36"))
            print(wrap_block("  summary: ", row["summary"] or "-", width=80))
            print(f"  files: {', '.join(data['files_modified']) if data['files_modified'] else '-'}")
            print(
                "  decisions: "
                + (", ".join(data["decisions_made"]) if data["decisions_made"] else "-")
            )
            print(f"  created: {humanize_datetime(row['created_at'])}")
        return EXIT_OK

    rows = conn.execute(
        "SELECT * FROM session_refs ORDER BY created_at DESC LIMIT ?",
        (args.limit,),
    ).fetchall()
    if args.json:
        data = []
        for row in rows:
            item = row_to_dict(row)
            for field in ("files_modified", "decisions_made"):
                if item[field]:
                    item[field] = json.loads(item[field])
                else:
                    item[field] = []
            data.append(item)
        print(json.dumps(data, indent=2))
        return EXIT_OK

    if not rows:
        print("No session refs found.")
        return EXIT_OK

    for row in rows:
        summary = row["summary"] or "-"
        print(f"[{row['id']}] {row['session_id']}  created={humanize_datetime(row['created_at'])}")
        print(wrap_block("  summary: ", summary, width=80))
    return EXIT_OK


def _active_episode_map(conn):
    rows = conn.execute(
        """
        SELECT key, value
        FROM kv
        WHERE key LIKE 'active_episode:%'
        ORDER BY key ASC
        """
    ).fetchall()
    mapping = {}
    for row in rows:
        scope = row["key"].split(":", 1)[1]
        value = decode_json_value(row["value"])
        if isinstance(value, str) and value:
            mapping[scope] = value
    return mapping


def _format_duration(started_at, ended_at=None, now=None):
    start_dt = parse_dt(started_at)
    end_dt = parse_dt(ended_at) if ended_at else (now or dt.datetime.now())
    if start_dt is None or end_dt is None:
        return "-"
    total = max(0, int((end_dt - start_dt).total_seconds()))
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    if days > 0:
        return f"{days}d {hours}h"
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def cmd_episode(args, conn):
    if args.episode_command == "start":
        active = conn.execute(
            """
            SELECT id
            FROM episodes
            WHERE scope = ? AND status = 'active'
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (args.scope,),
        ).fetchone()
        if active is not None:
            return fail(f"active episode already exists for scope '{args.scope}': {active['id']}")

        scope_slug = "".join(c if c.isalnum() or c in "-_" else "-" for c in args.scope)
        started_at = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        episode_id = f"ep_{scope_slug}_{started_at}"
        conn.execute(
            """
            INSERT INTO episodes(id, scope, intent, status, started_at)
            VALUES (?, ?, ?, 'active', CURRENT_TIMESTAMP)
            """,
            (episode_id, args.scope, args.intent),
        )
        conn.execute(
            """
            INSERT INTO kv(key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """,
            (f"active_episode:{args.scope}", json.dumps(episode_id)),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM episodes WHERE id = ?", (episode_id,)).fetchone()
        if args.json:
            data = row_to_dict(row)
            data["duration"] = _format_duration(row["started_at"], row["ended_at"])
            print(json.dumps(data, indent=2))
        else:
            print(color(f"Started episode {episode_id}", "32"))
        return EXIT_OK

    if args.episode_command == "end":
        target = None
        target_scope = args.scope
        active_map = _active_episode_map(conn)

        if target_scope:
            episode_id = active_map.get(target_scope)
            if episode_id:
                target = conn.execute(
                    "SELECT * FROM episodes WHERE id = ?",
                    (episode_id,),
                ).fetchone()
            if target is None:
                target = conn.execute(
                    """
                    SELECT *
                    FROM episodes
                    WHERE scope = ? AND status = 'active'
                    ORDER BY started_at DESC
                    LIMIT 1
                    """,
                    (target_scope,),
                ).fetchone()
        else:
            if len(active_map) == 1:
                only_scope, only_id = next(iter(active_map.items()))
                target_scope = only_scope
                target = conn.execute(
                    "SELECT * FROM episodes WHERE id = ?",
                    (only_id,),
                ).fetchone()
            elif len(active_map) > 1:
                scopes = ", ".join(sorted(active_map))
                return fail(f"multiple active episodes found ({scopes}); use --scope")
            else:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM episodes
                    WHERE status = 'active'
                    ORDER BY started_at DESC
                    LIMIT 2
                    """
                ).fetchall()
                if not rows:
                    return fail("no active episode found", code=EXIT_NOT_FOUND)
                if len(rows) > 1:
                    scopes = ", ".join(sorted({row["scope"] for row in rows}))
                    return fail(f"multiple active episodes found ({scopes}); use --scope")
                target = rows[0]
                target_scope = target["scope"]

        if target is None:
            return fail(f"no active episode found for scope '{target_scope}'", code=EXIT_NOT_FOUND)

        if args.summary is not None:
            conn.execute(
                """
                UPDATE episodes
                SET ended_at = CURRENT_TIMESTAMP,
                    status = 'done',
                    summary = ?
                WHERE id = ?
                """,
                (args.summary, target["id"]),
            )
        else:
            conn.execute(
                """
                UPDATE episodes
                SET ended_at = CURRENT_TIMESTAMP,
                    status = 'done'
                WHERE id = ?
                """,
                (target["id"],),
            )
        conn.execute("DELETE FROM kv WHERE key = ?", (f"active_episode:{target_scope}",))
        conn.commit()

        row = conn.execute("SELECT * FROM episodes WHERE id = ?", (target["id"],)).fetchone()
        if args.json:
            data = row_to_dict(row)
            data["duration"] = _format_duration(row["started_at"], row["ended_at"])
            print(json.dumps(data, indent=2))
        else:
            print(color(f"Ended episode {row['id']}", "33"))
        return EXIT_OK

    if args.episode_command == "current":
        now = dt.datetime.now()
        if args.scope:
            episode_id = _active_episode_map(conn).get(args.scope)
            row = None
            if episode_id:
                row = conn.execute("SELECT * FROM episodes WHERE id = ?", (episode_id,)).fetchone()
            if row is None:
                row = conn.execute(
                    """
                    SELECT *
                    FROM episodes
                    WHERE scope = ? AND status = 'active'
                    ORDER BY started_at DESC
                    LIMIT 1
                    """,
                    (args.scope,),
                ).fetchone()
            if args.json:
                if row is None:
                    print(json.dumps(None))
                else:
                    data = row_to_dict(row)
                    data["duration"] = _format_duration(row["started_at"], row["ended_at"], now)
                    print(json.dumps(data, indent=2))
                return EXIT_OK
            if row is None:
                print(f"No active episode for scope '{args.scope}'.")
                return EXIT_OK
            print(
                f"{row['id']}  scope={row['scope']}  status={row['status']}  "
                f"started={humanize_datetime(row['started_at'], now)}  "
                f"duration={_format_duration(row['started_at'], row['ended_at'], now)}"
            )
            if row["intent"]:
                print(wrap_block("  intent: ", row["intent"], width=80))
            return EXIT_OK

        rows = conn.execute(
            """
            SELECT *
            FROM episodes
            WHERE status = 'active'
            ORDER BY started_at DESC
            """
        ).fetchall()
        if args.json:
            data = []
            for row in rows:
                item = row_to_dict(row)
                item["duration"] = _format_duration(row["started_at"], row["ended_at"], now)
                data.append(item)
            print(json.dumps(data, indent=2))
            return EXIT_OK
        if not rows:
            print("No active episodes.")
            return EXIT_OK
        for row in rows:
            print(
                f"{row['id']}  scope={row['scope']}  status={row['status']}  "
                f"started={humanize_datetime(row['started_at'], now)}  "
                f"duration={_format_duration(row['started_at'], row['ended_at'], now)}"
            )
            if row["intent"]:
                print(wrap_block("  intent: ", row["intent"], width=80))
        return EXIT_OK

    rows = conn.execute(
        f"""
        SELECT *
        FROM episodes
        {"WHERE scope = ?" if args.scope else ""}
        ORDER BY started_at DESC
        LIMIT ?
        """,
        (args.scope, args.limit) if args.scope else (args.limit,),
    ).fetchall()
    now = dt.datetime.now()
    if args.json:
        data = []
        for row in rows:
            item = row_to_dict(row)
            item["duration"] = _format_duration(row["started_at"], row["ended_at"], now)
            data.append(item)
        print(json.dumps(data, indent=2))
        return EXIT_OK

    if not rows:
        print("No episodes found.")
        return EXIT_OK
    for row in rows:
        print(
            f"{row['id']}  scope={row['scope']}  status={row['status']}  "
            f"started={row['started_at']}  duration={_format_duration(row['started_at'], row['ended_at'], now)}"
        )
        if row["intent"]:
            print(wrap_block("  intent: ", row["intent"], width=80))
    return EXIT_OK


def cmd_stats(args, conn):
    memories_total = conn.execute("SELECT COUNT(*) AS c FROM memories").fetchone()["c"]
    memories_active = conn.execute("SELECT COUNT(*) AS c FROM memories WHERE deleted_at IS NULL").fetchone()["c"]
    memories_deleted = conn.execute("SELECT COUNT(*) AS c FROM memories WHERE deleted_at IS NOT NULL").fetchone()["c"]
    memory_breakdown_rows = conn.execute(
        "SELECT type, COUNT(*) AS c FROM memories GROUP BY type ORDER BY type"
    ).fetchall()
    memory_breakdown = {row["type"]: row["c"] for row in memory_breakdown_rows}
    topic_key_count = conn.execute(
        "SELECT COUNT(*) AS c FROM memories WHERE topic_key IS NOT NULL AND topic_key <> ''"
    ).fetchone()["c"]
    revised_count = conn.execute(
        "SELECT COUNT(*) AS c FROM memories WHERE COALESCE(revision_count, 1) > 1"
    ).fetchone()["c"]
    duplicate_total = conn.execute(
        "SELECT COALESCE(SUM(duplicate_count), 0) AS c FROM memories"
    ).fetchone()["c"]

    schema_version_row = conn.execute("SELECT value FROM kv WHERE key = 'schema_version'").fetchone()
    schema_version = 0
    if schema_version_row is not None:
        try:
            decoded_version = decode_json_value(schema_version_row["value"])
            if isinstance(decoded_version, (int, float, str, bool)):
                schema_version = int(decoded_version)
            else:
                schema_version = 0
        except (ValueError, TypeError, json.JSONDecodeError):
            schema_version = 0

    fts_enabled = table_exists(conn, "memories_fts")
    fts_rows = 0
    if fts_enabled:
        fts_rows = conn.execute("SELECT COUNT(*) AS c FROM memories_fts").fetchone()["c"]

    task_rows = conn.execute("SELECT status, COUNT(*) AS c FROM tasks GROUP BY status").fetchall()
    task_counts = {row["status"]: row["c"] for row in task_rows}

    overdue_count = conn.execute(
        """
        SELECT COUNT(*) AS c FROM tasks
        WHERE status IN ('pending', 'active')
          AND due_date IS NOT NULL
          AND due_date < ?
        """,
        (dt.datetime.now().isoformat(timespec="seconds"),),
    ).fetchone()["c"]

    entities_count = conn.execute("SELECT COUNT(*) AS c FROM entities").fetchone()["c"]
    sessions_count = conn.execute("SELECT COUNT(*) AS c FROM session_refs").fetchone()["c"]

    db_size = args.db.stat().st_size if args.db.exists() else 0
    payload = {
        "memories": {
            "total": memories_total,
            "active": memories_active,
            "deleted": memories_deleted,
            "with_topic_key": topic_key_count,
            "revised": revised_count,
            "duplicate_total": duplicate_total,
            "by_type": memory_breakdown,
        },
        "search": {
            "schema_version": schema_version,
            "fts5_enabled": fts_enabled,
            "fts_rows": fts_rows,
        },
        "tasks": {
            "pending": task_counts.get("pending", 0),
            "active": task_counts.get("active", 0),
            "done": task_counts.get("done", 0),
            "cancelled": task_counts.get("cancelled", 0),
            "overdue": overdue_count,
        },
        "entities": entities_count,
        "sessions": sessions_count,
        "db_size_bytes": db_size,
    }

    if args.json:
        print(json.dumps(payload, indent=2))
        return EXIT_OK

    parts = []
    for key in MEMORY_TYPES:
        if key in memory_breakdown:
            parts.append(f"{memory_breakdown[key]} {key}s")
    mem_details = ", ".join(parts) if parts else "none"
    print(f"Memories: {memories_total} ({mem_details})")
    print(
        "Memory detail: "
        f"{memories_active} active, "
        f"{memories_deleted} deleted, "
        f"{topic_key_count} topic-keyed, "
        f"{revised_count} revised, "
        f"dup_total={duplicate_total}"
    )
    print(f"Search: schema=v{schema_version}, fts5={'on' if fts_enabled else 'off'}, fts_rows={fts_rows}")
    print(
        "Tasks: "
        f"{task_counts.get('pending', 0)} pending, "
        f"{task_counts.get('done', 0)} done, "
        f"{overdue_count} overdue"
    )
    print(f"Entities: {entities_count}")
    print(f"Sessions: {sessions_count}")
    print(f"DB size: {db_size // 1024 if db_size >= 1024 else db_size}{'KB' if db_size >= 1024 else 'B'}")
    return EXIT_OK


# --- Pattern Detection & Self-Improvement ---


def cluster_memories_by_tags(memories):
    """Group memories by their tags, counting tag frequency."""
    tag_clusters = {}  # tag -> list of memory dicts
    for mem in memories:
        tags = split_tags(mem.get('tags', []))
        if isinstance(mem.get('tags'), list):
            tags = mem['tags']
        for tag in tags:
            tag_lower = tag.lower().strip()
            if not tag_lower:
                continue
            if tag_lower not in tag_clusters:
                tag_clusters[tag_lower] = []
            tag_clusters[tag_lower].append(mem)
    return tag_clusters


def find_recurring_patterns(conn, threshold=3, types=None):
    """Find tag clusters that appear >= threshold times across failure/pattern memories."""
    if types is None:
        types = ('failure', 'pattern')
    placeholders = ','.join('?' * len(types))
    rows = conn.execute(
        f"""
        SELECT * FROM memories
        WHERE type IN ({placeholders})
          AND deleted_at IS NULL
          AND superseded_by IS NULL
          AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        ORDER BY created_at DESC
        """,
        types,
    ).fetchall()
    memories = [memory_to_json(row) for row in rows]
    clusters = cluster_memories_by_tags(memories)
    # Filter to clusters meeting threshold
    recurring = {}
    for tag, mems in clusters.items():
        if len(mems) >= threshold:
            recurring[tag] = mems
    return recurring


def get_synced_ids(conn):
    """Get set of memory IDs already synced to knowledge files."""
    row = conn.execute("SELECT value FROM kv WHERE key = ?", ('patterns_synced_ids',)).fetchone()
    if row is None:
        return set()
    try:
        data = json.loads(row['value'])
        return set(data) if isinstance(data, list) else set()
    except (json.JSONDecodeError, TypeError):
        return set()


def save_synced_ids(conn, ids):
    """Save synced memory IDs to kv store."""
    conn.execute(
        "INSERT INTO kv(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
        ('patterns_synced_ids', json.dumps(sorted(ids))),
    )


def sync_knowledge_file(filepath, memories, synced_ids):
    """Append new memories to a knowledge markdown file. Returns set of newly synced IDs."""
    new_ids = set()
    new_entries = []
    for mem in memories:
        mid = mem['id']
        if mid in synced_ids:
            continue
        new_ids.add(mid)
        created = str(mem.get('created_at', 'unknown'))[:10]
        content = mem['content']
        tags_list = mem.get('tags', [])
        tags = ', '.join(tags_list) if isinstance(tags_list, list) else str(tags_list)
        scope = mem.get('scope', 'global')
        entry = (
            f"### [{created}] {content[:80]}{'...' if len(content) > 80 else ''}\n"
            f"- **Full**: {content}\n"
            f"- **Scope**: {scope}\n"
            f"- **Tags**: {tags}\n"
            f"- **Memory ID**: {mid}\n"
        )
        new_entries.append(entry)

    if not new_entries:
        return new_ids

    existing = Path(filepath).read_text() if Path(filepath).exists() else ''
    with open(filepath, 'a') as f:
        if not existing.endswith('\n'):
            f.write('\n')
        for entry in new_entries:
            f.write(f'\n{entry}\n')

    return new_ids


def cmd_patterns(args, conn):
    if args.patterns_command == 'analyze':
        threshold = args.threshold
        types = ('failure', 'pattern')
        if args.include_all:
            types = MEMORY_TYPES
        recurring = find_recurring_patterns(conn, threshold=threshold, types=types)

        if args.json:
            payload = {}
            for tag, mems in sorted(recurring.items(), key=lambda x: -len(x[1])):
                payload[tag] = {
                    'count': len(mems),
                    'memories': mems,
                }
            print(json.dumps({'threshold': threshold, 'clusters': payload}, indent=2))
            return EXIT_OK

        if not recurring:
            print(f'No recurring patterns found (threshold={threshold}).')
            return EXIT_OK

        print(color(f'Recurring patterns (threshold >= {threshold}):', '1'))
        print()
        for tag, mems in sorted(recurring.items(), key=lambda x: -len(x[1])):
            print(color(f'  [{tag}] \u2014 {len(mems)} occurrences', '36'))
            for mem in mems[:5]:  # Show up to 5
                content_preview = mem['content'][:100]
                print(f'    [{mem["id"]}] {mem["type"]}: {content_preview}')
            if len(mems) > 5:
                print(f'    ... and {len(mems) - 5} more')
            print()
        return EXIT_OK

    if args.patterns_command == 'suggest':
        threshold = args.threshold
        recurring = find_recurring_patterns(conn, threshold=threshold)
        if not recurring:
            print('No patterns meeting threshold for suggestion.')
            return EXIT_OK

        # Check which clusters haven't been proposed yet
        proposed_key = 'patterns_proposed_tags'
        row = conn.execute("SELECT value FROM kv WHERE key = ?", (proposed_key,)).fetchone()
        already_proposed = set()
        if row:
            try:
                already_proposed = set(json.loads(row['value']))
            except (json.JSONDecodeError, TypeError):
                pass

        new_clusters = {
            tag: mems for tag, mems in recurring.items()
            if tag not in already_proposed
        }

        if not new_clusters:
            print('All recurring patterns already have proposals.')
            return EXIT_OK

        suggestions = []
        for tag, mems in sorted(new_clusters.items(), key=lambda x: -len(x[1])):
            failure_count = sum(1 for m in mems if m['type'] == 'failure')
            pattern_count = sum(1 for m in mems if m['type'] == 'pattern')
            summaries = [m['content'][:120] for m in mems[:5]]
            suggestion = {
                'tag': tag,
                'total': len(mems),
                'failures': failure_count,
                'patterns': pattern_count,
                'evidence': summaries,
            }
            suggestions.append(suggestion)

        if args.json:
            print(json.dumps({'suggestions': suggestions}, indent=2))
            return EXIT_OK

        print(color(f'Pattern suggestions ({len(suggestions)} new):', '1'))
        print()
        for s in suggestions:
            kind = 'failures' if s['failures'] > s['patterns'] else 'patterns'
            print(color(f"  [{s['tag']}] \u2014 {s['total']} {kind}", '33'))
            for ev in s['evidence']:
                print(f'    \u2022 {ev}')
            print()
        print('Run `nous-memory prompt propose "<description>"` to create a proposal,')
        print('or `nous-memory patterns propose <tag>` to auto-generate one.')
        return EXIT_OK

    if args.patterns_command == 'propose':
        tag = args.tag.lower().strip()
        recurring = find_recurring_patterns(conn, threshold=args.threshold)
        if tag not in recurring:
            return fail(f"tag '{tag}' not found in recurring patterns (threshold={args.threshold})")

        mems = recurring[tag]
        failure_count = sum(1 for m in mems if m['type'] == 'failure')
        pattern_count = sum(1 for m in mems if m['type'] == 'pattern')

        # Build proposal content
        now_str = dt.datetime.now().isoformat(timespec='seconds')
        evidence_lines = []
        for m in mems:
            evidence_lines.append(f"- [{m['id']}] ({m['type']}, {m.get('scope', 'global')}): {m['content']}")

        paths = get_prompt_paths(getattr(args, "workspace", None))
        proposal_id = next_proposal_id(paths)
        slug = slugify(f'pattern-{tag}')
        filename = f'{proposal_id:03d}-{slug}.md'
        paths['proposals_dir'].mkdir(parents=True, exist_ok=True)
        proposal_path = paths['proposals_dir'] / filename

        content = (
            f'# Proposal {proposal_id}: Self-improvement from pattern [{tag}]\n\n'
            f'**Date**: {now_str}\n'
            f'**Status**: pending\n'
            f'**Source**: Auto-generated by `nous-memory patterns propose {tag}`\n\n'
            f'## Observation\n\n'
            f'Detected {len(mems)} occurrences of tag `{tag}` across memories '
            f'({failure_count} failures, {pattern_count} patterns).\n\n'
            f'### Evidence\n\n'
            + '\n'.join(evidence_lines) + '\n\n'
            f'## Proposed Changes\n\n'
            f'<!-- Describe what should change in AGENTS.md based on this pattern -->\n\n'
            f'## New AGENTS.md Content\n\n'
            f'<!-- Paste the full new AGENTS.md content below this line -->\n'
        )
        proposal_path.write_text(content)

        # Register in kv
        conn.execute(
            "INSERT INTO kv(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
            (f'proposal.{proposal_id}', json.dumps({
                'id': proposal_id,
                'description': f'Self-improvement from pattern [{tag}]',
                'file': str(proposal_path),
                'status': 'pending',
                'created': now_str,
                'auto_generated': True,
                'source_tag': tag,
                'evidence_count': len(mems),
            })),
        )

        # Mark tag as proposed
        proposed_key = 'patterns_proposed_tags'
        row = conn.execute("SELECT value FROM kv WHERE key = ?", (proposed_key,)).fetchone()
        proposed = set()
        if row:
            try:
                proposed = set(json.loads(row['value']))
            except (json.JSONDecodeError, TypeError):
                pass
        proposed.add(tag)
        conn.execute(
            "INSERT INTO kv(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
            (proposed_key, json.dumps(sorted(proposed))),
        )
        conn.commit()

        if args.json:
            print(json.dumps({'proposal_id': proposal_id, 'file': str(proposal_path), 'tag': tag}, indent=2))
        else:
            print(color(f'Created proposal [{proposal_id}] from pattern [{tag}]', '32'))
            print(f'  Evidence: {len(mems)} memories ({failure_count} failures, {pattern_count} patterns)')
            print(f'  File: {proposal_path}')
            print(f'  Next: edit the proposal, then `nous-memory prompt apply {proposal_id}`')
        return EXIT_OK

    if args.patterns_command == 'sync':
        brain_dir = resolve_workspace(getattr(args, 'workspace', None)) / 'brain'
        failures_path = brain_dir / 'knowledge' / 'failures.md'
        patterns_path = brain_dir / 'knowledge' / 'patterns.md'

        # Get all failure and pattern memories
        failure_rows = conn.execute(
            """SELECT * FROM memories WHERE type = 'failure'
            AND deleted_at IS NULL
            AND superseded_by IS NULL
            AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
            ORDER BY created_at ASC""",
        ).fetchall()
        pattern_rows = conn.execute(
            """SELECT * FROM memories WHERE type = 'pattern'
            AND deleted_at IS NULL
            AND superseded_by IS NULL
            AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
            ORDER BY created_at ASC""",
        ).fetchall()

        synced_ids = get_synced_ids(conn)
        failure_mems = [memory_to_json(r) for r in failure_rows]
        pattern_mems = [memory_to_json(r) for r in pattern_rows]

        new_failure_ids = sync_knowledge_file(failures_path, failure_mems, synced_ids)
        new_pattern_ids = sync_knowledge_file(patterns_path, pattern_mems, synced_ids)

        all_new = new_failure_ids | new_pattern_ids
        synced_ids.update(all_new)
        save_synced_ids(conn, synced_ids)
        conn.commit()

        if args.json:
            print(json.dumps({
                'failures_synced': len(new_failure_ids),
                'patterns_synced': len(new_pattern_ids),
                'total_synced': len(synced_ids),
            }, indent=2))
        else:
            print(color(f'Synced {len(new_failure_ids)} failures, {len(new_pattern_ids)} patterns to knowledge files', '32'))
            if all_new:
                print(f'  New IDs: {sorted(all_new)}')
            else:
                print('  No new memories to sync.')
        return EXIT_OK

    return fail('unknown patterns command')


# --- Prompt Management ---


def get_prompt_paths(workspace=None):
    """Resolve prompt management paths relative to workspace."""
    repo_root = resolve_workspace(workspace)
    brain_dir = repo_root / "brain"
    return {
        "active": repo_root / "AGENTS.md",
        "versions_dir": brain_dir / "prompts" / "versions",
        "proposals_dir": brain_dir / "prompts" / "proposals",
        "manifest": brain_dir / "prompts" / "versions" / "manifest.json",
    }


def load_manifest(paths):
    mpath = paths["manifest"]
    if mpath.exists():
        return json.loads(mpath.read_text())
    return []


def save_manifest(paths, manifest):
    paths["manifest"].write_text(json.dumps(manifest, indent=2) + "\n")


def next_version(manifest):
    if not manifest:
        return 1
    return max(e["version"] for e in manifest) + 1


def next_proposal_id(paths):
    proposals_dir = paths["proposals_dir"]
    proposals_dir.mkdir(parents=True, exist_ok=True)
    existing = sorted(proposals_dir.glob("*.md"))
    if not existing:
        return 1
    for f in reversed(existing):
        parts = f.stem.split("-", 1)
        if parts[0].isdigit():
            return int(parts[0]) + 1
    return 1


def slugify(text, max_len=40):
    slug = text.lower().strip()
    slug = ''.join(c if c.isalnum() or c in ' -' else '' for c in slug)
    slug = '-'.join(slug.split())
    return slug[:max_len].rstrip('-')


def get_bridge_paths(workspace=None):
    repo_root = resolve_workspace(workspace)
    brain_dir = repo_root / 'brain'
    return {
        'brain_dir': brain_dir,
        'template': brain_dir / 'bridge' / 'AGENTS.md.template',
        'project_briefs_dir': brain_dir / 'knowledge' / 'projects',
    }


def load_bridge_registry(conn):
    row = conn.execute("SELECT value FROM kv WHERE key = ?", ('bridge_registry',)).fetchone()
    if row is None:
        return {'projects': {}}
    try:
        data = json.loads(row['value'])
    except json.JSONDecodeError:
        return {'projects': {}}
    if not isinstance(data, dict):
        return {'projects': {}}
    if not isinstance(data.get('projects'), dict):
        data['projects'] = {}
    return data


def save_bridge_registry(conn, data):
    conn.execute(
        "INSERT INTO kv(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
        ('bridge_registry', json.dumps(data, separators=(',', ':'))),
    )


def render_bridge_content(project_dir, paths):
    template = paths['template']
    if not template.exists():
        return None
    project = project_dir.name
    brain = 'brain'
    project_context = ''
    brief = paths['project_briefs_dir'] / f'{project}.md'
    if brief.exists():
        project_context = f"Read `brain/knowledge/projects/{project}.md` for project context."
    content = template.read_text()
    content = content.replace('{{PROJECT}}', project)
    content = content.replace('{{BRAIN}}', brain)
    content = content.replace('{{PROJECT_CONTEXT}}', project_context)
    return content


def cmd_bridge(args, conn):
    paths = get_bridge_paths(getattr(args, "workspace", None))
    if not paths['template'].exists():
        return fail(f"bridge template not found at {paths['template']}")

    registry = load_bridge_registry(conn)

    if args.bridge_command == 'generate':
        project_dir = Path(args.project_dir).expanduser().resolve()
        if not project_dir.exists() or not project_dir.is_dir():
            return fail(f'project directory not found: {project_dir}')
        agents_file = project_dir / 'AGENTS.md'
        if agents_file.exists() and not args.force:
            print(color(f'Warning: {agents_file} exists; use --force to overwrite', '33'))
            return EXIT_ERROR
        content = render_bridge_content(project_dir, paths)
        if content is None:
            return fail(f"bridge template not found at {paths['template']}")
        agents_file.write_text(content)
        # Create brain symlink if not present
        brain_link = project_dir / 'brain'
        if not brain_link.exists():
            import os
            os.symlink(str(paths['brain_dir']), str(brain_link))
            print(color(f'  symlink: brain -> {paths["brain_dir"]}', '90'))
        generated_at = dt.datetime.now().isoformat(timespec='seconds')
        registry['projects'][str(project_dir)] = {
            'project': project_dir.name,
            'generated_at': generated_at,
        }
        save_bridge_registry(conn, registry)
        conn.commit()
        if args.json:
            print(json.dumps({
                'project_dir': str(project_dir),
                'project': project_dir.name,
                'agents_file': str(agents_file),
                'generated_at': generated_at,
            }, indent=2))
        else:
            print(color(f'Generated bridge AGENTS.md for {project_dir.name}', '32'))
            print(f'  path: {agents_file}')
        return EXIT_OK

    if args.bridge_command == 'sync':
        projects = registry.get('projects', {})
        if not projects:
            if args.json:
                print(json.dumps({'bridges': []}, indent=2))
            else:
                print('No bridge projects registered.')
            return EXIT_OK
        rows = []
        updated = False
        for path in sorted(projects.keys()):
            project_dir = Path(path)
            if not project_dir.exists() or not project_dir.is_dir():
                rows.append({
                    'project_dir': path,
                    'project': projects[path].get('project', project_dir.name),
                    'status': 'missing',
                })
                continue
            content = render_bridge_content(project_dir, paths)
            if content is None:
                return fail(f"bridge template not found at {paths['template']}")
            agents_file = project_dir / 'AGENTS.md'
            agents_file.write_text(content)
            generated_at = dt.datetime.now().isoformat(timespec='seconds')
            projects[path] = {
                'project': project_dir.name,
                'generated_at': generated_at,
            }
            rows.append({
                'project_dir': path,
                'project': project_dir.name,
                'status': 'synced',
                'agents_file': str(agents_file),
                'generated_at': generated_at,
            })
            updated = True
        if updated:
            save_bridge_registry(conn, registry)
            conn.commit()
        if args.json:
            print(json.dumps({'bridges': rows}, indent=2))
        else:
            for row in rows:
                sc = '32' if row['status'] == 'synced' else '33'
                print(f"[{color(row['status'], sc)}] {row['project']}  {row['project_dir']}")
        return EXIT_OK

    if args.bridge_command == 'list':
        projects = registry.get('projects', {})
        rows = []
        for path in sorted(projects.keys()):
            item = projects[path]
            status = 'exists' if (Path(path) / 'AGENTS.md').exists() else 'missing'
            rows.append({
                'project_dir': path,
                'project': item.get('project'),
                'generated_at': item.get('generated_at'),
                'status': status,
            })
        if args.json:
            print(json.dumps({'bridges': rows}, indent=2))
            return EXIT_OK
        if not rows:
            print('No bridge projects registered.')
            return EXIT_OK
        for row in rows:
            sc = '32' if row['status'] == 'exists' else '33'
            when = humanize_datetime(row['generated_at']) if row['generated_at'] else '-'
            print(f"[{color(row['status'], sc)}] {row['project']}  {row['project_dir']}")
            print(f'  generated: {when}')
        return EXIT_OK

    if args.bridge_command == 'remove':
        project_dir = Path(args.project_dir).expanduser().resolve()
        key = str(project_dir)
        projects = registry.get('projects', {})
        if key not in projects:
            return fail(f'bridge not found for: {project_dir}', code=EXIT_NOT_FOUND)
        agents_file = project_dir / 'AGENTS.md'
        removed_file = False
        if agents_file.exists():
            agents_file.unlink()
            removed_file = True
        del projects[key]
        save_bridge_registry(conn, registry)
        conn.commit()
        if args.json:
            print(json.dumps({
                'project_dir': key,
                'removed_file': removed_file,
                'removed_registry': True,
            }, indent=2))
        else:
            print(color(f'Removed bridge for {project_dir.name}', '33'))
            print(f'  AGENTS.md deleted: {"yes" if removed_file else "no (missing)"}')
        return EXIT_OK

    return fail('unknown bridge command')


def cmd_prompt(args, conn):
    paths = get_prompt_paths(getattr(args, "workspace", None))

    if args.prompt_command == "show":
        active = paths["active"]
        if not active.exists():
            return fail(f"active prompt not found at {active}")
        content = active.read_text()
        if args.json:
            manifest = load_manifest(paths)
            current_v = manifest[-1]["version"] if manifest else "unknown"
            print(json.dumps({"version": current_v, "path": str(active), "content": content}, indent=2))
        else:
            print(content)
        return EXIT_OK

    if args.prompt_command == "snapshot":
        active = paths["active"]
        if not active.exists():
            return fail(f"active prompt not found at {active}")
        paths["versions_dir"].mkdir(parents=True, exist_ok=True)
        manifest = load_manifest(paths)
        version = next_version(manifest)
        content = active.read_text()
        version_file = paths["versions_dir"] / f"v{version}.md"
        version_file.write_text(content)
        entry = {
            "version": version,
            "date": dt.datetime.now().isoformat(timespec="seconds"),
            "message": args.message or f"snapshot v{version}",
            "lines": content.count("\n"),
            "size": len(content),
        }
        manifest.append(entry)
        save_manifest(paths, manifest)
        if args.json:
            print(json.dumps(entry, indent=2))
        else:
            print(color(f"Snapshot v{version}: {version_file.name} ({entry['lines']} lines)", "32"))
        return EXIT_OK

    if args.prompt_command == "propose":
        paths["proposals_dir"].mkdir(parents=True, exist_ok=True)
        proposal_id = next_proposal_id(paths)
        slug = slugify(args.description)
        filename = f"{proposal_id:03d}-{slug}.md"
        proposal_path = paths["proposals_dir"] / filename
        if args.file:
            source = Path(args.file)
            if not source.exists():
                return fail(f"source file not found: {args.file}")
            content = source.read_text()
        else:
            now_str = dt.datetime.now().isoformat(timespec="seconds")
            content = (
                f"# Proposal {proposal_id}: {args.description}\n\n"
                f"**Date**: {now_str}\n"
                f"**Status**: pending\n\n"
                f"## Observation\n\n"
                f"<!-- What pattern/issue triggered this proposal? -->\n\n"
                f"## Proposed Changes\n\n"
                f"<!-- Describe the changes to AGENTS.md -->\n\n"
                f"## New AGENTS.md Content\n\n"
                f"<!-- Paste the full new AGENTS.md content below this line -->\n"
            )
        proposal_path.write_text(content)
        conn.execute(
            "INSERT INTO kv(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
            (f"proposal.{proposal_id}", json.dumps({
                "id": proposal_id,
                "description": args.description,
                "file": str(proposal_path),
                "status": "pending",
                "created": dt.datetime.now().isoformat(timespec="seconds"),
            })),
        )
        conn.commit()
        if args.json:
            print(json.dumps({"id": proposal_id, "file": str(proposal_path)}, indent=2))
        else:
            print(color(f"Created proposal [{proposal_id}]: {proposal_path.name}", "32"))
            if not args.file:
                print(f"  Edit the proposal file, then run: nous-memory prompt apply {proposal_id}")
        return EXIT_OK

    if args.prompt_command == "proposals":
        rows = conn.execute(
            "SELECT key, value FROM kv WHERE key LIKE 'proposal.%' ORDER BY key"
        ).fetchall()
        proposals = []
        for row in rows:
            data = json.loads(row["value"])
            if args.all or data.get("status") == "pending":
                proposals.append(data)
        if args.json:
            print(json.dumps(proposals, indent=2))
            return EXIT_OK
        if not proposals:
            print("No pending proposals." if not args.all else "No proposals.")
            return EXIT_OK
        for p in proposals:
            sc = "33" if p["status"] == "pending" else "32" if p["status"] == "applied" else "31"
            print(f"[{p['id']}] {color(p['status'], sc)}  {p['description']}")
            print(f"  file: {p['file']}")
            print(f"  created: {humanize_datetime(p['created'])}")
            print()
        return EXIT_OK

    if args.prompt_command == "apply":
        kv_row = conn.execute(
            "SELECT value FROM kv WHERE key = ?", (f"proposal.{args.proposal_id}",)
        ).fetchone()
        if kv_row is None:
            return fail(f"proposal {args.proposal_id} not found")
        proposal_data = json.loads(kv_row["value"])
        if proposal_data["status"] != "pending":
            return fail(f"proposal {args.proposal_id} is already {proposal_data['status']}")
        proposal_path = Path(proposal_data["file"])
        if not proposal_path.exists():
            return fail(f"proposal file missing: {proposal_path}")
        proposal_content = proposal_path.read_text()
        marker = "## New AGENTS.md Content"
        if marker in proposal_content:
            idx = proposal_content.index(marker) + len(marker)
            new_content = proposal_content[idx:].lstrip("\n")
        else:
            new_content = proposal_content
        if not new_content.strip():
            return fail("proposal has no content to apply (empty after marker)")
        active = paths["active"]
        # Auto-snapshot current version before overwriting
        paths["versions_dir"].mkdir(parents=True, exist_ok=True)
        manifest = load_manifest(paths)
        old_version = next_version(manifest)
        old_content = active.read_text() if active.exists() else ""
        version_file = paths["versions_dir"] / f"v{old_version}.md"
        version_file.write_text(old_content)
        manifest.append({
            "version": old_version,
            "date": dt.datetime.now().isoformat(timespec="seconds"),
            "message": f"pre-apply snapshot (before proposal {args.proposal_id})",
            "lines": old_content.count("\n"),
            "size": len(old_content),
        })
        save_manifest(paths, manifest)
        # Write new content
        active.write_text(new_content)
        # Update proposal status
        proposal_data["status"] = "applied"
        proposal_data["applied_at"] = dt.datetime.now().isoformat(timespec="seconds")
        proposal_data["snapshot_version"] = old_version
        conn.execute(
            "UPDATE kv SET value = ?, updated_at = CURRENT_TIMESTAMP WHERE key = ?",
            (json.dumps(proposal_data), f"proposal.{args.proposal_id}"),
        )
        conn.commit()
        if args.json:
            print(json.dumps({"applied": args.proposal_id, "snapshot": old_version}, indent=2))
        else:
            print(color(f"Applied proposal [{args.proposal_id}]", "32"))
            print(f"  Old version saved as v{old_version}")
            print(f"  AGENTS.md updated ({new_content.count(chr(10))} lines)")
        return EXIT_OK

    if args.prompt_command == "reject":
        kv_row = conn.execute(
            "SELECT value FROM kv WHERE key = ?", (f"proposal.{args.proposal_id}",)
        ).fetchone()
        if kv_row is None:
            return fail(f"proposal {args.proposal_id} not found")
        proposal_data = json.loads(kv_row["value"])
        proposal_data["status"] = "rejected"
        proposal_data["rejected_at"] = dt.datetime.now().isoformat(timespec="seconds")
        if args.reason:
            proposal_data["reject_reason"] = args.reason
        conn.execute(
            "UPDATE kv SET value = ?, updated_at = CURRENT_TIMESTAMP WHERE key = ?",
            (json.dumps(proposal_data), f"proposal.{args.proposal_id}"),
        )
        conn.commit()
        if args.json:
            print(json.dumps(proposal_data, indent=2))
        else:
            print(color(f"Rejected proposal [{args.proposal_id}]", "33"))
        return EXIT_OK

    if args.prompt_command == "history":
        manifest = load_manifest(paths)
        if args.json:
            print(json.dumps(manifest, indent=2))
            return EXIT_OK
        if not manifest:
            print("No version history.")
            return EXIT_OK
        for entry in manifest:
            print(f"v{entry['version']}  {humanize_datetime(entry['date'])}  {entry.get('lines', '?')} lines")
            print(f"  {entry.get('message', '-')}")
            print()
        return EXIT_OK

    if args.prompt_command == "diff":
        if args.proposal:
            kv_row = conn.execute(
                "SELECT value FROM kv WHERE key = ?", (f"proposal.{args.proposal}",)
            ).fetchone()
            if kv_row is None:
                return fail(f"proposal {args.proposal} not found")
            proposal_data = json.loads(kv_row["value"])
            ppath = Path(proposal_data["file"])
            if not ppath.exists():
                return fail(f"proposal file missing: {ppath}")
            pcontent = ppath.read_text()
            marker = "## New AGENTS.md Content"
            if marker in pcontent:
                idx = pcontent.index(marker) + len(marker)
                new_content = pcontent[idx:].lstrip("\n")
            else:
                new_content = pcontent
            with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as tmp:
                tmp.write(new_content)
                tmp_path = tmp.name
            try:
                result = subprocess.run(
                    ["diff", "-u", "--label", "AGENTS.md (active)", str(paths["active"]),
                     "--label", f"proposal-{args.proposal}", tmp_path],
                    capture_output=True, text=True,
                )
                print(result.stdout if result.stdout else "No differences.")
            finally:
                Path(tmp_path).unlink(missing_ok=True)
            return EXIT_OK
        v_from = args.v_from or "active"
        v_to = args.v_to or "active"
        if v_from == "active":
            path_from, label_from = paths["active"], "AGENTS.md (active)"
        else:
            path_from = paths["versions_dir"] / f"v{v_from}.md"
            label_from = f"v{v_from}"
        if v_to == "active":
            path_to, label_to = paths["active"], "AGENTS.md (active)"
        else:
            path_to = paths["versions_dir"] / f"v{v_to}.md"
            label_to = f"v{v_to}"
        if not path_from.exists():
            return fail(f"{label_from} not found")
        if not path_to.exists():
            return fail(f"{label_to} not found")
        result = subprocess.run(
            ["diff", "-u", "--label", label_from, str(path_from),
             "--label", label_to, str(path_to)],
            capture_output=True, text=True,
        )
        print(result.stdout if result.stdout else "No differences.")
        return EXIT_OK

    return fail("unknown prompt command")

# --- Token-Budgeted Bootstrap ---


def _build_section(header, lines, remaining_budget):
    """Build a section string within remaining character budget.
    Returns (section_text, chars_used). If section won't fit at all, returns ('', 0)."""
    if remaining_budget <= 0:
        return "", 0
    header_line = f"## {header}\n"
    if remaining_budget < len(header_line) + 20:
        return "", 0
    budget = remaining_budget - len(header_line)
    included = []
    used = 0
    for line in lines:
        line_cost = len(line) + 1  # +1 for newline
        if used + line_cost > budget:
            remaining_count = len(lines) - len(included)
            if remaining_count > 0:
                ellipsis = f"  ... and {remaining_count} more"
                if used + len(ellipsis) + 1 <= budget:
                    included.append(ellipsis)
                    used += len(ellipsis) + 1
            break
        included.append(line)
        used += line_cost
    if not included:
        return "", 0
    text = header_line + "\n".join(included) + "\n"
    return text, len(text)


def cmd_bootstrap(args, conn):
    """Token-budgeted session bootstrap with identity-first default."""
    budget = args.budget
    scope = args.scope
    now = dt.datetime.now()
    tier = getattr(args, 'tier', 'l1')
    critical_only = getattr(args, 'critical_only', not getattr(args, 'full_constraints', False))

    def _format_bootstrap_content(content, max_len):
        normalized = (content or '').replace("\n", " ").strip()
        if tier == 'l2':
            return normalized
        if len(normalized) > max_len:
            return normalized[: max_len - 3] + '...'
        return normalized

    # --- Identity 1: hard constraints ---
    # When critical_only=True (default), load only constraints tagged 'critical'
    # for minimal post-compaction context. Use critical_only=False for full set.
    if critical_only:
        constraint_tag_filter = "AND (',' || tags || ',' LIKE '%,critical,%')"
    else:
        constraint_tag_filter = "AND (',' || tags || ',' LIKE '%,hard-constraint,%')"
    constraint_rows = conn.execute(
        f"""
        SELECT id, content, headline FROM memories
        WHERE type = 'preference'
          AND deleted_at IS NULL
          AND superseded_by IS NULL
          AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
          {constraint_tag_filter}
          AND NOT (metadata IS NOT NULL AND json_extract(metadata, '$.auto_extracted') = 1 AND verified_at IS NULL)
        ORDER BY id ASC
        """,
    ).fetchall()
    constraint_lines = []
    for row in constraint_rows:
        if tier == 'l0':
            constraint_text = row['headline'] or extract_headline(row['content'])
        else:
            constraint_text = _format_bootstrap_content(row['content'], 120)
        constraint_lines.append(f"- [{row['id']}] {constraint_text}")

    # --- Identity 2: non-hard-constraint preferences ---
    preference_values = ()
    preference_scope_clause = ""
    if scope:
        preference_scope_clause = "AND scope = ?"
        preference_values = (scope,)
    preference_rows = conn.execute(
        f"""
        SELECT id, content, headline FROM memories
        WHERE type = 'preference'
          AND deleted_at IS NULL
          AND superseded_by IS NULL
          AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
          AND (tags IS NULL OR NOT (',' || tags || ',' LIKE '%,hard-constraint,%'))
          AND NOT (metadata IS NOT NULL AND json_extract(metadata, '$.auto_extracted') = 1 AND verified_at IS NULL)
          {preference_scope_clause}
        ORDER BY created_at DESC, id DESC
        LIMIT 20
        """,
        preference_values,
    ).fetchall()
    preference_lines = []
    for row in preference_rows:
        if tier == 'l0':
            preference_text = row['headline'] or extract_headline(row['content'])
        else:
            preference_text = _format_bootstrap_content(row['content'], 120)
        preference_lines.append(f"- [{row['id']}] {preference_text}")

    # --- Identity 3: active episode pointers ---
    # Auto-close episodes older than 24h
    stale_cutoff = (now - dt.timedelta(hours=24)).isoformat(timespec='seconds')
    conn.execute(
        """
        UPDATE episodes
        SET status = 'done',
            ended_at = CURRENT_TIMESTAMP,
            summary = COALESCE(summary, '') || ' [auto-closed: stale >24h]'
        WHERE status = 'active'
          AND started_at < ?
        """,
        (stale_cutoff,),
    )
    conn.commit()

    episode_values = ()
    episode_scope_clause = ""
    if scope:
        episode_scope_clause = "AND scope = ?"
        episode_values = (scope,)
    active_episode_rows = conn.execute(
        f"""
        SELECT id, scope, intent
        FROM episodes
        WHERE status = 'active'
          {episode_scope_clause}
        ORDER BY started_at DESC
        """,
        episode_values,
    ).fetchall()
    episode_lines = []
    for row in active_episode_rows:
        intent = (row["intent"] or "-").replace("\n", " ").strip()
        if len(intent) > 120:
            intent = intent[:117] + "..."
        episode_lines.append(f"- Active: {row['id']} ({row['scope']}) - {intent}")

    alert_lines = []
    task_lines = []
    recent_lines = []
    pattern_lines = []

    if args.handoff:
        # --- Handoff: pending + due-soon tasks ---
        alert_limit = (now + dt.timedelta(hours=24)).isoformat(timespec="seconds")
        alert_rows = conn.execute(
            """
            SELECT t.id, t.title, t.priority, t.due_date, t.status
            FROM tasks t
            WHERE t.status IN ('pending', 'active')
              AND t.due_date IS NOT NULL
              AND t.due_date <= ?
            ORDER BY t.due_date ASC
            """,
            (alert_limit,),
        ).fetchall()
        for row in alert_rows:
            due_parsed = parse_dt(row["due_date"])
            if due_parsed and due_parsed < now:
                due_label = humanize_datetime(row["due_date"], now) + " OVERDUE"
            else:
                due_label = humanize_datetime(row["due_date"], now)
            alert_lines.append(f"- [{row['id']}] {row['title']} ({row['priority']}, {due_label})")

        task_rows = conn.execute(
            """
            SELECT t.id, t.title, t.priority, t.due_date
            FROM tasks t
            WHERE t.status = 'pending'
            ORDER BY (t.due_date IS NULL), t.due_date ASC, t.created_at DESC
            """,
        ).fetchall()
        for row in task_rows:
            due = humanize_datetime(row["due_date"], now) if row["due_date"] else "-"
            task_lines.append(f"- [{row['id']}] {row['title']} ({row['priority']}, due={due})")

        # --- Handoff: recent context for active episode scope(s) ---
        active_scope_names = []
        if scope:
            active_scope_names = [scope]
        else:
            active_scope_names = [r["scope"] for r in active_episode_rows]
        recent_limit = min(5, max(3, args.recent_limit))
        for scope_name in active_scope_names:
            recent_rows = conn.execute(
                """
                SELECT id, type, content, headline, created_at
                FROM memories
                WHERE deleted_at IS NULL
                  AND superseded_by IS NULL
                  AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
                  AND scope = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (scope_name, recent_limit),
            ).fetchall()
            for row in recent_rows:
                if tier == 'l0':
                    content = row['headline'] or extract_headline(row['content'])
                else:
                    content = _format_bootstrap_content(row['content'], 100)
                age = humanize_datetime(row["created_at"], now)
                recent_lines.append(f"- [{row['id']}] {scope_name} {row['type']}: {content} ({age})")

        # --- Handoff: recent failures + patterns ---
        pattern_clauses = [
            "deleted_at IS NULL",
            "superseded_by IS NULL",
            "(expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)",
            "type IN ('failure', 'pattern')",
        ]
        pattern_values = []
        if scope:
            pattern_clauses.append("scope = ?")
            pattern_values.append(scope)
        pattern_values.append(5)
        pattern_rows = conn.execute(
            f"""
            SELECT id, type, content, created_at FROM memories
            WHERE {" AND ".join(pattern_clauses)}
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            tuple(pattern_values),
        ).fetchall()
        for row in pattern_rows:
            content = _format_bootstrap_content(row['content'], 100)
            marker = "x" if row["type"] == "failure" else "+"
            pattern_lines.append(f"- {marker} [{row['id']}] {content}")

    # --- Unverified auto-extracted count (always shown as hint, no content) ---
    unverified_scope_clause = 'AND scope = ?' if scope else ''
    unverified_scope_vals = (scope,) if scope else ()
    unverified_count = conn.execute(
        f"""
        SELECT COUNT(*) AS c FROM memories
        WHERE deleted_at IS NULL
          AND superseded_by IS NULL
          AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
          AND json_extract(metadata, '$.auto_extracted') = 1
          AND verified_at IS NULL
          {unverified_scope_clause}
        """,
        unverified_scope_vals,
    ).fetchone()['c']

    # --- News: recent news observations (last 48h, tagged 'news') ---
    # Load critical news first, then recent, capped at 3 total
    news_cutoff = (now - dt.timedelta(hours=48)).isoformat(timespec='seconds')
    news_rows = conn.execute(
        """
        SELECT id, content, tags, created_at FROM memories
        WHERE type = 'observation'
          AND deleted_at IS NULL
          AND superseded_by IS NULL
          AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
          AND (',' || tags || ',' LIKE '%,news,%')
          AND created_at > ?
        ORDER BY
          CASE WHEN (',' || tags || ',' LIKE '%,critical,%') THEN 0 ELSE 1 END,
          created_at DESC
        LIMIT 3
        """,
        (news_cutoff,),
    ).fetchall()
    news_lines = []
    for row in news_rows:
        content = _format_bootstrap_content(row['content'], 150)
        is_critical = ',critical,' in (',' + (row['tags'] or '') + ',')
        prefix = '🔴' if is_critical else '📰'
        news_lines.append(f"- {prefix} [{row['id']}] {content}")

    sections = [
        ("Constraints", constraint_lines),
        ("Preferences", preference_lines),
        ("Episodes", episode_lines),
    ]
    if news_lines:
        sections.append(("News (last 48h)", news_lines))
    if args.handoff:
        sections.extend([
            ("Alerts", alert_lines),
            ("Tasks", task_lines),
            ("Recent", recent_lines),
            ("Patterns", pattern_lines),
        ])

    output_parts = []
    remaining = budget
    for header, lines in sections:
        if not lines:
            continue
        text, used = _build_section(header, lines, remaining)
        if text:
            output_parts.append(text)
            remaining -= used

    if not output_parts:
        result = "No bootstrap context available."
    else:
        result = "\n".join(output_parts).rstrip()
        if unverified_count > 0:
            hint = f"\n\u26a0 {unverified_count} unverified auto-extracted memories pending review (run `curate` to see details)"
            result += hint

    if args.json:
        payload = {
            "budget": budget,
            "used": budget - remaining,
            "scope": scope,
            "handoff": bool(args.handoff),
            "sections": {
                "constraints": len(constraint_lines),
                "preferences": len(preference_lines),
                "episodes": len(episode_lines),
                "alerts": len(alert_lines),
                "tasks": len(task_lines),
                "recent": len(recent_lines),
                "patterns": len(pattern_lines),
                "news": len(news_lines),
            },
            "unverified_auto_extracted": unverified_count,
            "output": result,
        }
        print(json.dumps(payload, indent=2))
        return EXIT_OK

    print(result)
    return EXIT_OK



DREAM_STALE_DAYS = 30
DREAM_EXPIRY_LOOKAHEAD_DAYS = 7


def cmd_dream(args, conn):
    """Background dreaming: idle-time memory re-evaluation.

    Scans the memory store and produces a report with:
    1. Stale decisions - decisions older than N days that may need review
    2. Expiring memories - memories with expires_at within lookahead window
    3. Contradictions - multiple active decisions in same scope on similar topics
    4. Recurring patterns - tag clusters indicating systematic issues
    5. Memory health - overall stats and recommendations
    """
    now = dt.datetime.now()
    scope = getattr(args, 'scope', None)
    stale_days = getattr(args, 'stale_days', DREAM_STALE_DAYS)
    report = {}

    # 1. Stale decisions (old, not superseded, may need review)
    scope_clause = 'AND scope = ?' if scope else ''
    scope_vals = (scope,) if scope else ()
    cutoff = (now - dt.timedelta(days=stale_days)).isoformat(timespec='seconds')
    stale_rows = conn.execute(
        f"""
        SELECT id, type, scope, content, created_at, updated_at
        FROM memories
        WHERE type = 'decision'
          AND deleted_at IS NULL
          AND superseded_by IS NULL
          AND updated_at < ?
          {scope_clause}
        ORDER BY updated_at ASC
        LIMIT 20
        """,
        (cutoff, *scope_vals),
    ).fetchall()
    report['stale_decisions'] = []
    for r in stale_rows:
        updated_at = parse_dt(r['updated_at'])
        if updated_at is None:
            continue
        report['stale_decisions'].append({
            'id': r['id'],
            'scope': r['scope'],
            'age_days': (now - updated_at).days,
            'snippet': r['content'][:100],
        })

    # 2. Expiring memories (approaching or past expires_at)
    lookahead = (now + dt.timedelta(days=DREAM_EXPIRY_LOOKAHEAD_DAYS)).isoformat(timespec='seconds')
    expiring_rows = conn.execute(
        f"""
        SELECT id, type, scope, content, expires_at
        FROM memories
        WHERE deleted_at IS NULL
          AND superseded_by IS NULL
          AND expires_at IS NOT NULL
          AND expires_at <= ?
          {scope_clause}
        ORDER BY expires_at ASC
        LIMIT 20
        """,
        (lookahead, *scope_vals),
    ).fetchall()
    report['expiring'] = [
        {'id': r['id'], 'type': r['type'], 'scope': r['scope'],
         'expires': r['expires_at'], 'snippet': r['content'][:100]} for r in expiring_rows
    ]

    # 3. Contradiction candidates (multiple active decisions same scope, high word overlap)
    contradiction_candidates = []
    decision_rows = conn.execute(
        f"""
        SELECT id, scope, content, created_at
        FROM memories
        WHERE type = 'decision'
          AND deleted_at IS NULL
          AND superseded_by IS NULL
          {scope_clause}
        ORDER BY scope, created_at DESC
        """,
        scope_vals,
    ).fetchall()
    by_scope = {}
    for row in decision_rows:
        s = row['scope'] or 'global'
        by_scope.setdefault(s, []).append(row)
    for s, decisions in by_scope.items():
        if len(decisions) < 2:
            continue
        for i, newer in enumerate(decisions[:10]):
            for older in decisions[i+1:min(i+11, len(decisions))]:
                ratio = word_overlap_ratio(newer['content'], older['content'])
                if ratio >= 0.40:
                    contradiction_candidates.append({
                        'newer_id': newer['id'], 'older_id': older['id'],
                        'scope': s, 'overlap': round(ratio, 2),
                        'newer_snippet': newer['content'][:80],
                        'older_snippet': older['content'][:80],
                    })
    report['contradiction_candidates'] = contradiction_candidates[:10]

    # 4. Recurring patterns
    recurring = find_recurring_patterns(conn, threshold=3)
    report['recurring_patterns'] = {
        tag: len(memories) for tag, memories in recurring.items()
    } if recurring else {}

    # 5. Memory health
    total = conn.execute('SELECT COUNT(*) AS c FROM memories WHERE deleted_at IS NULL').fetchone()['c']
    oldest = conn.execute(
        'SELECT MIN(created_at) AS oldest FROM memories WHERE deleted_at IS NULL'
    ).fetchone()['oldest']
    newest = conn.execute(
        'SELECT MAX(created_at) AS newest FROM memories WHERE deleted_at IS NULL'
    ).fetchone()['newest']
    dup_total = conn.execute(
        'SELECT COALESCE(SUM(duplicate_count), 0) AS c FROM memories'
    ).fetchone()['c']
    report['health'] = {
        'total_active': total,
        'oldest_memory': oldest,
        'newest_memory': newest,
        'duplicates_prevented': dup_total,
        'stale_count': len(report['stale_decisions']),
        'expiring_count': len(report['expiring']),
        'contradiction_count': len(report['contradiction_candidates']),
        'pattern_clusters': len(report['recurring_patterns']),
    }

    if args.json:
        print(json.dumps(report, indent=2))
        return EXIT_OK

    print(color('=== Dream Report ===', '36'))
    print()
    h = report['health']
    print(color('Memory Health:', '1'))
    print(f'  Active memories: {h["total_active"]}  |  Duplicates prevented: {h["duplicates_prevented"]}')
    print(f'  Oldest: {humanize_datetime(h["oldest_memory"])}  |  Newest: {humanize_datetime(h["newest_memory"])}')
    print()

    if report['stale_decisions']:
        print(color(f'Stale Decisions ({len(report["stale_decisions"])} older than {stale_days}d):', '33'))
        for item in report['stale_decisions']:
            print(f'  [{item["id"]}] {item["age_days"]}d old  scope={item["scope"]}')
            print(f'    {item["snippet"]}')
        print()

    if report['expiring']:
        print(color(f'Expiring/Expired ({len(report["expiring"])}):', '31'))
        for item in report['expiring']:
            print(f'  [{item["id"]}] {item["type"]}  expires={humanize_datetime(item["expires"])}')
            print(f'    {item["snippet"]}')
        print()

    if report['contradiction_candidates']:
        print(color(f'Contradiction Candidates ({len(report["contradiction_candidates"])}):', '33'))
        for item in report['contradiction_candidates']:
            print(f'  [{item["newer_id"]}] vs [{item["older_id"]}]  scope={item["scope"]}  overlap={item["overlap"]}')
            print(f'    newer: {item["newer_snippet"]}')
            print(f'    older: {item["older_snippet"]}')
        print()

    if report['recurring_patterns']:
        print(color(f'Recurring Pattern Clusters ({len(report["recurring_patterns"])}):', '36'))
        for tag, count in sorted(report['recurring_patterns'].items(), key=lambda x: -x[1]):
            print(f'  {tag}: {count} occurrences')
        print()

    if not any([report['stale_decisions'], report['expiring'],
               report['contradiction_candidates'], report['recurring_patterns']]):
        print(color('All clear - no issues found.', '32'))

    return EXIT_OK


def _detect_relationships(conn, scope=None):
    scope_clause = 'AND scope = ?' if scope else ''
    scope_vals = (scope,) if scope else ()

    rows = conn.execute(
        f"""
        SELECT id, type, scope, content, tags, created_at, headline
        FROM memories
        WHERE deleted_at IS NULL
          AND superseded_by IS NULL
          {scope_clause}
        ORDER BY scope, created_at DESC
        """,
        scope_vals,
    ).fetchall()

    by_scope = {}
    for row in rows:
        scope_name = row['scope'] or 'global'
        by_scope.setdefault(scope_name, []).append(row)

    links = []
    for scope_name, memories in by_scope.items():
        if len(memories) < 2:
            continue
        for i, newer in enumerate(memories[:50]):
            for older in memories[i + 1:min(i + 20, len(memories))]:
                ratio = word_overlap_ratio(newer['content'], older['content'])

                if ratio >= 0.80:
                    links.append({
                        'src_id': newer['id'],
                        'dst_id': older['id'],
                        'relation': 'DUPLICATE_OF',
                        'note': f'word overlap {ratio:.0%}',
                        'scope': scope_name,
                        'overlap': round(ratio, 2),
                    })
                elif ratio >= 0.50:
                    if newer['type'] == older['type']:
                        links.append({
                            'src_id': newer['id'],
                            'dst_id': older['id'],
                            'relation': 'SUPERSEDES',
                            'note': f'same type+scope, overlap {ratio:.0%}',
                            'scope': scope_name,
                            'overlap': round(ratio, 2),
                        })
                    else:
                        links.append({
                            'src_id': newer['id'],
                            'dst_id': older['id'],
                            'relation': 'LINKED',
                            'note': f'cross-type overlap {ratio:.0%}',
                            'scope': scope_name,
                            'overlap': round(ratio, 2),
                        })
                elif ratio >= 0.30:
                    resolution_pairs = {
                        ('decision', 'failure'),
                        ('pattern', 'failure'),
                    }
                    if (newer['type'], older['type']) in resolution_pairs:
                        links.append({
                            'src_id': newer['id'],
                            'dst_id': older['id'],
                            'relation': 'RESOLVES',
                            'note': f'{newer["type"]} resolves {older["type"]}, overlap {ratio:.0%}',
                            'scope': scope_name,
                            'overlap': round(ratio, 2),
                        })

    return links


def cmd_curate(args, conn):
    now = dt.datetime.now()
    scope = getattr(args, 'scope', None)
    stale_days = getattr(args, 'stale_days', DREAM_STALE_DAYS)
    json_output = getattr(args, 'json_output', False) or getattr(args, 'json', False)
    apply_changes = getattr(args, 'apply', False)
    report = {}

    scope_clause = 'AND scope = ?' if scope else ''
    scope_vals = (scope,) if scope else ()
    cutoff = (now - dt.timedelta(days=stale_days)).isoformat(timespec='seconds')
    stale_rows = conn.execute(
        f"""
        SELECT id, type, scope, content, created_at, updated_at
        FROM memories
        WHERE type = 'decision'
          AND deleted_at IS NULL
          AND superseded_by IS NULL
          AND updated_at < ?
          {scope_clause}
        ORDER BY updated_at ASC
        LIMIT 20
        """,
        (cutoff, *scope_vals),
    ).fetchall()
    report['stale_decisions'] = []
    for r in stale_rows:
        updated_at = parse_dt(r['updated_at'])
        if updated_at is None:
            continue
        report['stale_decisions'].append({
            'id': r['id'],
            'scope': r['scope'],
            'age_days': (now - updated_at).days,
            'snippet': r['content'][:100],
        })

    lookahead = (now + dt.timedelta(days=DREAM_EXPIRY_LOOKAHEAD_DAYS)).isoformat(timespec='seconds')
    expiring_rows = conn.execute(
        f"""
        SELECT id, type, scope, content, expires_at
        FROM memories
        WHERE deleted_at IS NULL
          AND superseded_by IS NULL
          AND expires_at IS NOT NULL
          AND expires_at <= ?
          {scope_clause}
        ORDER BY expires_at ASC
        LIMIT 20
        """,
        (lookahead, *scope_vals),
    ).fetchall()
    report['expiring'] = [
        {'id': r['id'], 'type': r['type'], 'scope': r['scope'],
         'expires': r['expires_at'], 'snippet': r['content'][:100]} for r in expiring_rows
    ]

    contradiction_candidates = []
    decision_rows = conn.execute(
        f"""
        SELECT id, scope, content, created_at
        FROM memories
        WHERE type = 'decision'
          AND deleted_at IS NULL
          AND superseded_by IS NULL
          {scope_clause}
        ORDER BY scope, created_at DESC
        """,
        scope_vals,
    ).fetchall()
    by_scope = {}
    for row in decision_rows:
        scope_name = row['scope'] or 'global'
        by_scope.setdefault(scope_name, []).append(row)
    for scope_name, decisions in by_scope.items():
        if len(decisions) < 2:
            continue
        for i, newer in enumerate(decisions[:10]):
            for older in decisions[i + 1:min(i + 11, len(decisions))]:
                ratio = word_overlap_ratio(newer['content'], older['content'])
                if ratio >= 0.40:
                    contradiction_candidates.append({
                        'newer_id': newer['id'],
                        'older_id': older['id'],
                        'scope': scope_name,
                        'overlap': round(ratio, 2),
                        'newer_snippet': newer['content'][:80],
                        'older_snippet': older['content'][:80],
                    })
    report['contradiction_candidates'] = contradiction_candidates[:10]

    recurring = find_recurring_patterns(conn, threshold=3)
    report['recurring_patterns'] = {
        tag: len(memories) for tag, memories in recurring.items()
    } if recurring else {}

    total = conn.execute('SELECT COUNT(*) AS c FROM memories WHERE deleted_at IS NULL').fetchone()['c']
    oldest = conn.execute(
        'SELECT MIN(created_at) AS oldest FROM memories WHERE deleted_at IS NULL'
    ).fetchone()['oldest']
    newest = conn.execute(
        'SELECT MAX(created_at) AS newest FROM memories WHERE deleted_at IS NULL'
    ).fetchone()['newest']
    dup_total = conn.execute(
        'SELECT COALESCE(SUM(duplicate_count), 0) AS c FROM memories'
    ).fetchone()['c']
    report['health'] = {
        'total_active': total,
        'oldest_memory': oldest,
        'newest_memory': newest,
        'duplicates_prevented': dup_total,
        'stale_count': len(report['stale_decisions']),
        'expiring_count': len(report['expiring']),
        'contradiction_count': len(report['contradiction_candidates']),
        'pattern_clusters': len(report['recurring_patterns']),
    }

    # --- Unverified auto-extracted memories ---
    unverified_rows = conn.execute(
        f"""
        SELECT id, type, scope, content, metadata, created_at, expires_at
        FROM memories
        WHERE deleted_at IS NULL
          AND superseded_by IS NULL
          AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
          AND json_extract(metadata, '$.auto_extracted') = 1
          AND verified_at IS NULL
          {scope_clause}
        ORDER BY created_at DESC
        LIMIT 30
        """,
        scope_vals,
    ).fetchall()
    report['unverified_auto_extracted'] = []
    for r in unverified_rows:
        meta = {}
        if r['metadata']:
            try:
                meta = json.loads(r['metadata']) if isinstance(r['metadata'], str) else (r['metadata'] or {})
            except (json.JSONDecodeError, TypeError):
                meta = {}
        report['unverified_auto_extracted'].append({
            'id': r['id'],
            'type': r['type'],
            'scope': r['scope'],
            'confidence': meta.get('confidence', '?'),
            'snippet': r['content'][:100],
            'created': r['created_at'],
            'expires': r['expires_at'],
        })

    detected_relationships = _detect_relationships(conn, scope=scope)
    links_created = 0
    superseded_count = 0
    duplicates_deleted = 0
    relationship_rows = []

    for link in detected_relationships:
        exists = conn.execute(
            """
            SELECT 1 FROM memory_links
            WHERE src_memory_id = ? AND dst_memory_id = ? AND relation = ?
            """,
            (link['src_id'], link['dst_id'], link['relation']),
        ).fetchone()
        is_new = exists is None

        if apply_changes and is_new:
            conn.execute(
                """
                INSERT INTO memory_links(src_memory_id, relation, dst_memory_id, note)
                VALUES (?, ?, ?, ?)
                """,
                (link['src_id'], link['relation'], link['dst_id'], link['note']),
            )
            links_created += 1

        if apply_changes and link['relation'] in ('SUPERSEDES', 'DUPLICATE_OF'):
            res = conn.execute(
                """
                UPDATE memories
                SET superseded_by = ?
                WHERE id = ? AND superseded_by IS NULL
                """,
                (link['src_id'], link['dst_id']),
            )
            superseded_count += res.rowcount

        if apply_changes and link['relation'] == 'DUPLICATE_OF':
            res = conn.execute(
                """
                UPDATE memories
                SET deleted_at = CURRENT_TIMESTAMP
                WHERE id = ? AND deleted_at IS NULL
                """,
                (link['dst_id'],),
            )
            duplicates_deleted += res.rowcount

        relationship_rows.append({
            'src_id': link['src_id'],
            'dst_id': link['dst_id'],
            'relation': link['relation'],
            'note': link['note'],
            'scope': link['scope'],
            'is_new': is_new,
        })

    if apply_changes:
        conn.commit()

    new_relationship_rows = [row for row in relationship_rows if row['is_new']]
    report['relationships'] = {
        'detected_total': len(relationship_rows),
        'detected_new': len(new_relationship_rows),
        'links_created': links_created,
        'superseded_count': superseded_count,
        'duplicates_deleted': duplicates_deleted,
        'apply': apply_changes,
        'items': relationship_rows,
    }

    if json_output:
        print(json.dumps(report, indent=2))
        return EXIT_OK

    print(color('=== Curator Report ===', '36'))
    print()

    h = report['health']
    print(color('Memory Health:', '1'))
    print(f'  Active memories: {h["total_active"]}  |  Duplicates prevented: {h["duplicates_prevented"]}')
    print(f'  Oldest: {humanize_datetime(h["oldest_memory"])}  |  Newest: {humanize_datetime(h["newest_memory"])}')
    print()

    if report['unverified_auto_extracted']:
        print(color(f'Unverified Auto-Extracted ({len(report["unverified_auto_extracted"])} pending review):', '33'))
        print(f'  Use `verify <id>` to approve or `forget <id>` to discard.')
        for item in report['unverified_auto_extracted']:
            conf = item['confidence']
            conf_str = f'{conf:.0%}' if isinstance(conf, (int, float)) else str(conf)
            expires_str = f'  expires={humanize_datetime(item["expires"])}' if item['expires'] else ''
            print(f'  [{item["id"]}] {item["type"]}  scope={item["scope"]}  confidence={conf_str}{expires_str}')
            print(f'    {item["snippet"]}')
        print()

    if report['stale_decisions']:
        print(color(f'Stale Decisions ({len(report["stale_decisions"])} older than {stale_days}d):', '33'))
        for item in report['stale_decisions']:
            print(f'  [{item["id"]}] {item["age_days"]}d old  scope={item["scope"]}')
            print(f'    {item["snippet"]}')
        print()

    if report['expiring']:
        print(color(f'Expiring/Expired ({len(report["expiring"])}):', '31'))
        for item in report['expiring']:
            print(f'  [{item["id"]}] {item["type"]}  expires={humanize_datetime(item["expires"])}')
            print(f'    {item["snippet"]}')
        print()

    if report['contradiction_candidates']:
        print(color(f'Contradiction Candidates ({len(report["contradiction_candidates"])}):', '33'))
        for item in report['contradiction_candidates']:
            print(f'  [{item["newer_id"]}] vs [{item["older_id"]}]  scope={item["scope"]}  overlap={item["overlap"]}')
            print(f'    newer: {item["newer_snippet"]}')
            print(f'    older: {item["older_snippet"]}')
        print()

    if report['recurring_patterns']:
        print(color(f'Recurring Pattern Clusters ({len(report["recurring_patterns"])}):', '36'))
        for tag, count in sorted(report['recurring_patterns'].items(), key=lambda x: -x[1]):
            print(f'  {tag}: {count} occurrences')
        print()

    if not any([
        report['stale_decisions'],
        report['expiring'],
        report['contradiction_candidates'],
        report['recurring_patterns'],
        report['unverified_auto_extracted'],
    ]):
        print(color('All clear - no issues found.', '32'))
        print()

    print(color(f'Relationships Detected ({len(new_relationship_rows)} new):', '1'))
    for item in new_relationship_rows:
        print(f'  [{item["src_id"]}] {item["relation"]} [{item["dst_id"]}] - {item["note"]}')
    if not new_relationship_rows:
        print('  None')
    print()

    print(f'Links Created: {links_created}')
    print(f'Superseded: {superseded_count} memories marked as superseded')
    if apply_changes:
        print(f'Duplicates Soft-Deleted: {duplicates_deleted}')
    else:
        print('Dry run: no changes applied (use --apply to persist links/supersedes/deletes).')

    return EXIT_OK

def capture_memory(conn, mem_type, scope, content, tags=None):
    """Helper to insert a memory record directly."""
    cur = conn.execute(
        "INSERT INTO memories(type, scope, content, tags) VALUES (?, ?, ?, ?)",
        (mem_type, scope, content, normalize_tags(tags)),
    )
    conn.commit()
    return cur.lastrowid


def cmd_model(args, conn):
    """Manage AI model selection and policy."""
    import os
    
    # Policy file path
    POLICY_FILE = Path(__file__).parent / "model-policy.json"
    
    if args.model_command == "status":
        # Load policy
        if POLICY_FILE.exists():
            policy = json.loads(POLICY_FILE.read_text())
        else:
            policy = {"base_model": "unknown", "fallback_chain": []}
        
        # Get current model from env or policy
        current_model = os.environ.get('OPENCODE_MODEL', policy.get('base_model', 'unknown'))
        
        if args.json:
            print(json.dumps({
                "current_model": current_model,
                "policy": policy
            }, indent=2))
            return EXIT_OK
        
        print(color('=== Model Status ===', '36'))
        print(f"Current model: {color(current_model, '1')}")
        print(f"Policy file: {POLICY_FILE}")
        print()
        print(color('Fallback Chain:', '1'))
        for item in policy.get('fallback_chain', []):
            if isinstance(item, dict):
                print(f"  Tier {item.get('tier', '?')}: {item.get('model', '?')} ({item.get('condition', '?')})")
        print()
        print(color('Escalation Criteria:', '1'))
        for criterion in policy.get('escalation_criteria', []):
            print(f"  • {criterion}")
        return EXIT_OK
    
    if args.model_command == "switch":
        model_id = args.model_id
        reason = args.reason or "Manual switch"
        duration = args.duration
        
        # Log the switch intent to memory
        capture_memory(conn, "decision", "nous-memory", 
            f"Model switch: {model_id} | Reason: {reason}" + (f" | Duration: {duration}" if duration else ""),
            tags="model,switch,autonomy")
        
        if args.json:
            print(json.dumps({
                "model": model_id,
                "reason": reason,
                "duration": duration,
                "note": "Set OPENCODE_MODEL environment variable or update config to activate"
            }, indent=2))
            return EXIT_OK
        
        print(color(f'Model switch initiated: {model_id}', '36'))
        print(f'Reason: {reason}')
        if duration:
            print(f'Duration: {duration}')
        print()
        print(color('To activate:', '33'))
        print(f'  export OPENCODE_MODEL={model_id}')
        print('  # Or update ~/.config/opencode/config.json')
        return EXIT_OK
    
    if args.model_command == "stats":
        days = args.days
        cutoff = (dt.datetime.now() - dt.timedelta(days=days)).isoformat()
        
        # Query model-related memories
        rows = conn.execute(
            """
            SELECT id, type, content, tags, created_at
            FROM memories
            WHERE (content LIKE '%model%' OR (',' || tags || ',' LIKE '%,model,%'))
              AND created_at > ?
              AND deleted_at IS NULL
            ORDER BY created_at DESC
            LIMIT 50
            """,
            (cutoff,)
        ).fetchall()
        
        # Extract model usage patterns
        switches = [r for r in rows if 'switch' in (r['tags'] or '')]
        successes = [r for r in rows if 'success' in (r['tags'] or '')]
        failures = [r for r in rows if 'failure' in (r['tags'] or '')]
        
        if args.json:
            print(json.dumps({
                "period_days": days,
                "total_model_events": len(rows),
                "switches": len(switches),
                "successes": len(successes),
                "failures": len(failures),
                "recent_events": [{"id": r["id"], "content": r["content"][:100]} for r in rows[:10]]
            }, indent=2))
            return EXIT_OK
        
        print(color(f'=== Model Stats (last {days} days) ===', '36'))
        print(f'Total model events: {len(rows)}')
        print(f'  Switches: {len(switches)}')
        print(f'  Successes: {len(successes)}')
        print(f'  Failures: {len(failures)}')
        if rows:
            print()
            print(color('Recent events:', '1'))
            for r in rows[:5]:
                print(f'  [{r["id"]}] {r["created_at"][:10]}: {r["content"][:60]}...')
        return EXIT_OK
    
    if args.model_command == "recommend":
        task = args.task_description or "general coding"
        files = args.files
        complexity = args.complexity
        
        # Recommendation logic based on policy
        recommendations = []
        
        if complexity == 'high' or files >= 5:
            recommendations.append({
                "model": "anthropic/claude-opus-4-6",
                "confidence": "high",
                "reason": "Complex task requiring deep reasoning"
            })
        elif files >= 3 or 'architecture' in task.lower():
            recommendations.append({
                "model": "github-copilot/gpt-4.1",
                "confidence": "medium",
                "reason": "Multi-file work, good context handling"
            })
        else:
            recommendations.append({
                "model": "kimi-for-coding/k2p5",
                "confidence": "high", 
                "reason": "Fast, cost-effective for routine tasks"
            })
        
        if args.json:
            print(json.dumps({
                "task": task,
                "files": files,
                "complexity": complexity,
                "recommendations": recommendations
            }, indent=2))
            return EXIT_OK
        
        print(color(f'=== Model Recommendation ===', '36'))
        print(f'Task: {task}')
        print(f'Files involved: {files}')
        print(f'Complexity: {complexity}')
        print()
        print(color('Recommendations:', '1'))
        for rec in recommendations:
            print(f"  {rec['model']} ({rec['confidence']} confidence)")
            print(f"    Why: {rec['reason']}")
        return EXIT_OK
    
    if args.model_command == "policy":
        if POLICY_FILE.exists():
            policy = json.loads(POLICY_FILE.read_text())
            if args.json:
                print(json.dumps(policy, indent=2))
            else:
                print(color('=== Model Policy ===', '36'))
                print(json.dumps(policy, indent=2))
        else:
            if args.json:
                print(json.dumps({"error": "Policy file not found"}))
            else:
                print(color('Policy file not found', '31'))
        return EXIT_OK
    
    if args.model_command == "select":
        task_type = args.task_type
        
        # Load policy
        if POLICY_FILE.exists():
            policy = json.loads(POLICY_FILE.read_text())
        else:
            return fail("Policy file not found")
        
        categories = policy.get('task_categories', {})
        
        if task_type not in categories:
            # Try fuzzy match
            matches = [k for k in categories if task_type.lower() in k.lower()]
            if len(matches) == 1:
                task_type = matches[0]
            elif matches:
                if args.json:
                    print(json.dumps({"error": "ambiguous", "matches": matches}))
                else:
                    print(color(f'Ambiguous task type "{task_type}". Did you mean:', '33'))
                    for m in matches:
                        cat = categories[m]
                        print(f'  {m} \u2014 {cat["description"]}')
                return EXIT_OK
            else:
                if args.json:
                    print(json.dumps({"error": "unknown", "available": list(categories.keys())}))
                else:
                    print(color(f'Unknown task type: {task_type}', '31'))
                    print('Available categories:')
                    for k, v in categories.items():
                        print(f'  {k} \u2014 {v["description"]}')
                return EXIT_OK
        
        cat = categories[task_type]
        selected_model = cat['model']
        needs_approval = cat.get('approval_required', False)
        
        # Log the selection to memory
        capture_memory(conn, "observation", "nous",
            f"Model select: {task_type} \u2192 {selected_model}" + (" (approval required)" if needs_approval else ""),
            tags="model,select,autonomy")
        
        if args.json:
            print(json.dumps({
                "task_type": task_type,
                "model": selected_model,
                "description": cat['description'],
                "approval_required": needs_approval,
                "fallback": policy.get('base_model', 'unknown')
            }, indent=2))
            return EXIT_OK
        
        if needs_approval:
            print(color(f'\u26a0 Task type "{task_type}" requires approval before switching', '33'))
        print(color(f'Selected model for {task_type}:', '36'))
        print(f'  Model: {color(selected_model, "1")}')
        print(f'  Description: {cat["description"]}')
        print(f'  Approval required: {"yes" if needs_approval else "no"}')
        print()
        if needs_approval:
            print(color('Action: Request user approval before switching.', '33'))
        else:
            print(color('Action: Safe to switch autonomously.', '32'))
            print(f'  export OPENCODE_MODEL={selected_model}')
        return EXIT_OK
    
    return fail(f"Unknown model command: {args.model_command}")
