# nous-memory

Persistent memory system for AI agents — capture, recall, and evolve knowledge across sessions.

## What is this?

AI agents typically start each session with no memory of past work, past decisions, or project context. nous-memory solves this by providing a persistent SQLite-based memory store with full-text search, task management, and intelligent context bootstrapping.

Built for opencode/oh-my-opencode users and AI agent developers who need their assistants to remember things across sessions.

## Features

**Core Memory Management**

- Six memory types: decision, preference, fact, observation, failure, pattern
- Full-text search via SQLite FTS5 with Porter stemming
- Automatic duplicate detection and similarity synthesis
- Topic-key upserts for evolving memories (preferences, constraints)
- Soft-delete with recovery, or hard-delete for permanent removal

**Context & Bootstrapping**

- Token-budgeted bootstrap — returns prioritized context within a character limit
- Constraints (always included), alerts (overdue tasks), pending tasks, recent memories, patterns
- Episode tracking — group memories by work session with access scoring
- Handoff mode — export full context for session continuity

**Task & Entity Management**

- Tasks with priorities, due dates, and repeat rules
- Reminders with natural date parsing ("tomorrow", "in 3 days", "next monday")
- Entities (projects, providers, tools, people, repos) with linked memories
- Key-value store for configuration and state

**Self-Improvement & Maintenance**

- Pattern analysis — detect recurring failure/pattern tag clusters
- Dream/staleness reports — identify stale decisions, expiring memories, contradiction candidates
- Prompt proposal system — versioned AGENTS.md management
- Bridge generation — sync constraints to project AGENTS.md files

**Integration**

- MCP server for modern AI agent protocols (stdio and HTTP transports)
- Optional semantic search via nous-daemon (hybrid vector + keyword)
- JSON output for programmatic use
- Zero dependencies (stdlib only, Python 3.10+)

## Quick Start

### Installation

```bash
# From PyPI (when published)
pip install nous-memory

# With MCP support
pip install 'nous-memory[mcp]'

# From source
git clone https://github.com/nous-labs/nous-memory.git
cd nous-memory
pip install -e .
```

### Basic Usage

Capture a memory:

```bash
$ nous-memory capture --type decision --scope myproject --tags "api,auth" "Use JWT for API authentication"
Captured memory [1]
```

Recall memories:

```bash
$ nous-memory recall --scope myproject auth
[1] decision  scope=myproject  created=just now
  content: Use JWT for API authentication
  tags: api, auth
```

Search with compact output:

```bash
$ nous-memory search "JWT"
[1] decision  scope=myproject  rank=-0.0000
  Use JWT for API authentication
  tags: api, auth
```

Bootstrap session context (the killer feature):

```bash
$ nous-memory bootstrap --scope myproject --budget 2000
Constraints (hard preferences):
  - Use semantic versioning for releases
  - Never commit without tests

Alerts (overdue/due soon):
  [3] Review auth implementation  due=2h ago (overdue)

Tasks (pending):
  [4] Update documentation        priority=medium
  [5] Fix linting errors          priority=high

Recent decisions/observations (myproject):
  [1] decision: Use JWT for API authentication
  [2] observation: Database migration took 45s

Patterns (failures/patterns):
  - auth,security: 5 occurrences (review for prompt improvement)
```

Manage tasks:

```bash
$ nous-memory tasks add "Review PR #42" --priority high --due tomorrow
Created task [6]

$ nous-memory tasks
[6] Review PR #42  status=pending  priority=high  due=tomorrow

$ nous-memory tasks done 6
Task [6] -> done

$ nous-memory remind "in 2 hours" "Deploy to staging"
Reminder task [7] due 2026-02-26T15:30:00
```

View system health:

```bash
$ nous-memory stats
Database: ~/.local/share/nous-memory/state.db
Size: 1.2 MB

Memories: 142 total
  decision: 45 | preference: 12 | fact: 23
  observation: 38 | failure: 15 | pattern: 9

Tasks: 8 pending, 3 done, 1 cancelled
Entities: 12
FTS5 index: ready
```

### MCP Server

Run the MCP server for remote or containerized access:

```bash
# stdio transport (default, for MCP client integration)
nous-memory-mcp

# HTTP transport for containers
nous-memory-mcp --transport streamable-http --host 0.0.0.0 --port 8765
```

Configure in your MCP client (Claude Desktop, Cline, etc.):

```json
{
  "mcpServers": {
    "nous-memory": {
      "command": "nous-memory-mcp",
      "env": {
        "NOUS_MEMORY_DB": "/path/to/state.db"
      }
    }
  }
}
```

## Architecture

**SQLite Backend**

Everything stores in a single SQLite file. No server required for basic operation. Schema includes:

- `memories` — core memory storage with validity timestamps
- `memories_fts` — virtual FTS5 table for full-text search
- `tasks`, `entities`, `kv` — supporting tables
- `episodes`, `episode_memories` — session grouping
- `memory_access_events`, `memory_access_stats` — usage tracking

**Memory Types**

| Type | Purpose | Default Policy |
|------|---------|----------------|
| decision | Architectural choices, design decisions | TTL 90 days |
| preference | Hard constraints, style preferences | Never stale |
| fact | Verified knowledge | TTL 60 days |
| observation | Notes, discoveries | TTL 60 days |
| failure | What went wrong and why | Half-life decay |
| pattern | Recurring effective approaches | TTL 60 days |

**Token-Budgeted Bootstrap**

The bootstrap command returns context in priority order until the character budget is exhausted:

1. Constraints (always included)
2. Alerts (overdue/due-soon tasks)
3. All pending tasks
4. Recent memories for scope
5. Patterns for scope

This prevents context overflow while ensuring critical information is always included.

**Dream/Staleness Maintenance**

The `dream` command produces a maintenance report:

- Stale decisions — decisions older than threshold that may need review
- Expiring memories — approaching their expires_at date
- Contradiction candidates — overlapping active decisions that may conflict
- Recurring pattern clusters — tag combinations appearing frequently
- Memory health statistics

**Episode Tracking**

Episodes group related work. When you `episode start myproject`, subsequent memory captures are linked to that episode. Access scoring tracks which memories are recalled most frequently within episodes, improving bootstrap relevance.

## Integration with OpenCode / oh-my-opencode

**Session Bootstrap Pattern**

Add to your project's `AGENTS.md`:

```markdown
## Session Bootstrap

On every new session, run:

```bash
nous-memory bootstrap --scope myproject
```

This returns constraints, alerts, tasks, and recent context in a token-budgeted format.
```

**Hook Integration**

Use hooks to auto-capture decisions and patterns. Example post-task hook:

```bash
#!/bin/bash
# ~/.config/opencode/hooks/post-task.sh
if [ "$EXIT_CODE" -ne 0 ]; then
  nous-memory capture --type failure --tags "$TASK_TAGS" "$TASK_ERROR"
fi
```

**Containerized Setups**

For Docker or remote environments, use the MCP server:

```bash
# On host, expose via HTTP
nous-memory-mcp --transport streamable-http --port 8765

# In container, point to host
docker run -e NOUS_DAEMON_URL=http://host.docker.internal:8765 myimage
```

## Configuration

**Database Path Resolution**

Precedence: `--db` flag > `NOUS_MEMORY_DB` env > XDG default

```bash
# Default location
~/.local/share/nous-memory/state.db

# Custom location
export NOUS_MEMORY_DB=/workspace/brain/state.db
```

**Environment Variables**

| Variable | Purpose |
|----------|---------|
| `NOUS_MEMORY_DB` | SQLite database path |
| `NOUS_MEMORY_WORKSPACE` | Workspace root for prompt/bridge resolution |
| `NOUS_DAEMON_URL` | URL for semantic search daemon (default: http://localhost:8080) |

**Semantic Search**

Enable hybrid vector + keyword search via nous-daemon:

```bash
# Requires running daemon
export NOUS_DAEMON_URL=http://localhost:8080
nous-memory recall --semantic "authentication patterns"
```

## All Commands

| Command | Description |
|---------|-------------|
| `capture` | Store a memory |
| `recall` | Retrieve memories with optional search |
| `search` | Compact FTS search with ranked snippets |
| `get` | Show full details for one memory |
| `timeline` | Show +/-24h context around a memory |
| `update` | Update memory by superseding |
| `forget` | Soft or hard delete a memory |
| `tasks` | List, add, done, cancel tasks |
| `remind` | Create reminder task with due date |
| `entities` | Manage entities (projects, tools, etc.) |
| `kv` | Key-value store (get, set, list, delete) |
| `session` | Log and list session references |
| `episode` | Start, end, list, show current episodes |
| `bootstrap` | Token-budgeted session context |
| `dream` | Staleness report and maintenance |
| `stats` | Memory system statistics |
| `patterns` | Analyze, suggest, propose, sync |
| `prompt` | Manage AGENTS.md prompts |
| `bridge` | Generate project bridge files |
| `model` | Model selection and policy |

Global flags: `--db`, `--workspace`, `--json`, `--verbose`, `--init`

## License

MIT
