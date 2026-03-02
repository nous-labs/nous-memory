"""nous-memory MCP server — exposes persistent memory tools via MCP protocol.

Wraps the existing nous-memory CLI as an MCP server for containerized Nous
instances and remote AI agents. Supports stdio and streamable-http transports.

Usage:
    # stdio (default — for MCP client integration)
    ./brain/nous-memory-mcp

    # streamable HTTP (for containers / remote access)
    ./brain/nous-memory-mcp --transport streamable-http --host 0.0.0.0 --port 8765
"""

import io
import json
import sys
from argparse import Namespace
from typing import Optional

try:
    import importlib
    FastMCP = importlib.import_module("mcp.server.fastmcp").FastMCP
except Exception as exc:
    FastMCP = None
    _MCP_IMPORT_ERROR = exc
else:
    _MCP_IMPORT_ERROR = None


from nous_memory import bootstrap, bridge, core, dream, entities as entities_mod, episodes as episodes_mod, kv as kv_mod, memory, models as models_mod, patterns, prompt, sessions as sessions_mod, stats, tasks as tasks_mod

# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------

mcp = None
if FastMCP is not None:
    mcp = FastMCP(
    "nous-memory",
    instructions=(
        "Persistent memory system for AI agents. "
        "Stores decisions, preferences, facts, observations, failures, and patterns "
        "in SQLite with FTS5 full-text search. "
        "Manages tasks, entities, sessions, and provides token-budgeted context bootstrapping."
    ),
    host="0.0.0.0",
    port=8765,
)

DB_PATH = core.resolve_db_path()


def _tool():
    if mcp is None:
        return lambda f: f
    return mcp.tool()



def _call(cmd_func, **kwargs):
    """Call an nous-memory command function, capturing JSON output.

    Creates a Namespace with json=True so all handlers produce JSON.
    Captures stdout (where handlers print) and stderr (where fail() prints).
    Returns parsed JSON on success, or an error dict on failure.
    """
    args = Namespace(json=True, verbose=False, db=DB_PATH, **kwargs)

    with core.connect_db(DB_PATH) as conn:
        core.ensure_schema(conn, DB_PATH)

        old_stdout, old_stderr = sys.stdout, sys.stderr
        sys.stdout = out_buf = io.StringIO()
        sys.stderr = err_buf = io.StringIO()
        try:
            code = cmd_func(args, conn)
        finally:
            sys.stdout, sys.stderr = old_stdout, old_stderr

        output = out_buf.getvalue().strip()
        errors = err_buf.getvalue().strip()

        if code != 0:
            return {"error": errors or output or "unknown error", "exit_code": code}

        if not output:
            return {"ok": True}

        try:
            return json.loads(output)
        except json.JSONDecodeError:
            return {"output": output}


# ===========================================================================
# Memory tools (3 high-use tools kept separate)
# ===========================================================================


@_tool()
def memory_capture(
    type: str,
    content: str,
    scope: str = "global",
    tags: Optional[str] = None,
    source: Optional[str] = None,
    expires: Optional[str] = None,
    topic_key: Optional[str] = None,
    no_synthesis: bool = False,
    metadata: Optional[str] = None,
) -> dict:
    """Store a memory.

    Args:
        type: Memory type — decision, preference, fact, observation, failure, or pattern.
        content: The memory content text.
        scope: Scope identifier (default: global). Usually a project name.
        tags: Comma-separated tags for categorization.
        source: Origin of the memory (e.g. session ID, URL).
        expires: Expiry date — ISO format or relative (tomorrow, in 3 days).
        topic_key: Stable topic identifier for in-place upserts.
        no_synthesis: Skip duplicate/similarity detection if True.
        metadata: Optional JSON string with additional data (e.g. '{"auto_extracted": true, "confidence": 0.65}').
    """
    return _call(
        memory.cmd_capture,
        type=type, content=content, scope=scope, tags=tags,
        source=source, expires=expires, topic_key=topic_key,
        no_synthesis=no_synthesis, metadata=metadata, headline=None,
    )


@_tool()
def memory_recall(
    query: Optional[str] = None,
    type: Optional[str] = None,
    scope: Optional[str] = None,
    tags: Optional[str] = None,
    limit: int = 20,
    active: bool = False,
    semantic: bool = False,
) -> list | dict:
    """Retrieve memories with optional FTS search and filters.

    Args:
        query: Full-text search query (optional).
        type: Filter by memory type (decision, preference, fact, observation, failure, pattern).
        scope: Filter by scope (e.g. project name).
        tags: Filter by tags (comma-separated, all must match).
        limit: Max results (default: 20).
        active: Only return non-superseded, non-expired memories.
        semantic: Use semantic search via daemon API (hybrid vector + keyword).
    """
    import os
    daemon_url = os.environ.get('NOUS_DAEMON_URL') if semantic else None
    return _call(
        memory.cmd_recall,
        query=query, type=type, scope=scope, tags=tags,
        limit=limit, active=active,
        semantic=semantic, daemon_url=daemon_url,
    )


@_tool()
def memory_search(
    query: str,
    type: Optional[str] = None,
    scope: Optional[str] = None,
    tags: Optional[str] = None,
    limit: int = 20,
    active: bool = False,
) -> list | dict:
    """Compact FTS search returning ranked snippets.

    Args:
        query: Full-text search query (required).
        type: Filter by memory type.
        scope: Filter by scope.
        tags: Filter by tags (comma-separated).
        limit: Max results (default: 20).
        active: Only non-superseded, non-expired.
    """
    return _call(
        memory.cmd_search,
        query=query, type=type, scope=scope, tags=tags,
        limit=limit, active=active,
    )


# ===========================================================================
# Memory operations (5 minor ops consolidated into 1)
# ===========================================================================


@_tool()
def memory_ops(
    action: str,
    id: Optional[int] = None,
    content: Optional[str] = None,
    hard: bool = False,
) -> dict:
    """Memory operations: get, timeline, update, forget, verify.

    Args:
        action: One of: get, timeline, update, forget, verify.
        id: Memory ID (required for all actions).
        content: New content (required for update action).
        hard: Permanent delete (only for forget action, default: False).
    """
    if action == "get":
        return _call(memory.cmd_get, id=id)
    elif action == "timeline":
        return _call(memory.cmd_timeline, id=id)
    elif action == "update":
        return _call(memory.cmd_update, id=id, content=content)
    elif action == "forget":
        return _call(memory.cmd_forget, id=id, hard=hard)
    elif action == "verify":
        return _call(core.cmd_verify, id=id)
    else:
        return {"error": f"Unknown action: {action}. Use: get, timeline, update, forget, verify"}


# ===========================================================================
# Bootstrap & analysis (consolidated)
# ===========================================================================


@_tool()
def memory_analyze(
    action: str,
    scope: Optional[str] = None,
    budget: int = 4000,
    recent_limit: int = 5,
    stale_days: int = 30,
) -> dict:
    """Memory analysis: bootstrap context, dream analysis, or stats.

    Args:
        action: One of: bootstrap, dream, stats.
        scope: Project scope filter (for bootstrap/dream).
        budget: Max characters for bootstrap output (default: 4000).
        recent_limit: Max recent memories for bootstrap (default: 5).
        stale_days: Threshold for stale decisions in dream (default: 30).
    """
    if action == "bootstrap":
        return _call(
            bootstrap.cmd_bootstrap,
            scope=scope, budget=budget, recent_limit=recent_limit, handoff=False,
            critical_only=True,
        )
    elif action == "dream":
        return _call(
            dream.cmd_dream,
            scope=scope, stale_days=stale_days, command="dream",
        )
    elif action == "stats":
        return _call(stats.cmd_stats)
    else:
        return {"error": f"Unknown action: {action}. Use: bootstrap, dream, stats"}


# ===========================================================================
# Task tools (consolidated)
# ===========================================================================


@_tool()
def task(
    action: str,
    id: Optional[int] = None,
    title: Optional[str] = None,
    priority: str = "medium",
    due: Optional[str] = None,
    description: Optional[str] = None,
    repeat_rule: Optional[str] = None,
    entity_id: Optional[int] = None,
    entity_name: Optional[str] = None,
    tags: Optional[str] = None,
    all: bool = False,
) -> list | dict:
    """Task management: list, add, done, cancel, remind.

    Args:
        action: One of: list, add, done, cancel, remind.
        id: Task ID (for done/cancel).
        title: Task title (for add/remind).
        priority: low, medium (default), high, or critical (for add).
        due: Due date — ISO format or relative (for add/remind).
        description: Optional description (for add).
        repeat_rule: Cron expression for recurring tasks (for add).
        entity_id: Link to entity by ID (for add).
        entity_name: Link to entity by name (for add).
        tags: Comma-separated tags (for add).
        all: Include all statuses in list (default: pending only).
    """
    if action == "list":
        return _call(tasks_mod.cmd_tasks, tasks_command=None, all=all, due=False)
    elif action == "add":
        return _call(
            tasks_mod.cmd_tasks,
            tasks_command="add", title=title, priority=priority,
            due=due, description=description, repeat_rule=repeat_rule,
            entity_id=entity_id, entity_name=entity_name, tags=tags,
            all=False,
        )
    elif action == "done":
        return _call(tasks_mod.cmd_tasks, tasks_command="done", id=id, all=False, due=False)
    elif action == "cancel":
        return _call(tasks_mod.cmd_tasks, tasks_command="cancel", id=id, all=False, due=False)
    elif action == "remind":
        return _call(tasks_mod.cmd_remind, when=due, title=title)
    else:
        return {"error": f"Unknown action: {action}. Use: list, add, done, cancel, remind"}


# ===========================================================================
# Entity tools (consolidated)
# ===========================================================================


@_tool()
def entity(
    action: str,
    name: Optional[str] = None,
    type: Optional[str] = None,
    metadata: Optional[str] = None,
) -> list | dict:
    """Entity management: add, show, list.

    Args:
        action: One of: add, show, list.
        name: Entity name (for add/show).
        type: Entity type — project, provider, tool, person, or repo (for add).
        metadata: Optional JSON string with additional data (for add).
    """
    if action == "add":
        return _call(entities_mod.cmd_entities, entities_command="add", type=type, name=name, metadata=metadata)
    elif action == "show":
        return _call(entities_mod.cmd_entities, entities_command="show", name=name)
    elif action == "list":
        return _call(entities_mod.cmd_entities, entities_command=None)
    else:
        return {"error": f"Unknown action: {action}. Use: add, show, list"}


# ===========================================================================
# Session tools (consolidated)
# ===========================================================================


@_tool()
def session(
    action: str,
    id: Optional[str] = None,
    session_id: Optional[str] = None,
    summary: Optional[str] = None,
    files: Optional[str] = None,
    decisions: Optional[str] = None,
    limit: int = 20,
) -> list | dict:
    """Session reference management: log, list, show.

    Args:
        action: One of: log, list, show.
        id: Session identifier (for log).
        session_id: Session ID to look up (for show).
        summary: Brief session summary (for log).
        files: JSON array of modified files (for log).
        decisions: JSON array of decisions made (for log).
        limit: Max sessions to return (for list, default: 20).
    """
    if action == "log":
        return _call(sessions_mod.cmd_session, session_command="log", id=id, summary=summary, files=files, decisions=decisions)
    elif action == "list":
        return _call(sessions_mod.cmd_session, session_command="list", limit=limit)
    elif action == "show":
        return _call(sessions_mod.cmd_session, session_command="show", session_id=session_id)
    else:
        return {"error": f"Unknown action: {action}. Use: log, list, show"}


# ===========================================================================
# KV tools (consolidated)
# ===========================================================================


@_tool()
def kv(
    action: str,
    key: Optional[str] = None,
    value: Optional[str] = None,
) -> list | dict:
    """Key-value store: get, set, list, delete.

    Args:
        action: One of: get, set, list, delete.
        key: Key name (for get/set/delete).
        value: JSON string value (for set).
    """
    if action == "get":
        return _call(kv_mod.cmd_kv, kv_command="get", key=key)
    elif action == "set":
        return _call(kv_mod.cmd_kv, kv_command="set", key=key, value=value)
    elif action == "list":
        return _call(kv_mod.cmd_kv, kv_command=None, key=None)
    elif action == "delete":
        return _call(kv_mod.cmd_kv, kv_command="delete", key=key)
    else:
        return {"error": f"Unknown action: {action}. Use: get, set, list, delete"}


# ===========================================================================
# Episode tools (consolidated)
# ===========================================================================


@_tool()
def episode(
    action: str,
    scope: Optional[str] = None,
    intent: Optional[str] = None,
    summary: Optional[str] = None,
    limit: int = 20,
) -> list | dict:
    """Episode management: start, end, list, current.

    Args:
        action: One of: start, end, list, current.
        scope: Project scope (for start/end/list/current).
        intent: What this episode is about (for start).
        summary: Episode summary (for end).
        limit: Max results (for list, default: 20).
    """
    if action == "start":
        return _call(episodes_mod.cmd_episode, episode_command="start", scope=scope, intent=intent)
    elif action == "end":
        return _call(episodes_mod.cmd_episode, episode_command="end", scope=scope, summary=summary)
    elif action == "list":
        return _call(episodes_mod.cmd_episode, episode_command="list", scope=scope, limit=limit)
    elif action == "current":
        return _call(episodes_mod.cmd_episode, episode_command="current", scope=scope)
    else:
        return {"error": f"Unknown action: {action}. Use: start, end, list, current"}


# ===========================================================================
# Pattern analysis tools (consolidated)
# ===========================================================================


@_tool()
def pattern(
    action: str,
    threshold: int = 3,
    include_all: bool = False,
    tag: Optional[str] = None,
) -> dict:
    """Pattern analysis: analyze clusters, suggest improvements, propose changes, sync to markdown.

    Args:
        action: One of: analyze, suggest, propose, sync.
        threshold: Minimum occurrences to count as recurring (default: 3).
        include_all: Include all memory types in analyze (default: False).
        tag: Tag to generate proposal from (for propose).
    """
    if action == "analyze":
        return _call(patterns.cmd_patterns, patterns_command="analyze", threshold=threshold, include_all=include_all)
    elif action == "suggest":
        return _call(patterns.cmd_patterns, patterns_command="suggest", threshold=threshold)
    elif action == "propose":
        return _call(patterns.cmd_patterns, patterns_command="propose", tag=tag, threshold=threshold)
    elif action == "sync":
        return _call(patterns.cmd_patterns, patterns_command="sync")
    else:
        return {"error": f"Unknown action: {action}. Use: analyze, suggest, propose, sync"}


# ===========================================================================
# Prompt management tools (consolidated)
# ===========================================================================


@_tool()
def prompt_mgmt(
    action: str,
    message: Optional[str] = None,
    description: Optional[str] = None,
    file: Optional[str] = None,
    proposal_id: Optional[int] = None,
    proposal: Optional[int] = None,
    reason: Optional[str] = None,
    v_from: Optional[str] = None,
    v_to: Optional[str] = None,
    all: bool = False,
) -> list | dict:
    """Prompt version management: show, snapshot, propose, proposals, apply, reject, history, diff.

    Args:
        action: One of: show, snapshot, propose, proposals, apply, reject, history, diff.
        message: Snapshot description (for snapshot).
        description: Brief description of proposed change (for propose).
        file: Optional source file with proposed content (for propose).
        proposal_id: Proposal ID (for apply/reject).
        proposal: Proposal ID to diff against active (for diff).
        reason: Reason for rejection (for reject).
        v_from: Version to diff from (for diff).
        v_to: Version to diff to (for diff).
        all: Include all statuses in proposals list (default: pending only).
    """
    if action == "show":
        return _call(prompt.cmd_prompt, prompt_command="show")
    elif action == "snapshot":
        return _call(prompt.cmd_prompt, prompt_command="snapshot", message=message)
    elif action == "propose":
        return _call(prompt.cmd_prompt, prompt_command="propose", description=description, file=file)
    elif action == "proposals":
        return _call(prompt.cmd_prompt, prompt_command="proposals", all=all)
    elif action == "apply":
        return _call(prompt.cmd_prompt, prompt_command="apply", proposal_id=proposal_id)
    elif action == "reject":
        return _call(prompt.cmd_prompt, prompt_command="reject", proposal_id=proposal_id, reason=reason)
    elif action == "history":
        return _call(prompt.cmd_prompt, prompt_command="history")
    elif action == "diff":
        return _call(prompt.cmd_prompt, prompt_command="diff", proposal=proposal, v_from=v_from, v_to=v_to)
    else:
        return {"error": f"Unknown action: {action}. Use: show, snapshot, propose, proposals, apply, reject, history, diff"}


# ===========================================================================
# Bridge tools (consolidated)
# ===========================================================================


@_tool()
def bridge_mgmt(
    action: str,
    project_dir: Optional[str] = None,
    force: bool = False,
) -> list | dict:
    """Bridge AGENTS.md management: generate, sync, list, remove.

    Args:
        action: One of: generate, sync, list, remove.
        project_dir: Path to the project directory (for generate/remove).
        force: Overwrite existing AGENTS.md (for generate, default: False).
    """
    if action == "generate":
        return _call(bridge.cmd_bridge, bridge_command="generate", project_dir=project_dir, force=force)
    elif action == "sync":
        return _call(bridge.cmd_bridge, bridge_command="sync")
    elif action == "list":
        return _call(bridge.cmd_bridge, bridge_command="list")
    elif action == "remove":
        return _call(bridge.cmd_bridge, bridge_command="remove", project_dir=project_dir)
    else:
        return {"error": f"Unknown action: {action}. Use: generate, sync, list, remove"}


# ===========================================================================
# Model management tools (consolidated)
# ===========================================================================


@_tool()
def model(
    action: str,
    model_id: Optional[str] = None,
    reason: Optional[str] = None,
    duration: Optional[str] = None,
    days: int = 7,
    task_description: Optional[str] = None,
    files: int = 0,
    complexity: str = "medium",
    task_type: Optional[str] = None,
) -> dict:
    """Model management: status, switch, stats, select, recommend, policy.

    Args:
        action: One of: status, switch, stats, select, recommend, policy.
        model_id: Model identifier (for switch, e.g. "kimi-for-coding/k2p5").
        reason: Reason for switch (for switch).
        duration: Temporary switch duration (for switch, e.g. "30m", "2h").
        days: Lookback period for stats (default: 7).
        task_description: Description of the task (for recommend).
        files: Number of files involved (for recommend, default: 0).
        complexity: Task complexity — low, medium, or high (for recommend).
        task_type: Task category (for select, e.g. "quick_fix", "architecture").
    """
    if action == "status":
        return _call(models_mod.cmd_model, model_command="status")
    elif action == "switch":
        return _call(models_mod.cmd_model, model_command="switch", model_id=model_id, reason=reason, duration=duration)
    elif action == "stats":
        return _call(models_mod.cmd_model, model_command="stats", days=days)
    elif action == "select":
        return _call(models_mod.cmd_model, model_command="select", task_type=task_type)
    elif action == "recommend":
        return _call(models_mod.cmd_model, model_command="recommend", task_description=task_description, files=files, complexity=complexity)
    elif action == "policy":
        return _call(models_mod.cmd_model, model_command="policy")
    else:
        return {"error": f"Unknown action: {action}. Use: status, switch, stats, select, recommend, policy"}


# ===========================================================================
# Entry point
# ===========================================================================

def main():
    if _MCP_IMPORT_ERROR is not None:
        print(f"Error: install with mcp extra: pip install 'nous-memory[mcp]' ({_MCP_IMPORT_ERROR})", file=sys.stderr)
        return 1
    assert mcp is not None

    import argparse

    parser = argparse.ArgumentParser(
        description="nous-memory MCP server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  ./nous-memory-mcp                                    # stdio transport\n"
            "  ./nous-memory-mcp --transport streamable-http        # HTTP on 127.0.0.1:8765\n"
            "  ./nous-memory-mcp --transport streamable-http --host 0.0.0.0 --port 9000\n"
        ),
    )
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "streamable-http"],
        default="stdio",
        help="MCP transport protocol (default: stdio)",
    )
    parser.add_argument("--host", default="127.0.0.1", help="bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8765, help="bind port (default: 8765)")
    parser.add_argument("--db", type=str, default=None, help="SQLite DB path")

    cli_args = parser.parse_args()
    # In mcp>=1.26, host/port are __init__ params, not run() params.
    # Reconfigure if non-default values provided.
    if cli_args.transport != "stdio":
        mcp.settings.host = cli_args.host
        mcp.settings.port = cli_args.port

    if cli_args.db:
        global DB_PATH
        DB_PATH = core.resolve_db_path(cli_args.db)
    else:
        DB_PATH = core.resolve_db_path()

    mcp.run(transport=cli_args.transport)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
