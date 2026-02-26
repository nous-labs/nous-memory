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


from nous_memory import bootstrap, bridge, core, dream, entities, episodes, kv, memory, models, patterns, prompt, sessions, stats, tasks

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
# Memory tools
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
    """
    return _call(
        memory.cmd_capture,
        type=type, content=content, scope=scope, tags=tags,
        source=source, expires=expires, topic_key=topic_key,
        no_synthesis=no_synthesis,
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


@_tool()
def memory_get(id: int) -> dict:
    """Get full details for a single memory by its ID."""
    return _call(memory.cmd_get, id=id)


@_tool()
def memory_timeline(id: int) -> dict:
    """Show ±24 hour context around a memory — what was captured before and after."""
    return _call(memory.cmd_timeline, id=id)


@_tool()
def memory_update(id: int, content: str) -> dict:
    """Update a memory by creating a new version that supersedes the old one.

    Args:
        id: Memory ID to supersede.
        content: New content for the memory.
    """
    return _call(memory.cmd_update, id=id, content=content)


@_tool()
def memory_forget(id: int, hard: bool = False) -> dict:
    """Delete a memory. Soft-delete by default (recoverable). Use hard=True for permanent removal.

    Args:
        id: Memory ID to delete.
        hard: If True, permanently delete (cannot be recovered).
    """
    return _call(memory.cmd_forget, id=id, hard=hard)


# ===========================================================================
# Bootstrap & analysis
# ===========================================================================


@_tool()
def memory_bootstrap(
    scope: Optional[str] = None,
    budget: int = 4000,
    recent_limit: int = 5,
) -> dict:
    """Token-budgeted session context injection.

    Returns prioritized context within a character budget:
    1. Constraints (hard-constraint preferences — always included)
    2. Alerts (due/overdue tasks)
    3. Tasks (all pending)
    4. Recent decisions/observations for scope
    5. Patterns (failures and patterns for scope)

    Args:
        scope: Project scope filter (e.g. "nous-memory").
        budget: Max characters for output (default: 4000).
        recent_limit: Max recent memories to include (default: 5).
    """
    return _call(
        bootstrap.cmd_bootstrap,
        scope=scope, budget=budget, recent_limit=recent_limit, handoff=False,
    )


@_tool()
def memory_dream(
    scope: Optional[str] = None,
    stale_days: int = 30,
) -> dict:
    """Background dreaming — idle-time memory re-evaluation.

    Produces a report with:
    - Stale decisions (older than stale_days, may need review)
    - Expiring memories (approaching expires_at)
    - Contradiction candidates (overlapping active decisions)
    - Recurring pattern clusters
    - Memory health statistics

    Args:
        scope: Scope filter (e.g. project name).
        stale_days: Threshold in days for stale decisions (default: 30).
    """
    return _call(
        dream.cmd_dream,
        scope=scope, stale_days=stale_days, command="dream",
    )


@_tool()
def memory_stats() -> dict:
    """Show memory system statistics — counts by type, search index status, tasks, entities, DB size."""
    return _call(stats.cmd_stats)


# ===========================================================================
# Task tools
# ===========================================================================


@_tool()
def task_list(
    all: bool = False,
    due: bool = False,
) -> list | dict:
    """List tasks.

    Args:
        all: Include all statuses (pending, active, done, cancelled). Default: pending only.
        due: Show only overdue and due-within-24h tasks.
    """
    return _call(
        tasks.cmd_tasks,
        tasks_command=None, all=all, due=due,
    )


@_tool()
def task_add(
    title: str,
    priority: str = "medium",
    due: Optional[str] = None,
    description: Optional[str] = None,
    repeat_rule: Optional[str] = None,
    entity_id: Optional[int] = None,
    entity_name: Optional[str] = None,
    tags: Optional[str] = None,
) -> dict:
    """Add a new task.

    Args:
        title: Task title.
        priority: low, medium (default), high, or critical.
        due: Due date — ISO format or relative (tomorrow, in 3 days, next monday).
        description: Optional longer description.
        repeat_rule: Cron expression for recurring tasks.
        entity_id: Link to entity by ID.
        entity_name: Link to entity by name.
        tags: Comma-separated tags.
    """
    return _call(
        tasks.cmd_tasks,
        tasks_command="add", title=title, priority=priority,
        due=due, description=description, repeat_rule=repeat_rule,
        entity_id=entity_id, entity_name=entity_name, tags=tags,
        all=False,
    )


@_tool()
def task_done(id: int) -> dict:
    """Mark a task as done.

    Args:
        id: Task ID to mark complete.
    """
    return _call(tasks.cmd_tasks, tasks_command="done", id=id, all=False, due=False)


@_tool()
def task_cancel(id: int) -> dict:
    """Cancel a task.

    Args:
        id: Task ID to cancel.
    """
    return _call(tasks.cmd_tasks, tasks_command="cancel", id=id, all=False, due=False)


@_tool()
def remind(when: str, title: str) -> dict:
    """Create a reminder task with a due date.

    Args:
        when: Due date — ISO format or relative (tomorrow, in 3 days, next monday, 2026-03-12).
        title: Reminder description.
    """
    return _call(tasks.cmd_remind, when=when, title=title)


# ===========================================================================
# Entity tools
# ===========================================================================


@_tool()
def entity_add(type: str, name: str, metadata: Optional[str] = None) -> dict:
    """Add a new entity.

    Args:
        type: Entity type — project, provider, tool, person, or repo.
        name: Unique entity name.
        metadata: Optional JSON string with additional data.
    """
    return _call(
        entities.cmd_entities,
        entities_command="add", type=type, name=name, metadata=metadata,
    )


@_tool()
def entity_show(name: str) -> dict:
    """Show entity details and all linked memories.

    Args:
        name: Entity name to look up.
    """
    return _call(entities.cmd_entities, entities_command="show", name=name)


@_tool()
def entity_list() -> list | dict:
    """List all entities with their types and metadata."""
    return _call(entities.cmd_entities, entities_command=None)


# ===========================================================================
# Session tools
# ===========================================================================


@_tool()
def session_log(
    id: str,
    summary: Optional[str] = None,
    files: Optional[str] = None,
    decisions: Optional[str] = None,
) -> dict:
    """Log a session reference for cross-session continuity.

    Args:
        id: Session identifier.
        summary: Brief session summary.
        files: JSON array of modified files.
        decisions: JSON array of decisions made.
    """
    return _call(
        sessions.cmd_session,
        session_command="log", id=id, summary=summary,
        files=files, decisions=decisions,
    )


@_tool()
def session_list(limit: int = 20) -> list | dict:
    """List recent session references.

    Args:
        limit: Max sessions to return (default: 20).
    """
    return _call(sessions.cmd_session, session_command="list", limit=limit)


@_tool()
def session_show(session_id: str) -> dict:
    """Show full details for a session reference.

    Args:
        session_id: Session ID to look up.
    """
    return _call(sessions.cmd_session, session_command="show", session_id=session_id)


# ===========================================================================
# KV tools
# ===========================================================================


@_tool()
def kv_get(key: str) -> dict:
    """Get a value from the key-value store.

    Args:
        key: Key to look up.
    """
    return _call(kv.cmd_kv, kv_command="get", key=key)


@_tool()
def kv_set(key: str, value: str) -> dict:
    """Set a value in the key-value store.

    Args:
        key: Key to set.
        value: JSON string value.
    """
    return _call(kv.cmd_kv, kv_command="set", key=key, value=value)


@_tool()
def kv_list() -> list | dict:
    """List all key-value entries."""
    return _call(kv.cmd_kv, kv_command=None, key=None)


@_tool()
def kv_delete(key: str) -> dict:
    """Delete a key from the key-value store.

    Args:
        key: Key to delete.
    """
    return _call(kv.cmd_kv, kv_command="delete", key=key)


# ===========================================================================
# Episode tools
# ===========================================================================


@_tool()
def episode_start(scope: str, intent: Optional[str] = None) -> dict:
    """Start a new episode for a scope.

    Args:
        scope: Project scope (e.g. "nous-memory").
        intent: What this episode is about.
    """
    return _call(episodes.cmd_episode, episode_command="start", scope=scope, intent=intent)


@_tool()
def episode_end(scope: Optional[str] = None, summary: Optional[str] = None) -> dict:
    """End the current active episode.

    Args:
        scope: Scope to end episode for (optional if only one active).
        summary: Episode summary.
    """
    return _call(episodes.cmd_episode, episode_command="end", scope=scope, summary=summary)


@_tool()
def episode_list(scope: Optional[str] = None, limit: int = 20) -> list | dict:
    """List episodes.

    Args:
        scope: Filter by scope.
        limit: Max results (default: 20).
    """
    return _call(episodes.cmd_episode, episode_command="list", scope=scope, limit=limit)


@_tool()
def episode_current(scope: Optional[str] = None) -> dict:
    """Show current active episode.

    Args:
        scope: Filter by scope.
    """
    return _call(episodes.cmd_episode, episode_command="current", scope=scope)


# ===========================================================================
# Pattern analysis tools
# ===========================================================================


@_tool()
def patterns_analyze(threshold: int = 3, include_all: bool = False) -> dict:
    """Find recurring tag clusters in failure/pattern memories.

    Args:
        threshold: Minimum occurrences to count as recurring (default: 3).
        include_all: Include all memory types, not just failure/pattern.
    """
    return _call(patterns.cmd_patterns, patterns_command="analyze", threshold=threshold, include_all=include_all)


@_tool()
def patterns_suggest(threshold: int = 3) -> dict:
    """Suggest prompt improvements from recurring patterns.

    Args:
        threshold: Minimum occurrences (default: 3).
    """
    return _call(patterns.cmd_patterns, patterns_command="suggest", threshold=threshold)


@_tool()
def patterns_propose(tag: str, threshold: int = 3) -> dict:
    """Auto-generate a prompt change proposal from a recurring tag.

    Args:
        tag: Tag to generate proposal from.
        threshold: Minimum occurrences (default: 3).
    """
    return _call(patterns.cmd_patterns, patterns_command="propose", tag=tag, threshold=threshold)


@_tool()
def patterns_sync() -> dict:
    """Sync failure/pattern memories to knowledge markdown files."""
    return _call(patterns.cmd_patterns, patterns_command="sync")


# ===========================================================================
# Prompt management tools
# ===========================================================================


@_tool()
def prompt_show() -> dict:
    """Show the active AGENTS.md prompt content and version."""
    return _call(prompt.cmd_prompt, prompt_command="show")


@_tool()
def prompt_snapshot(message: Optional[str] = None) -> dict:
    """Snapshot the current prompt version.

    Args:
        message: Snapshot description message.
    """
    return _call(prompt.cmd_prompt, prompt_command="snapshot", message=message)


@_tool()
def prompt_propose(description: str, file: Optional[str] = None) -> dict:
    """Create a prompt change proposal.

    Args:
        description: Brief description of the proposed change.
        file: Optional source file with proposed content.
    """
    return _call(prompt.cmd_prompt, prompt_command="propose", description=description, file=file)


@_tool()
def prompt_proposals(all: bool = False) -> list | dict:
    """List prompt change proposals.

    Args:
        all: Include all statuses, not just pending.
    """
    return _call(prompt.cmd_prompt, prompt_command="proposals", all=all)


@_tool()
def prompt_apply(proposal_id: int) -> dict:
    """Apply a pending prompt proposal to AGENTS.md.

    Args:
        proposal_id: ID of the proposal to apply.
    """
    return _call(prompt.cmd_prompt, prompt_command="apply", proposal_id=proposal_id)


@_tool()
def prompt_reject(proposal_id: int, reason: Optional[str] = None) -> dict:
    """Reject a prompt proposal.

    Args:
        proposal_id: ID of the proposal to reject.
        reason: Reason for rejection.
    """
    return _call(prompt.cmd_prompt, prompt_command="reject", proposal_id=proposal_id, reason=reason)


@_tool()
def prompt_history() -> list | dict:
    """Show prompt version history."""
    return _call(prompt.cmd_prompt, prompt_command="history")


@_tool()
def prompt_diff(
    proposal: Optional[int] = None,
    v_from: Optional[str] = None,
    v_to: Optional[str] = None,
) -> dict:
    """Diff prompt versions or a proposal against active.

    Args:
        proposal: Proposal ID to diff against active prompt.
        v_from: Version to diff from (e.g. "1").
        v_to: Version to diff to (e.g. "2").
    """
    return _call(prompt.cmd_prompt, prompt_command="diff", proposal=proposal, v_from=v_from, v_to=v_to)


# ===========================================================================
# Bridge tools
# ===========================================================================


@_tool()
def bridge_generate(project_dir: str, force: bool = False) -> dict:
    """Generate a bridge AGENTS.md for a project directory.

    Args:
        project_dir: Path to the project directory.
        force: Overwrite existing AGENTS.md if present.
    """
    return _call(bridge.cmd_bridge, bridge_command="generate", project_dir=project_dir, force=force)


@_tool()
def bridge_sync() -> dict:
    """Regenerate all registered bridge AGENTS.md files."""
    return _call(bridge.cmd_bridge, bridge_command="sync")


@_tool()
def bridge_list() -> list | dict:
    """List all registered bridge projects."""
    return _call(bridge.cmd_bridge, bridge_command="list")


@_tool()
def bridge_remove(project_dir: str) -> dict:
    """Remove a bridge project and its AGENTS.md.

    Args:
        project_dir: Path to the project directory.
    """
    return _call(bridge.cmd_bridge, bridge_command="remove", project_dir=project_dir)


# ===========================================================================
# Model management tools
# ===========================================================================


@_tool()
def model_status() -> dict:
    """Show current model configuration and policy."""
    return _call(models.cmd_model, model_command="status")


@_tool()
def model_switch(model_id: str, reason: Optional[str] = None, duration: Optional[str] = None) -> dict:
    """Switch to a different model.

    Args:
        model_id: Model identifier (e.g. "kimi-for-coding/k2p5").
        reason: Reason for switch (captured to memory).
        duration: Temporary switch duration (e.g. "30m", "2h").
    """
    return _call(models.cmd_model, model_command="switch", model_id=model_id, reason=reason, duration=duration)


@_tool()
def model_stats(days: int = 7) -> dict:
    """Show model usage statistics.

    Args:
        days: Lookback period in days (default: 7).
    """
    return _call(models.cmd_model, model_command="stats", days=days)


@_tool()
def model_recommend(
    task_description: Optional[str] = None,
    files: int = 0,
    complexity: str = "medium",
) -> dict:
    """Recommend a model for a given task.

    Args:
        task_description: Description of the task.
        files: Number of files involved (default: 0).
        complexity: Task complexity — low, medium, or high (default: medium).
    """
    return _call(
        models.cmd_model,
        model_command="recommend", task_description=task_description,
        files=files, complexity=complexity,
    )


@_tool()
def model_policy() -> dict:
    """Show the current model selection policy."""
    return _call(models.cmd_model, model_command="policy")


@_tool()
def model_select(task_type: str) -> dict:
    """Select the best model for a task category.

    Args:
        task_type: Task category (e.g. "quick_fix", "architecture", "debugging").
    """
    return _call(models.cmd_model, model_command="select", task_type=task_type)

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
