"""nous-memory CLI entrypoint."""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from .core import EXIT_OK, color, connect_db, ensure_schema, fail, log_verbose, resolve_db_path
from .memory import cmd_capture, cmd_recall, cmd_search, cmd_get, cmd_timeline, cmd_update, cmd_forget
from .tasks import cmd_tasks, cmd_remind
from .entities import cmd_entities
from .kv import cmd_kv
from .sessions import cmd_session
from .stats import cmd_stats
from .bootstrap import cmd_bootstrap
from .episodes import cmd_episode
from .dream import cmd_dream
from .bridge import cmd_bridge
from .patterns import cmd_patterns
from .prompt import cmd_prompt
from .models import cmd_model
from .core import MEMORY_TYPES, TASK_PRIORITIES, ENTITY_TYPES

def build_parser():
    parser = argparse.ArgumentParser(
        prog="nous-memory",
        description="Persistent memory CLI for AI agents.",
    )
    parser.add_argument("--db", type=str, default=None, help="SQLite DB path")
    parser.add_argument("--workspace", type=str, default=None, help="Workspace root for prompt/bridge paths")
    parser.add_argument("--json", action="store_true", help="output JSON")
    parser.add_argument("--verbose", action="store_true", help="debug logging")
    parser.add_argument("--init", action="store_true", help="initialize/migrate schema")

    subparsers = parser.add_subparsers(dest="command")

    capture = subparsers.add_parser("capture", help="store a memory")
    capture.add_argument("--type", required=True, choices=MEMORY_TYPES)
    capture.add_argument("--scope", default="global")
    capture.add_argument("--tags")
    capture.add_argument("--source")
    capture.add_argument("--expires", help="expiry date/time (ISO or relative)")
    capture.add_argument("--topic-key", help="stable topic identifier for in-place updates")
    capture.add_argument("--no-synthesis", action="store_true", help="skip duplicate/similarity detection")
    capture.add_argument("content", nargs="?")
    capture.add_argument("--content", dest="content_flag", help="memory content text")

    recall = subparsers.add_parser("recall", help="retrieve memories")
    recall.add_argument("query", nargs="?")
    recall.add_argument("--type", choices=MEMORY_TYPES)
    recall.add_argument("--scope")
    recall.add_argument("--tags")
    recall.add_argument("--limit", type=int, default=20)
    recall.add_argument("--active", action="store_true")

    recall.add_argument("--semantic", action="store_true",
                        help="use semantic search via daemon API (hybrid vector + keyword)")
    recall.add_argument("--daemon-url", default=None,
                        help="daemon API URL (default: $NOUS_DAEMON_URL or http://localhost:8080)")

    search = subparsers.add_parser("search", help="compact full-text memory search")
    search.add_argument("query")
    search.add_argument("--type", choices=MEMORY_TYPES)
    search.add_argument("--scope")
    search.add_argument("--tags")
    search.add_argument("--active", action="store_true")
    search.add_argument("--limit", type=int, default=20)

    get = subparsers.add_parser("get", help="show full details for one memory")
    get.add_argument("id", type=int)

    timeline = subparsers.add_parser("timeline", help="show +/-24h context around a memory")
    timeline.add_argument("id", type=int)

    update = subparsers.add_parser("update", help="update a memory by superseding")
    update.add_argument("id", type=int)
    update.add_argument("content")

    forget = subparsers.add_parser("forget", help="forget a memory")
    forget.add_argument("id", type=int)
    forget.add_argument("--hard", action="store_true", help="hard delete")

    tasks = subparsers.add_parser("tasks", help="manage tasks")
    tasks.add_argument("--all", action="store_true", help="list all tasks")
    tasks.add_argument("--due", action="store_true", help="list overdue and due within 24h")
    tasks_sub = tasks.add_subparsers(dest="tasks_command")

    tasks_add = tasks_sub.add_parser("add", help="add a task")
    tasks_add.add_argument("--priority", choices=TASK_PRIORITIES, default="medium")
    tasks_add.add_argument("--due", help="due date/time (ISO or relative)")
    tasks_add.add_argument("--description")
    tasks_add.add_argument("--repeat-rule")
    tasks_add.add_argument("--entity-id", type=int)
    tasks_add.add_argument("--entity-name")
    tasks_add.add_argument("--tags")
    tasks_add.add_argument("title")

    tasks_done = tasks_sub.add_parser("done", help="mark task done")
    tasks_done.add_argument("id", type=int)

    tasks_cancel = tasks_sub.add_parser("cancel", help="cancel task")
    tasks_cancel.add_argument("id", type=int)

    remind = subparsers.add_parser("remind", help="create reminder task")
    remind.add_argument("when", help="e.g. tomorrow, in 3 days, next monday, 2026-03-12")
    remind.add_argument("title")

    entities = subparsers.add_parser("entities", help="manage entities")
    entities_sub = entities.add_subparsers(dest="entities_command")

    entities_add = entities_sub.add_parser("add", help="add entity")
    entities_add.add_argument("--type", required=True, choices=ENTITY_TYPES)
    entities_add.add_argument("--metadata")
    entities_add.add_argument("name")

    entities_show = entities_sub.add_parser("show", help="show entity")
    entities_show.add_argument("name")

    entities_update = entities_sub.add_parser("update", help="update entity")
    entities_update.add_argument("name")
    entities_update.add_argument("--type", choices=ENTITY_TYPES)
    entities_update.add_argument("--metadata")

    kv = subparsers.add_parser("kv", help="key-value store")
    kv_sub = kv.add_subparsers(dest="kv_command", required=True)

    kv_get = kv_sub.add_parser("get", help="get key")
    kv_get.add_argument("key")

    kv_set = kv_sub.add_parser("set", help="set key")
    kv_set.add_argument("key")
    kv_set.add_argument("value")

    kv_list = kv_sub.add_parser("list", help="list keys")

    kv_delete = kv_sub.add_parser("delete", help="delete key")
    kv_delete.add_argument("key")

    session = subparsers.add_parser("session", help="manage session references")
    session_sub = session.add_subparsers(dest="session_command", required=True)

    session_log = session_sub.add_parser("log", help="log session ref")
    session_log.add_argument("--id", required=True)
    session_log.add_argument("--summary")
    session_log.add_argument("--files", help="JSON array")
    session_log.add_argument("--decisions", help="JSON array")

    session_list = session_sub.add_parser("list", help="list sessions")
    session_list.add_argument("--limit", type=int, default=20)

    session_show = session_sub.add_parser("show", help="show session")
    session_show.add_argument("session_id")

    episode = subparsers.add_parser("episode", help="manage episodes")
    episode_sub = episode.add_subparsers(dest="episode_command", required=True)

    episode_start = episode_sub.add_parser("start", help="start an episode")
    episode_start.add_argument("--scope", required=True)
    episode_start.add_argument("--intent", help="episode intent")

    episode_end = episode_sub.add_parser("end", help="end current active episode")
    episode_end.add_argument("--scope", help="scope to end")
    episode_end.add_argument("--summary", help="episode summary")

    episode_list = episode_sub.add_parser("list", help="list episodes")
    episode_list.add_argument("--scope", help="scope filter")
    episode_list.add_argument("--limit", type=int, default=20)

    episode_current = episode_sub.add_parser("current", help="show active episode")
    episode_current.add_argument("--scope", help="scope filter")

    subparsers.add_parser("stats", help="show memory statistics")

    bootstrap = subparsers.add_parser("bootstrap", help="token-budgeted session context")
    bootstrap.add_argument("--scope", help="project scope filter")
    bootstrap.add_argument("--budget", type=int, default=4000, help="max chars (default: 4000)")
    bootstrap.add_argument("--recent-limit", type=int, default=5, help="max recent memories (default: 5)")
    bootstrap.add_argument("--handoff", action="store_true", help="include tasks, context, and patterns")



    dream = subparsers.add_parser("dream", help="background dreaming: idle-time memory re-evaluation")
    dream.add_argument("--scope", help="scope filter")
    dream.add_argument("--stale-days", type=int, default=30, help="stale decision threshold (default: 30)")
    dream.add_argument("--json", dest="json_output", action="store_true", help="JSON output")

    prompt = subparsers.add_parser("prompt", help="manage agent prompt (AGENTS.md)")
    prompt_sub = prompt.add_subparsers(dest="prompt_command")

    prompt_sub.add_parser("show", help="display active prompt")

    prompt_snap = prompt_sub.add_parser("snapshot", help="snapshot current prompt")
    prompt_snap.add_argument("--message", "-m", help="snapshot message")

    prompt_propose = prompt_sub.add_parser("propose", help="create a prompt change proposal")
    prompt_propose.add_argument("description", help="brief description of the change")
    prompt_propose.add_argument("--file", help="source file with proposed content")

    prompt_proposals = prompt_sub.add_parser("proposals", help="list proposals")
    prompt_proposals.add_argument("--all", action="store_true", help="include applied/rejected")

    prompt_apply = prompt_sub.add_parser("apply", help="apply a proposal to AGENTS.md")
    prompt_apply.add_argument("proposal_id", type=int, help="proposal ID to apply")

    prompt_reject = prompt_sub.add_parser("reject", help="reject a proposal")
    prompt_reject.add_argument("proposal_id", type=int, help="proposal ID to reject")
    prompt_reject.add_argument("--reason", help="rejection reason")

    prompt_sub.add_parser("history", help="show version history")

    prompt_diff = prompt_sub.add_parser("diff", help="diff versions or proposals")
    prompt_diff.add_argument("--proposal", type=int, help="diff proposal against active")
    prompt_diff.add_argument("--from", dest="v_from", help="version to diff from")
    prompt_diff.add_argument("--to", dest="v_to", help="version to diff to")

    bridge = subparsers.add_parser('bridge', help='manage bridge AGENTS.md files')
    bridge_sub = bridge.add_subparsers(dest='bridge_command', required=True)

    bridge_gen = bridge_sub.add_parser('generate', help='generate bridge AGENTS.md for a project')
    bridge_gen.add_argument('project_dir')
    bridge_gen.add_argument('--force', action='store_true', help='overwrite existing AGENTS.md')

    bridge_sub.add_parser('sync', help='regenerate all registered bridges')
    bridge_sub.add_parser('list', help='list bridge projects')

    bridge_remove = bridge_sub.add_parser('remove', help='remove a bridge project')
    bridge_remove.add_argument('project_dir')


    patterns = subparsers.add_parser('patterns', help='detect recurring patterns and suggest improvements')
    patterns_sub = patterns.add_subparsers(dest='patterns_command', required=True)

    patterns_analyze = patterns_sub.add_parser('analyze', help='find recurring tag clusters')
    patterns_analyze.add_argument('--threshold', type=int, default=3, help='min occurrences (default: 3)')
    patterns_analyze.add_argument('--include-all', action='store_true', help='include all memory types, not just failure/pattern')

    patterns_suggest = patterns_sub.add_parser('suggest', help='suggest prompt improvements from patterns')
    patterns_suggest.add_argument('--threshold', type=int, default=3, help='min occurrences (default: 3)')

    patterns_propose = patterns_sub.add_parser('propose', help='auto-generate prompt proposal from a tag')
    patterns_propose.add_argument('tag', help='tag to generate proposal from')
    patterns_propose.add_argument('--threshold', type=int, default=3, help='min occurrences (default: 3)')

    patterns_sub.add_parser('sync', help='sync failure/pattern memories to knowledge files')
    # Model management subparser
    model = subparsers.add_parser('model', help='manage AI model selection and policy')
    model_sub = model.add_subparsers(dest='model_command', required=True)
    
    model_sub.add_parser('status', help='show current model configuration')
    
    model_switch = model_sub.add_parser('switch', help='switch to a different model')
    model_switch.add_argument('model_id', help='model identifier (e.g., kimi-for-coding/k2p5)')
    model_switch.add_argument('--reason', help='reason for switch (captured to memory)')
    model_switch.add_argument('--duration', help='temporary switch duration (e.g., 30m, 2h)')
    
    model_stats = model_sub.add_parser('stats', help='show model usage statistics')
    model_stats.add_argument('--days', type=int, default=7, help='lookback period (default: 7)')
    
    model_recommend = model_sub.add_parser('recommend', help='recommend model for current task')
    model_recommend.add_argument('task_description', nargs='?', help='description of task')
    model_recommend.add_argument('--files', type=int, default=0, help='number of files involved')
    model_recommend.add_argument('--complexity', choices=['low', 'medium', 'high'], default='medium')
    
    model_sub.add_parser('policy', help='show model policy')


    model_select = model_sub.add_parser('select', help='select model for a task category')
    model_select.add_argument('task_type', help='task category (e.g., quick_fix, architecture, debugging)')
    return parser


def run(args):
    with connect_db(args.db) as conn:
        ensure_schema(conn, args.db)
        log_verbose(args, f"using db: {args.db}")
        if args.init and args.command is None:
            if args.json:
                print(json.dumps({"initialized": True, "db": str(args.db)}, indent=2))
            else:
                print(color(f"Initialized schema in {args.db}", "32"))
            return EXIT_OK

        if args.command == "capture":
            if args.content is None:
                args.content = args.content_flag
            if args.content is None:
                return fail("capture requires content (positional or --content)")
            return cmd_capture(args, conn)
        if args.command == "recall":
            return cmd_recall(args, conn)
        if args.command == "search":
            return cmd_search(args, conn)
        if args.command == "get":
            return cmd_get(args, conn)
        if args.command == "timeline":
            return cmd_timeline(args, conn)
        if args.command == "update":
            return cmd_update(args, conn)
        if args.command == "forget":
            return cmd_forget(args, conn)
        if args.command == "tasks":
            return cmd_tasks(args, conn)
        if args.command == "remind":
            return cmd_remind(args, conn)
        if args.command == "entities":
            return cmd_entities(args, conn)
        if args.command == "kv":
            return cmd_kv(args, conn)
        if args.command == "session":
            return cmd_session(args, conn)
        if args.command == "stats":
            return cmd_stats(args, conn)
        if args.command == "bootstrap":
            return cmd_bootstrap(args, conn)

        if args.command == "episode":
            return cmd_episode(args, conn)

        if args.command == "dream":
            return cmd_dream(args, conn)
        if args.command == 'bridge':
            return cmd_bridge(args, conn)


        if args.command == 'patterns':
            return cmd_patterns(args, conn)

        if args.command == "prompt":
            return cmd_prompt(args, conn)

        if args.command == "model":
            return cmd_model(args, conn)
    return fail("no command given")


def main(argv=None):
    parser = build_parser()
    # Allow global flags (--json, --verbose, --db) in any position
    raw = argv if argv is not None else sys.argv[1:]
    global_flags = ('--json', '--verbose', '--init')
    db_flag = False
    reordered = []
    trailing = []
    for arg in raw:
        if arg in global_flags:
            reordered.append(arg)
        elif arg in ('--db', '--workspace'):
            db_flag = True
            reordered.append(arg)
        elif db_flag:
            reordered.append(arg)
            db_flag = False
        else:
            trailing.append(arg)
    args = parser.parse_args(reordered + trailing)
    args.db = resolve_db_path(args.db)
    if args.command is None and not args.init:
        parser.print_help()
        return EXIT_OK
    try:
        return run(args)
    except KeyboardInterrupt:
        return fail("interrupted")
    except sqlite3.Error as exc:
        return fail(f"sqlite error: {exc}")


if __name__ == "__main__":
    raise SystemExit(main())
