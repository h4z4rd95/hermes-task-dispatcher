# Packfile — Hermes GitHub Task Dispatcher + Bounded Workspace Manager

Owner prompt: `h4z4rd95/Plan-Project-Situation` →
`hermes/HERMES_GITHUB_TASK_DISPATCHER_AND_BOUNDED_WORKSPACE_PROMPT_2026-10-05.md`

## Environment (verified by the lead before this packfile)

- Host: **Windows 11**, git-bash/MSYS shell. `python3.14` on PATH as `python`,
  `pytest` present. MSYS path translation is OFF → native Windows programs get
  `C:/...` forward-slash paths, never `/c/...`.
- Hermes `v0.21.5+6718` (git install) at
  `C:\Users\h4z4rd\AppData\Local\hermes\hermes-agent`; the `hermes` entrypoint
  is on PATH from its venv `Scripts`. `$HERMES_HOME` =
  `C:\Users\h4z4rd\AppData\Local\hermes`.
- The **gateway cron ticker is running** (heartbeat verified); `hermes cron`
  supports `--no-agent --script` (verified: a script job ran and recorded an
  execution). The dispatcher will register as ONE such job — do not invent a
  daemon.
- Session routing primitive, **both paths verified working**:
  `hermes chat --continue "<name>" --create-if-missing -q "<prompt>" -Q --format text`
  → resumes an existing named session, or creates it. Output ends with
  `session_id: <id>`; `Resumed session` ⇒ existing, `Starting fresh` ⇒ new.
- `GITHUB_TOKEN` lives in `$HERMES_HOME/.env` and authenticates as `h4z4rd95`
  with repo scope (verified: created repo `h4z4rd95/hermes-task-dispatcher`).
- **`/mnt/HC_Volume_107017670` does NOT exist on this host.** It is the Hetzner
  volume. The workspace pool root is therefore **configurable**; on this host it
  defaults to a sibling directory of the dispatcher repo. Do not hardcode it.
- Kanban exists (`hermes kanban ...`) but is profile-spawn based and no board is
  initialised. The prompt asks for a *machine-readable task inbox in the control
  repo* — that is the registry we build. Do not build a second kanban.
- Git identity is set globally (`h4z4rd95`).

## Your lane

Implement **only** the files listed for your lane in `CONTRACT.md` (read it
first — it is the interface contract). Do not create or edit any other file
except `state/` under the repo root and the pytest tmp dir.

Repo root: `C:\Users\h4z4rd\.hermes-tools\hermes-task-dispatcher`

## Non-negotiable rules

1. **Hiddify, DNS, production services, TSGb data, TraderBot runtime, Leantime
   production, and Dokploy are untouchable.** No file in those repos changes,
   no service is deployed, restarted or migrated. Your code must not even have a
   code path that writes to them.
2. **Secrets never enter git or logs.** `GITHUB_TOKEN` is read from the
   environment (the live gateway already has it) — never put it in a file, a
   command-line argument, or a log line.
3. **Fail closed.** Workspace reclaim deletes ONLY metadata-recorded, RECLAIM-
   policy directories, after a dry run. Unknown dirs are reported, never
   deleted.
4. Windows-safe paths everywhere (`pathlib`, no `/tmp` literals, no shell
   string-concatenated paths).

## Test discipline (applies to all lanes)

- `python -m pytest tests -q` from the repo root must pass with no network.
- Mock subprocess calls and the GitHub API. Lane B/C tests must never spawn a
   real agent session or clone a real repo.
- Keep the suite under ~30 seconds.

## What to return

A concise summary: files created, the public API implemented, test results
(`python -m pytest tests -q` output line), any deviation from CONTRACT.md with
the reason, and the exact command the lead should run to exercise your lane.
