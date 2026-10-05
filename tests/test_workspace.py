"""Tests for dispatcher/workspace.py (Lane C).

No network, no real git clone. The git subprocess is mocked by replacing
``WorkspaceManager._git`` with a fake that writes real files into a real temp
pool directory (so filesystem behaviour, metadata and reclaim are genuinely
exercised), and the GitHub repo-size API by replacing ``repo_size_bytes``.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from dispatcher.workspace import (
    POLICY_KEEP,
    POLICY_RECLAIM,
    Checkout,
    WorkspaceError,
    WorkspaceManager,
    WorkspaceQuotaError,
    WorkspaceSpaceError,
    default_pool_root,
)

REPO = "h4z4rd95/name"
BRANCH = "main"


def _fail_if_called(*args, **kwargs):
    raise AssertionError("forbidden real call: %r" % (args,))


class CompletedStub:
    """Minimal stand-in for subprocess.CompletedProcess."""

    stdout = ""
    stderr = ""


class FakeGit:
    """Records git invocations and materialises a fake checkout on clone/sync."""

    def __init__(self, fail_on=()):
        self.calls: list[tuple[tuple[str, ...], str]] = []
        self.fail_on = tuple(fail_on or ())
        self.clone_count = 0

    def install(self, manager: WorkspaceManager) -> WorkspaceManager:
        self_manager = self

        def _git(args, *, cwd):
            self_manager.calls.append((tuple(args), str(cwd)))
            joined = " ".join(args)
            for token in self_manager.fail_on:
                if token in joined:
                    raise WorkspaceError(f"git {joined} failed (simulated)")
            if args[0] == "clone":
                self_manager.clone_count += 1
                path = Path(args[-1])
                path.mkdir(parents=True, exist_ok=True)
                (path / ".git").mkdir(exist_ok=True)
                (path / ".git" / "HEAD").write_text("ref: refs/heads/" + BRANCH)
                (path / "README.md").write_text(
                    f"# {REPO}@{args[args.index('--branch') + 1]}\n"
                )
            elif args[0] == "fetch":
                (Path(cwd) / "fetched.txt").write_text("fetched\n")
            elif args[0] == "reset":
                (Path(cwd) / "README.md").write_text(
                    f"# {REPO}@{BRANCH} (reset)\n"
                )
            return CompletedStub()

        manager._git = _git  # type: ignore[method-assign]
        manager._fake_git = self  # type: ignore[attr-defined]
        return manager


@pytest.fixture
def pool(tmp_path: Path) -> Path:
    root = tmp_path / "pool"
    root.mkdir()
    return root


@pytest.fixture
def manager(pool: Path):
    mgr = WorkspaceManager(pool, global_quota_mb=1000)
    fake = FakeGit()
    fake.install(mgr)
    mgr.repo_size_bytes = lambda repo: 10 * 1024 * 1024  # 10 MiB, mocked API
    return mgr


@pytest.fixture
def no_env_pool(monkeypatch):
    monkeypatch.delenv("DISPATCHER_WORKSPACE_POOL", raising=False)

# ------------------------------------------------------------------ metadata

class TestMetadataStore:
    def test_records_every_field(self, manager: WorkspaceManager, pool: Path):
        out = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH,
                               purpose="build", keep=True)
        assert out.created is True
        assert out.path.is_dir()
        assert (out.path / ".git").is_dir()

        meta = json.loads((pool / ".dispatcher" / "workspaces.json").read_text())
        assert meta["version"] == 1
        rec = meta["records"]
        assert len(rec) == 1
        keys = set(rec[0])
        expected = {"path", "task_id", "repo", "branch", "purpose",
                    "size_bytes", "last_use", "created_at", "policy", "session_id"}
        assert expected <= keys
        assert rec[0]["task_id"] == "T-001"
        assert rec[0]["repo"] == REPO
        assert rec[0]["branch"] == BRANCH
        assert rec[0]["purpose"] == "build"
        assert rec[0]["policy"] == POLICY_KEEP
        assert rec[0]["size_bytes"] > 0
        assert isinstance(rec[0]["last_use"], float)
        assert isinstance(rec[0]["created_at"], float)
        assert rec[0]["session_id"] == ""

    def test_store_is_written_atomically(self, manager: WorkspaceManager, pool: Path):
        manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        # A leftover .tmp must never be visible after a successful write.
        assert not (pool / ".dispatcher" / "workspaces.json.tmp").exists()

    def test_persistence_across_instances(self, manager: WorkspaceManager, pool: Path):
        manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        again = WorkspaceManager(pool, global_quota_mb=1000)
        assert len(again.report()) == 1
        assert again.report()[0]["task_id"] == "T-001"

    def test_corrupt_metadata_starts_empty(self, manager: WorkspaceManager, pool: Path):
        manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        (pool / ".dispatcher" / "workspaces.json").write_text("{not json")
        again = WorkspaceManager(pool, global_quota_mb=1000)
        assert again.report() == []
        # The checkout becomes unmanaged -> reported, and protected from delete.
        assert len(again.unmanaged_dirs()) == 1


# ------------------------------------------------------------- reuse / clone

class TestReuse:
    def test_idempotent_reuse_marks_not_created(self, manager: WorkspaceManager):
        first = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        marker = first.path / "work.txt"
        marker.write_text("local edits")

        second = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)

        assert second.created is False
        assert second.path == first.path
        assert manager._fake_git.clone_count == 1  # type: ignore[attr-defined]
        assert marker.exists()  # local file preserved (only reset --hard runs)

    def test_fetch_and_reset_run_on_reuse(self, manager: WorkspaceManager):
        first = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        # Only the clone may have run so far.
        assert [a[0] for a, _ in manager._fake_git.calls] == ["clone"]  # type: ignore[attr-defined]

        second = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        assert second.created is False
        args = [" ".join(a) for a, _ in manager._fake_git.calls]  # type: ignore[attr-defined]
        assert " ".join(manager._fake_git.calls[0][0]).startswith("clone")  # type: ignore[attr-defined]
        assert any("fetch" in a for a in args)
        assert any("checkout " + BRANCH in a for a in args)
        assert any(f"reset --hard origin/{BRANCH}" in a for a in args)
        assert (first.path / "fetched.txt").exists()

    def test_different_task_gets_own_checkout(self, manager: WorkspaceManager):
        a = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        b = manager.checkout(task_id="T-002", repo=REPO, branch=BRANCH)
        assert a.path != b.path
        assert a.path.is_dir() and b.path.is_dir()
        assert len(manager.report()) == 2

    def test_stale_record_is_recloned(self, manager: WorkspaceManager):
        first = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        shutil.rmtree(first.path)  # disk gone, metadata still present
        manager.repo_size_bytes = lambda repo: 10 * 1024 * 1024
        second = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        assert second.created is True
        assert second.path == first.path
        assert len(manager.report()) == 1

    def test_refuses_to_clobber_unmanaged_dir(self, manager: WorkspaceManager, pool: Path):
        # A directory sitting on the path the checkout *would* take.
        collide = manager._path_for("stray", REPO, BRANCH)
        collide.mkdir(parents=True)
        (collide / "important.txt").write_text("nope")

        with pytest.raises(WorkspaceError, match="unmanaged"):
            manager.checkout(task_id="stray", repo=REPO, branch=BRANCH)
        assert (collide / "important.txt").exists()
        assert manager._fake_git.clone_count == 0  # type: ignore[attr-defined]
        assert manager.report() == []

    def test_unrelated_dirs_are_reported_not_blocked(
        self, manager: WorkspaceManager, pool: Path
    ):
        stray = pool / "stray__dir"
        stray.mkdir()
        out = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        assert out.created is True
        assert stray in manager.unmanaged_dirs()


# --------------------------------------------------------------- quota gates

class TestQuota:
    def test_global_quota_exceeded(self, manager: WorkspaceManager):
        manager.global_quota_mb = 1
        with pytest.raises(WorkspaceQuotaError, match="global"):
            manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        assert manager._fake_git.clone_count == 0  # type: ignore[attr-defined]
        assert manager.report() == []

    def test_task_budget_exceeded(self, manager: WorkspaceManager):
        with pytest.raises(WorkspaceQuotaError, match="task budget"):
            manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH,
                             task_budget_mb=1)
        assert manager._fake_git.clone_count == 0  # type: ignore[attr-defined]

    def test_unknown_repo_size_fails_closed(self, manager: WorkspaceManager):
        manager.repo_size_bytes = lambda repo: None
        with pytest.raises(WorkspaceQuotaError, match="cannot verify"):
            manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        assert manager._fake_git.clone_count == 0  # type: ignore[attr-defined]

    def test_insufficient_pool_space(self, manager: WorkspaceManager, pool: Path):
        manager.pool_free_bytes = lambda: 1024  # 1 KiB free
        with pytest.raises(WorkspaceSpaceError, match="free"):
            manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        assert manager._fake_git.clone_count == 0  # type: ignore[attr-defined]

    def test_quota_checked_before_clone_order(self, manager: WorkspaceManager):
        """Fail closed: no git invocation may precede the quota decision."""
        manager.global_quota_mb = 1
        try:
            manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        except WorkspaceQuotaError:
            pass
        assert manager._fake_git.calls == []  # type: ignore[attr-defined]

    def test_budget_zero_disables_task_check(self, manager: WorkspaceManager):
        out = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH,
                               task_budget_mb=0)
        assert out.created is True

    def test_repo_size_uses_github_api_kb(self, monkeypatch):
        seen: dict[str, object] = {}

        class Resp:
            def __init__(self, payload):
                self._payload = payload

            def read(self):
                return json.dumps(self._payload)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        def fake_urlopen(request, timeout=None):
            seen["url"] = request.get_full_url()
            seen["headers"] = dict(request.headers)
            seen["timeout"] = timeout
            return Resp({"size": 2048})  # 2048 KiB -> 2 MiB

        monkeypatch.setattr(
            "dispatcher.workspace.urllib.request.urlopen", fake_urlopen
        )
        mgr = WorkspaceManager(Path("unused-pool"), global_quota_mb=1000,
                               github_token="ghp_secret")
        assert mgr.repo_size_bytes(REPO) == 2048 * 1024
        assert seen["url"] == f"https://api.github.com/repos/{REPO}"
        assert "Bearer ghp_secret" == seen["headers"].get("Authorization")  # type: ignore[union-attr]
        assert seen["timeout"] == 15
        # Token rides in a header, never in the URL.
        assert "ghp_secret" not in str(seen["url"])

    def test_repo_size_unknown_on_api_failure(self, pool: Path, monkeypatch):
        def fake_urlopen(request, timeout=None):
            raise OSError("network down")

        monkeypatch.setattr(
            "dispatcher.workspace.urllib.request.urlopen", fake_urlopen
        )
        mgr = WorkspaceManager(pool, global_quota_mb=1000, github_token="ghp_x")
        assert mgr.repo_size_bytes(REPO) is None

    def test_repo_size_rejects_malformed_repo(self, pool: Path):
        mgr = WorkspaceManager(pool, global_quota_mb=1000, github_token="ghp_x")
        assert mgr.repo_size_bytes("not-a-repo") is None


# ------------------------------------------------------------ mutating API

class TestMutators:
    def test_release_and_keep_flip_policy(self, manager: WorkspaceManager):
        path = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH).path
        assert manager.report()[0]["policy"] == POLICY_RECLAIM

        manager.keep(path)
        assert manager.report()[0]["policy"] == POLICY_KEEP

        manager.release(path)
        assert manager.report()[0]["policy"] == POLICY_RECLAIM

    def test_bind_session_records_and_touches(self, manager: WorkspaceManager):
        path = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH).path
        before = manager.report()[0]["last_use"]
        manager.bind_session(path, "sess-abc")
        row = manager.report()[0]
        assert row["session_id"] == "sess-abc"
        assert row["last_use"] >= before

    def test_touch_updates_last_use(self, manager: WorkspaceManager):
        path = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH).path
        before = manager.report()[0]["last_use"]
        manager.touch(path)
        assert manager.report()[0]["last_use"] >= before

    def test_note_size_persists(self, manager: WorkspaceManager):
        path = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH).path
        manager.note_size(path, 12345)
        assert manager.report()[0]["size_bytes"] == 12345
        assert manager.pool_used_bytes() == 12345

    def test_mutators_reject_unknown_paths(self, manager: WorkspaceManager, tmp_path: Path):
        for fn in (manager.release, manager.keep, manager.touch):
            with pytest.raises(WorkspaceError, match="not a managed"):
                fn(tmp_path / "nope")
        with pytest.raises(WorkspaceError, match="not a managed"):
            manager.bind_session(tmp_path / "nope", "s")
        with pytest.raises(WorkspaceError, match="not a managed"):
            manager.note_size(tmp_path / "nope", 1)


# ------------------------------------------------------------------- reclaim

class TestReclaim:
    def test_dry_run_default_deletes_nothing(self, manager: WorkspaceManager):
        path = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH).path
        result = manager.reclaim()
        assert result["dry_run"] is True
        assert result["count"] == 1
        assert str(path) in result["would_delete"]
        assert result["freed_bytes"] > 0
        assert path.is_dir()
        assert len(manager.report()) == 1

    def test_reclaim_deletes_only_released(self, manager: WorkspaceManager):
        released = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH).path
        kept = manager.checkout(task_id="T-002", repo=REPO, branch=BRANCH).path
        manager.keep(kept)

        result = manager.reclaim(dry_run=False)
        assert result["dry_run"] is False
        assert result["count"] == 1
        assert str(released) in result["deleted"]
        assert not released.exists()
        assert kept.is_dir()
        assert len(manager.report()) == 1

    def test_reclaim_never_touches_unmanaged(self, manager: WorkspaceManager, pool: Path):
        stray = pool / "unmanaged-project"
        stray.mkdir()
        (stray / "data.txt").write_text("important")

        path = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH).path
        result = manager.reclaim(dry_run=False)
        assert result["count"] == 1
        assert str(stray) not in result["deleted"]
        assert (stray / "data.txt").exists()
        assert not path.exists()

    def test_task_id_filter(self, manager: WorkspaceManager):
        a = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH).path
        b = manager.checkout(task_id="T-002", repo=REPO, branch=BRANCH).path

        result = manager.reclaim(dry_run=False, task_ids=["T-002"])
        assert result["count"] == 1
        assert a.is_dir()
        assert not b.exists()

    def test_keep_policy_survives_reclaim(self, manager: WorkspaceManager):
        path = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH).path
        manager.keep(path)
        assert manager.reclaim(dry_run=False)["count"] == 0
        assert path.is_dir()

    def test_dry_run_reports_kept(self, manager: WorkspaceManager):
        kept = manager.checkout(task_id="T-002", repo=REPO, branch=BRANCH).path
        manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        manager.keep(kept)
        result = manager.reclaim()
        assert str(kept) in result["kept"]
        assert result["count"] == 1

    def test_reclaim_drops_gone_records(self, manager: WorkspaceManager):
        path = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH).path
        shutil.rmtree(path)
        result = manager.reclaim(dry_run=False)
        assert str(path) in result["missing"]
        assert manager.report() == []

    def test_recorded_path_outside_pool_never_deleted(self, manager: WorkspaceManager,
                                                      tmp_path: Path):
        elsewhere = tmp_path / "outside"
        elsewhere.mkdir()
        manager._ensure()
        manager._records.append({
            "path": str(elsewhere), "task_id": "T-x", "repo": REPO,
            "branch": BRANCH, "purpose": "", "size_bytes": 1,
            "last_use": 0.0, "created_at": 0.0, "policy": POLICY_RECLAIM,
            "session_id": "",
        })
        manager._save()
        result = manager.reclaim(dry_run=False)
        assert result["count"] == 0
        assert elsewhere.is_dir()


# ------------------------------------------------------------- introspection

class TestIntrospection:
    def test_unmanaged_dirs_lists_and_excludes_metadata(self, manager: WorkspaceManager,
                                                        pool: Path):
        path = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH).path
        stray = pool / "stray-repo"
        stray.mkdir()

        found = manager.unmanaged_dirs()
        assert stray in found
        assert path not in found
        assert pool / ".dispatcher" not in found

    def test_report_orders_by_last_use_desc(self, manager: WorkspaceManager):
        manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        manager.checkout(task_id="T-002", repo=REPO, branch=BRANCH)
        manager.checkout(task_id="T-003", repo=REPO, branch=BRANCH)
        manager.touch(manager._path_for("T-001", REPO, BRANCH))
        rows = manager.report()
        assert [r["task_id"] for r in rows] == ["T-001", "T-003", "T-002"]

    def test_report_rows_are_copies(self, manager: WorkspaceManager):
        manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        manager.report()[0]["task_id"] = "mutated"
        assert manager.report()[0]["task_id"] == "T-001"

    def test_pool_free_bytes_sane(self, manager: WorkspaceManager):
        free = manager.pool_free_bytes()
        assert isinstance(free, int) and free >= 0

    def test_pool_used_bytes(self, manager: WorkspaceManager):
        manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        assert manager.pool_used_bytes() > 0


# --------------------------------------------------------- validation / root

class TestValidationAndRoot:
    def test_invalid_repo_rejected(self, manager: WorkspaceManager):
        with pytest.raises(WorkspaceError, match="invalid repo"):
            manager.checkout(task_id="T-001", repo="../../etc", branch=BRANCH)

    def test_invalid_branch_rejected(self, manager: WorkspaceManager):
        with pytest.raises(WorkspaceError, match="invalid branch"):
            manager.checkout(task_id="T-001", repo=REPO, branch="main; rm -rf /")

    def test_invalid_task_id_rejected(self, manager: WorkspaceManager):
        with pytest.raises(WorkspaceError, match="invalid task_id"):
            manager.checkout(task_id="../escape", repo=REPO, branch=BRANCH)

    def test_checkout_paths_stay_inside_pool(self, manager: WorkspaceManager, pool: Path):
        # task_id is validated as a single safe path component, so a traversal
        # attempt is rejected outright rather than escaped.
        with pytest.raises(WorkspaceError, match="invalid task_id"):
            manager.checkout(task_id="T-001/..", repo=REPO, branch=BRANCH)

        out = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        assert pool.resolve() in out.path.resolve().parents

    def test_default_pool_root_env_override(self, monkeypatch, tmp_path: Path):
        wanted = tmp_path / "custom-pool"
        monkeypatch.setenv("DISPATCHER_WORKSPACE_POOL", str(wanted))
        assert default_pool_root() == wanted

    def test_default_pool_root_sibling_fallback(self, monkeypatch, no_env_pool,
                                                tmp_path: Path):
        monkeypatch.setattr("dispatcher.workspace.Path.exists", lambda self: False)
        monkeypatch.setattr(
            "dispatcher.workspace.Path.is_dir", lambda self: str(self) == "keep"
        )
        root = default_pool_root(tmp_path / "dispatcher-home")
        assert root == tmp_path / "dispatcher-workspaces"

    def test_checkout_dataclass_shape(self):
        c = Checkout(path=Path("x"), created=False, size_bytes=5)
        assert c.path == Path("x")
        assert c.created is False
        assert c.size_bytes == 5


# ------------------------------------------------------------------ plumbing

class TestGitPlumbing:
    @staticmethod
    def _real(pool: Path) -> WorkspaceManager:
        """A manager with the *real* git plumbing intact (network mocked)."""
        mgr = WorkspaceManager(pool, global_quota_mb=1000, github_token="ghp_x")
        mgr.repo_size_bytes = lambda repo: 10 * 1024 * 1024
        return mgr

    def test_git_failure_becomes_workspace_error(self, pool: Path, monkeypatch):
        def boom(*args, **kwargs):
            raise subprocess.CalledProcessError(
                128, ["git", "clone"], stderr="fatal: repository not found"
            )

        monkeypatch.setattr("dispatcher.workspace.subprocess.run", boom)
        with pytest.raises(WorkspaceError, match="failed"):
            self._real(pool).checkout(task_id="T-001", repo=REPO, branch=BRANCH)

    def test_git_timeout_becomes_workspace_error(self, pool: Path, monkeypatch):
        def boom(*args, **kwargs):
            raise subprocess.TimeoutExpired(cmd="git", timeout=1)

        monkeypatch.setattr("dispatcher.workspace.subprocess.run", boom)
        with pytest.raises(WorkspaceError, match="timed out"):
            self._real(pool).checkout(task_id="T-001", repo=REPO, branch=BRANCH)

    def test_git_missing_binary(self, pool: Path, monkeypatch):
        def boom(*args, **kwargs):
            raise OSError("no git")

        monkeypatch.setattr("dispatcher.workspace.subprocess.run", boom)
        with pytest.raises(WorkspaceError, match="failed"):
            self._real(pool).checkout(task_id="T-001", repo=REPO, branch=BRANCH)

    def test_shallow_clone_uses_depth(self, manager: WorkspaceManager):
        manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH,
                         shallow=True, depth=2)
        args = [" ".join(a) for a, _ in manager._fake_git.calls]  # type: ignore[attr-defined]
        assert any("clone" in a and "--depth 2" in a for a in args)

    def test_non_shallow_clone(self, manager: WorkspaceManager):
        manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH, shallow=False)
        args = [" ".join(a) for a, _ in manager._fake_git.calls]  # type: ignore[attr-defined]
        assert any("clone" in a and "--depth" not in a for a in args)

    def test_url_is_https_and_token_free(self, manager: WorkspaceManager):
        manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        clone = [a for a, _ in manager._fake_git.calls if a[0] == "clone"][0]  # type: ignore[attr-defined]
        joined = " ".join(clone)
        assert "https://github.com/h4z4rd95/name.git" in joined
        # No credential material may ever appear on a git command line.
        assert "ghp_" not in joined
        assert "token" not in joined.split("https://")[0]


# --------------------------------------------------------------- smoke / git

class TestNoNetwork:
    """Guards the test-discipline rule: no real clone must ever happen here."""

    def test_git_is_faked_in_suite(self, manager: WorkspaceManager):
        # The fixture has replaced _git; a checkout must not call the real
        # subprocess-backed implementation, and must not spawn any process.
        import dispatcher.workspace as ws

        real_run = ws.subprocess.run
        try:
            ws.subprocess.run = _fail_if_called  # type: ignore[method-assign]
            out = manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
        finally:
            ws.subprocess.run = real_run  # type: ignore[method-assign]
        assert out.created is True
        assert manager._fake_git.clone_count == 1  # type: ignore[attr-defined]

    def test_no_real_network_calls(self, manager: WorkspaceManager, monkeypatch):
        monkeypatch.setattr(
            "dispatcher.workspace.urllib.request.urlopen", _fail_if_called
        )
        # repo_size_bytes is mocked by the fixture, so this checkout must not
        # touch the network either.
        manager.checkout(task_id="T-001", repo=REPO, branch=BRANCH)
