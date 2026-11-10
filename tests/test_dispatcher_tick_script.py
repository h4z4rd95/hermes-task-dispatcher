from pathlib import Path

SCRIPT = (Path(__file__).resolve().parents[1] / "scripts" / "dispatcher_tick.sh").read_text()


def test_dispatcher_preflight_prefers_dedicated_main_worktree():
    assert 'CONTROL_REPO_PATH:-' in SCRIPT
    assert 'plan-project-situation-main/.git' in SCRIPT
    assert SCRIPT.index('plan-project-situation-main/.git') < SCRIPT.index('plan-project-situation"')


def test_dispatcher_preflight_preserves_legacy_checkout_as_host_fallback():
    assert 'CONTROL_REPO="$DISPATCHER_HOME/plan-project-situation"' in SCRIPT


def test_dispatcher_preflight_fails_closed_when_selected_checkout_is_missing():
    assert 'if [ ! -d "$CONTROL_REPO/.git" ]; then' in SCRIPT
    assert 'dispatcher tick blocked: control repo checkout missing' in SCRIPT
