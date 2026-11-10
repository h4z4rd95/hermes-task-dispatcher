from pathlib import Path
import unittest

SCRIPT = (Path(__file__).resolve().parents[1] / "scripts" / "dispatcher_tick.sh").read_text()


class DispatcherTickScriptTests(unittest.TestCase):
    def test_preflight_prefers_dedicated_main_worktree(self):
        self.assertIn("CONTROL_REPO_PATH:-", SCRIPT)
        self.assertIn('if [ -e "$DISPATCHER_HOME/plan-project-situation-main/.git" ]; then', SCRIPT)
        self.assertLess(
            SCRIPT.index("plan-project-situation-main/.git"),
            SCRIPT.index('CONTROL_REPO="$DISPATCHER_HOME/plan-project-situation"'),
        )

    def test_legacy_checkout_remains_host_fallback(self):
        self.assertIn('CONTROL_REPO="$DISPATCHER_HOME/plan-project-situation"', SCRIPT)

    def test_missing_selected_checkout_fails_closed(self):
        self.assertIn('if [ ! -e "$CONTROL_REPO/.git" ]; then', SCRIPT)
        self.assertIn("dispatcher tick blocked: control repo checkout missing", SCRIPT)


if __name__ == "__main__":
    unittest.main()
