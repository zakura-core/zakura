# perf-lab/tests/test_verdict.py
import json, subprocess, sys, unittest
from pathlib import Path

HERE = Path(__file__).parent
VERDICT = HERE.parent / "verdict.py"

FIXTURE = """## Checkpoint-sync benchmark

- binary source: build `adam/perf-exp/001-ckpt-limit`
- snapshot start height: **1707210**, stop height: **1737210**, feed: `167.99.162.47:8233` (peerset=1)
- sync knobs: checkpoint_verify=1500, download=150
- P2P mode: target p2p_stack=zakura, baseline p2p_stack=zakura

| binary | p2p_stack | end height | blocks covered | time taken | blocks/s | post-commit blk/s |
|--------|----------:|-----------:|---------------:|-----------:|---------:|------------------:|
| main (baseline) | zakura | 1737210 | 30000 | 1250s | 24.00 | 26.00 |
| adam/perf-exp/001-ckpt-limit (primary) | zakura | 1737210 | 30000 | 1190s | 25.21 | 27.30 |

**Speedup:** 24.00 → 25.21 blocks/s = **1.05×**
"""

def run(args, summary_text):
    d = HERE / "_tmp"; d.mkdir(exist_ok=True)
    f = d / "summary.md"; f.write_text(summary_text)
    p = subprocess.run([sys.executable, str(VERDICT), str(f), *args],
                       capture_output=True, text=True)
    return p, (json.loads(p.stdout) if p.returncode == 0 else None)

class TestVerdict(unittest.TestCase):
    def test_parses_gating_metric(self):
        p, out = run(["--threshold-pct", "3.0", "--noise-band-pct", "0.8"], FIXTURE)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertAlmostEqual(out["baseline_pc_bps"], 26.00)
        self.assertAlmostEqual(out["primary_pc_bps"], 27.30)
        self.assertAlmostEqual(out["delta_pct"], 5.0, places=1)

    def test_win_when_above_effective_threshold(self):
        _, out = run(["--threshold-pct", "3.0", "--noise-band-pct", "0.8"], FIXTURE)
        self.assertEqual(out["verdict"], "WIN_CANDIDATE")
        self.assertAlmostEqual(out["effective_threshold_pct"], 3.0)  # max(3.0, 2*0.8)

    def test_noise_band_can_raise_threshold(self):
        _, out = run(["--threshold-pct", "3.0", "--noise-band-pct", "4.0"], FIXTURE)
        self.assertEqual(out["effective_threshold_pct"], 8.0)
        self.assertEqual(out["verdict"], "NEUTRAL")

    def test_loss_detected(self):
        worse = FIXTURE.replace("| 1190s | 25.21 | 27.30 |", "| 1400s | 21.43 | 22.90 |")
        _, out = run(["--threshold-pct", "3.0", "--noise-band-pct", "0.8"], worse)
        self.assertEqual(out["verdict"], "LOSS")

    def test_aa_mode_reports_observed_noise(self):
        _, out = run(["--aa"], FIXTURE)
        self.assertAlmostEqual(out["observed_noise_pct"], 5.0, places=1)

    def test_missing_baseline_row_errors(self):
        p, _ = run([], FIXTURE.replace("(baseline)", "(nope)"))
        self.assertNotEqual(p.returncode, 0)

if __name__ == "__main__":
    unittest.main()
