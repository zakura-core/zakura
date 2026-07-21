# perf-lab/tests/test_verdict.py
import json, subprocess, sys, tempfile, unittest
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
    with tempfile.TemporaryDirectory() as d:
        f = Path(d) / "summary.md"; f.write_text(summary_text)
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

    def test_unequal_block_coverage_errors(self):
        # a wall-capped leg covers fewer blocks; comparing the rows is
        # meaningless (seen live: 83k vs 120k produced a bogus 56% delta)
        capped = FIXTURE.replace("| 1737210 | 30000 | 1190s | 25.21 | 27.30 |",
                                 "| 1730000 | 22790 | 2009s | 11.34 | 12.10 |")
        p, _ = run([], capped)
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("different block ranges", p.stderr)

    def test_missing_baseline_row_errors(self):
        p, _ = run([], FIXTURE.replace("(baseline)", "(nope)"))
        self.assertNotEqual(p.returncode, 0)

    def test_banner_rows_ignored(self):
        # the real bench appends 2-column bottleneck-verdict banners below the
        # throughput table; they must not perturb parsing
        with_banners = FIXTURE + """
### Bottleneck verdict (baseline)

| stage | utilization |
|-------|------------:|
| commit | 0.91 |
| download | 0.44 |

### Bottleneck verdict (primary)

| stage | utilization |
|-------|------------:|
| commit | 0.88 |
| download | 0.47 |
"""
        p, out = run(["--threshold-pct", "3.0", "--noise-band-pct", "0.8"], with_banners)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertAlmostEqual(out["baseline_pc_bps"], 26.00)
        self.assertAlmostEqual(out["primary_pc_bps"], 27.30)
        self.assertEqual(out["verdict"], "WIN_CANDIDATE")

    def test_threshold_boundary_is_inclusive(self):
        exact_win = FIXTURE.replace("| 1250s | 24.00 | 26.00 |", "| 1250s | 24.00 | 100.00 |") \
                           .replace("| 1190s | 25.21 | 27.30 |", "| 1190s | 25.21 | 103.00 |")
        _, out = run(["--threshold-pct", "3.0", "--noise-band-pct", "0.0"], exact_win)
        self.assertEqual(out["verdict"], "WIN_CANDIDATE")
        exact_loss = FIXTURE.replace("| 1250s | 24.00 | 26.00 |", "| 1250s | 24.00 | 100.00 |") \
                            .replace("| 1190s | 25.21 | 27.30 |", "| 1190s | 25.21 | 97.00 |")
        _, out = run(["--threshold-pct", "3.0", "--noise-band-pct", "0.0"], exact_loss)
        self.assertEqual(out["verdict"], "LOSS")

if __name__ == "__main__":
    unittest.main()
