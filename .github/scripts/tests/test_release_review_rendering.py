"""Check template boundaries and independent report defaults."""

import unittest

from test_release_review import BASE, HEAD, candidate
from release_review.reporting import render
from release_review.state import new_report


class RenderingTests(unittest.TestCase):
    def test_model_text_remains_data_in_both_templates(self):
        finding = candidate()
        finding["title"] = "{{ 7 * 7 }} {% include 'report.md.jinja' %}"
        report = new_report({"repository": "zakura-core/zakura", "head_sha": HEAD, "base_sha": BASE})
        report["findings"].append(finding)
        for comment in (False, True):
            with self.subTest(comment=comment):
                text = render(report, comment=comment)
                self.assertIn(r"\{\{ 7 \* 7 \}\}", text)
                self.assertEqual(text.count("## Release AI review —"), 1)

    def test_new_reports_do_not_share_mutable_state_or_pr_body(self):
        context = {"head_sha": HEAD, "pr_body": "release metadata"}
        first = new_report(context)
        second = new_report(context)
        first["errors"].append("failed")
        first["coverage"].append({"path": "changed.rs"})
        self.assertEqual(second["errors"], [])
        self.assertEqual(second["coverage"], [])
        self.assertNotIn("pr_body", first)
        self.assertEqual(context["pr_body"], "release metadata")
