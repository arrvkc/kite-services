import ast
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
REPORT_GENERATOR = (
    REPO_ROOT
    / "engines"
    / "strategy_deterministic_engine"
    / "scripts"
    / "generate_transition_html_report.py"
)
ARCHIVE_PUBLISHER = REPO_ROOT / "scripts" / "publish_transition_dashboard_archive.sh"


class TransitionReportFaviconTests(unittest.TestCase):
    def test_report_generator_has_favicon_for_empty_and_populated_reports(self):
        source = REPORT_GENERATOR.read_text()
        ast.parse(source)
        favicon = '<link rel="icon" href="/static/favicon.svg" type="image/svg+xml">'
        self.assertEqual(source.count(favicon), 2)
        self.assertLess(source.index(favicon), source.index("No transitions found."))
        self.assertIn("<head>", source)
        self.assertIn("</head>", source)

    def test_archive_index_has_root_relative_favicon(self):
        source = ARCHIVE_PUBLISHER.read_text()
        favicon = '<link rel="icon" href="/static/favicon.svg" type="image/svg+xml">'
        self.assertEqual(source.count(favicon), 1)
        self.assertLess(source.index(favicon), source.index("<title>"))

    def test_source_evidence_identifies_production_facing_pages(self):
        publisher = ARCHIVE_PUBLISHER.read_text()
        pipeline = (
            REPO_ROOT / "scripts" / "run_transition_dashboard_pipeline.sh"
        ).read_text()
        email_source = (
            REPO_ROOT / "scripts" / "send_transition_dashboard_email.py"
        ).read_text()

        self.assertIn('DEST_DIR="$ATMS_INSTANCE_HOST_PATH/reports/strategy-transition"', publisher)
        self.assertIn('cp "$LATEST_SRC" "$DEST_DIR/latest.html"', publisher)
        self.assertIn('cp "$LATEST_SRC" "$DEST_DIR/${RUN_DATE}.html"', publisher)
        self.assertIn('} > "$DEST_DIR/index.html"', publisher)
        self.assertIn("publish_transition_dashboard_archive.sh", pipeline)
        self.assertIn(
            'PUBLIC_BASE_URL = "https://eajee.in/reports/strategy-transition"',
            email_source,
        )
        self.assertIn('latest_url = f"{PUBLIC_BASE_URL}/latest.html"', email_source)
        self.assertIn('archive_url = f"{PUBLIC_BASE_URL}/"', email_source)


if __name__ == "__main__":
    unittest.main()
