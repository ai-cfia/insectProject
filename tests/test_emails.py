import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from jinja2 import DictLoader, Environment

from src.emails import (
    build_email_tables,
    make_email_title,
    render_html,
    sanitize_html_links,
    send_email,
    send_smtp_email,
)
from src.settings import Settings
from tests import settings


class TestBuildEmailTables(unittest.TestCase):
    def setUp(self):
        self.settings = settings.model_copy()
        self.settings.upper_taxa_column = "upper taxa"
        self.settings.upper_taxa = ["Insecta", "Plantae"]
        self.settings.other_taxa_label = "Other"
        self.columns = ["upper taxa", "Common Name", "Observation URL"]

    def test_all_empty(self):
        df = pd.DataFrame(columns=self.columns)
        tables = build_email_tables(self.settings, df, df)
        self.assertEqual(tables, [])

    def test_only_global(self):
        df = pd.DataFrame(
            {
                "upper taxa": ["Insecta"],
                "Common Name": ["A"],
                "Observation URL": ["http://a.com"],
            }
        )
        tables = build_email_tables(
            self.settings, df, pd.DataFrame(columns=self.columns)
        )
        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0][0], "iconic_taxa: Insecta")
        self.assertIn("http://a.com", tables[0][1])

    def test_only_us(self):
        df = pd.DataFrame(
            {
                "upper taxa": ["Plantae"],
                "Common Name": ["B"],
                "Observation URL": ["http://b.com"],
            }
        )
        tables = build_email_tables(
            self.settings, pd.DataFrame(columns=self.columns), df
        )
        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0][0], "US iconic_taxa: Plantae")
        self.assertIn("http://b.com", tables[0][1])

    def test_other_category(self):
        df = pd.DataFrame(
            {
                "upper taxa": ["Fungi"],
                "Common Name": ["X"],
                "Observation URL": ["http://x.com"],
            }
        )
        tables = build_email_tables(self.settings, df, df)
        labels = [label for label, _ in tables]
        self.assertIn("iconic_taxa: Other", labels)
        self.assertIn("US iconic_taxa: Other", labels)

    def test_multiple_categories(self):
        df = pd.DataFrame(
            {
                "upper taxa": ["Insecta", "Plantae", "Fungi"],
                "Common Name": ["A", "B", "C"],
                "Observation URL": ["http://a.com", "http://b.com", "http://c.com"],
            }
        )
        tables = build_email_tables(self.settings, df, df)
        labels = [label for label, _ in tables]
        expected = {
            "iconic_taxa: Insecta",
            "iconic_taxa: Plantae",
            "iconic_taxa: Other",
            "US iconic_taxa: Insecta",
            "US iconic_taxa: Plantae",
            "US iconic_taxa: Other",
        }
        self.assertEqual(set(labels), expected)


class TestSanitizeHtmlLinks(unittest.TestCase):
    def test_removes_target_blank(self):
        html = '<a href="http://example.com" target="_blank">link</a>'
        self.assertEqual(
            sanitize_html_links(html), '<a href="http://example.com" > URL </a>'
        )


class TestRenderHTML(unittest.TestCase):
    def setUp(self):
        template_str = """
        <html>
        <body>
            {% for title, table in tables %}
                <h2>{{ title }}</h2>
                {{ table|safe }}
            {% endfor %}
        </body>
        </html>
        """
        self.env = Environment(
            loader=DictLoader({"email_template.html": template_str}), autoescape=True
        )
        self.template_name = "email_template.html"

    def test_render_html_outputs_expected(self):
        tables = [
            (
                "Test Table",
                '<a href="http://example.com" target="_blank" rel="noopener">example</a>',
            )
        ]
        html = render_html(tables, self.env, self.template_name)
        self.assertIn("<h2>Test Table</h2>", html)
        self.assertIn("> URL </a>", html)

    def test_render_html_handles_empty_tables(self):
        tables = []
        html = render_html(tables, self.env, self.template_name)
        self.assertNotIn("<h2>", html)


class TestSendSMTPEmail(unittest.TestCase):
    @patch("src.emails.SMTP")
    def test_send_smtp_email_success(self, mock_smtp):
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server

        settings = Settings(
            smtp_host="smtp.example.com",
            smtp_port=587,
            smtp_debug_level=0,
            smtp_username="user",
            smtp_password="pass",
            sender_name="Sender",
            sender_email="sender@example.com",
            recipient_emails=["to@example.com"],
        )

        html = "<html><body>Test</body></html>"
        subject = "Test Subject"

        send_smtp_email(settings, html, subject)

        mock_smtp.assert_called_with("smtp.example.com", 587)
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("user", "pass")
        mock_server.sendmail.assert_called_once()


class TestSendEmail(unittest.TestCase):
    @patch("src.emails.send_smtp_email")
    def test_send_email_success(self, mock_send_smtp):
        s = settings.model_copy()
        s.recipient_emails = ["to@example.com"]
        s.upper_taxa_column = "upper taxa"
        s.upper_taxa = ["Insecta", "Plantae"]
        s.other_taxa_label = "Other"

        data = {
            "upper taxa": ["Insecta", "Plantae", "Other"],
            "Common Name": ["A", "B", "C"],
            "Observation URL": ["http://a", "http://b", "http://c"],
        }
        df = pd.DataFrame(data)

        send_email(s, df, df, subject="Test Report")

        mock_send_smtp.assert_called_once()


class TestMakeEmailTitle(unittest.TestCase):
    def setUp(self):
        self.settings = settings

    @patch("src.emails.get_yesterday", return_value="2025-03-20")
    def test_normal_title(self, mock_get_yesterday):
        expected = "Observations of Invasive Species Submitted on 2025-03-20"
        result = make_email_title(self.settings, error=False)
        self.assertEqual(result, expected)

    @patch("src.emails.get_yesterday", return_value="2025-03-20")
    def test_maintenance_title(self, mock_get_yesterday):
        expected = (
            "Observations of Invasive Species Submitted on 2025-03-20 — "
            "error: website is temporarily disabled due to maintenance"
        )
        result = make_email_title(self.settings, error=True)
        self.assertEqual(result, expected)
