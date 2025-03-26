import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from src.emails import render_email_body, sanitize_html_links, send_smtp_emails
from src.pydantic_models import EmailTable
from src.settings import Settings
from tests import settings


class TestSanitizeHtmlLinks(unittest.TestCase):
    def test_removes_target_blank(self):
        html = '<a href="http://example.com" target="_blank">link</a>'
        self.assertEqual(
            sanitize_html_links(html), '<a href="http://example.com" > URL </a>'
        )


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
            observations_email_recipients=["to@example.com"],
        )

        subject = "Test Subject"
        html = "<html><body>Test</body></html>"
        recipients = ["to@example.com"]

        send_smtp_emails(settings, recipients, subject, html)

        mock_smtp.assert_called_with("smtp.example.com", 587)
        mock_server.set_debuglevel.assert_called_once_with(0)
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("user", "pass")
        mock_server.sendmail.assert_called_once()


class TestEmailTemplates(unittest.TestCase):
    def setUp(self):
        self.settings = settings.model_copy()

    def test_comments_subject_with_error(self):
        result = self.settings.comments_email_subject_template.render(
            date_from=date(2025, 3, 1), date_to=date(2025, 3, 24), error=True
        )
        expected = "Failed to Retrieve Comments from 2025-03-01 to 2025-03-24"
        self.assertEqual(result.strip(), expected)

    def test_comments_subject_without_error(self):
        result = self.settings.comments_email_subject_template.render(
            date_from=date(2025, 3, 1), date_to=date(2025, 3, 24), error=False
        )
        expected = "Flagged Comments from 2025-03-01 to 2025-03-24"
        self.assertEqual(result.strip(), expected)

    def test_observations_subject_with_error(self):
        result = self.settings.observations_email_subject_template.render(
            date_on="2025-03-24", error=True
        )
        expected = (
            "Observations of Invasive Species Submitted on 2025-03-24 — "
            "Error: The website is temporarily unavailable due to maintenance"
        )
        self.assertEqual(result.strip(), expected)

    def test_observations_subject_without_error(self):
        result = self.settings.observations_email_subject_template.render(
            date_on="2025-03-24", error=False
        )
        expected = "Observations of Invasive Species Submitted on 2025-03-24"
        self.assertEqual(result.strip(), expected)

    def test_email_body_with_tables(self):
        html = render_email_body(
            template=self.settings.observations_email_body_template,
            tables=[
                EmailTable(
                    title="Test Table", html="<table><tr><td>Row</td></tr></table>"
                )
            ],
            empty_message="No data",
        )
        self.assertIn("Test Table", html)
        self.assertIn("<table><tr><td>Row</td></tr></table>", html)
        self.assertNotIn("No data", html)

    def test_email_body_without_tables(self):
        html = render_email_body(
            template=self.settings.observations_email_body_template,
            tables=[],
            empty_message="No data",
        )
        self.assertIn("No data", html)
