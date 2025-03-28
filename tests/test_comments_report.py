import unittest
from datetime import date
from unittest.mock import PropertyMock, patch

import pandas as pd
from jinja2 import Template

from src.comments_report import build_comments_email_tables, send_flagged_comments_email
from src.pydantic_models import EmailTable
from tests import settings


class TestBuildCommentsEmailTables(unittest.TestCase):
    def test_empty_dataframe_returns_empty_list(self):
        df = pd.DataFrame()
        result = build_comments_email_tables(df)
        self.assertEqual(result, [])

    def test_non_empty_dataframe_returns_email_table(self):
        df = pd.DataFrame(
            {
                "user": ["alice"],
                "comment": ["flagged content"],
                "link": ["http://example.com"],
            }
        )
        result = build_comments_email_tables(df)
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], EmailTable)
        self.assertIn("Flagged Comments", result[0].title)
        self.assertIn("<table", result[0].html)
        self.assertIn("http://example.com", result[0].html)


class TestSendFlaggedCommentsEmail(unittest.TestCase):
    def setUp(self):
        self.settings = settings.model_copy()
        self.settings.comments_email_error_message = "Error message"
        self.settings.comments_email_empty_message = "Empty message"
        self.settings.comments_email_recipients = ["test@example.com"]

        # Create test templates
        self.subject_template = Template(
            "Flagged Comments Report ({{ date_from }} to {{ date_to }})",
            autoescape=True,
        )
        self.body_template = Template(
            """
            <html>
                <body>
                    <h2>Flagged Comments Report</h2>
                    <p>Period: {{ date_from }} to {{ date_to }}</p>
                    {% for table in tables %}
                        <h3>{{ table.title }}</h3>
                        {{ table.html }}
                    {% endfor %}
                    {% if error %}
                        <p style="color: red;">{{ error_message }}</p>
                    {% else %}
                        <p>{{ empty_message }}</p>
                    {% endif %}
                </body>
            </html>
        """,
            autoescape=True,
        )

        self.df = pd.DataFrame(
            {
                "user": ["alice"],
                "comment": ["flagged content"],
                "link": ["http://example.com"],
            }
        )
        self.date_from = date(2024, 1, 1)
        self.date_to = date(2024, 1, 7)

    @patch("src.comments_report.send_smtp_emails")
    @patch("src.comments_report.render_email_body")
    @patch("src.comments_report.build_comments_email_tables")
    @patch(
        "src.settings.Settings.comments_email_subject_template",
        new_callable=PropertyMock,
    )
    @patch(
        "src.settings.Settings.comments_email_body_template", new_callable=PropertyMock
    )
    def test_send_flagged_comments_email_with_data(
        self,
        mock_body_template,
        mock_subject_template,
        mock_build_tables,
        mock_render_body,
        mock_send_emails,
    ):
        mock_subject_template.return_value = self.subject_template
        mock_body_template.return_value = self.body_template
        mock_build_tables.return_value = [
            EmailTable(title="Test", html="<table>test</table>")
        ]
        expected_body = "Test Body"
        mock_render_body.return_value = expected_body

        send_flagged_comments_email(
            self.settings, self.df, self.date_from, self.date_to
        )

        mock_build_tables.assert_called_once_with(self.df)

        mock_render_body.assert_called_once_with(
            self.body_template,
            mock_build_tables.return_value,
            self.settings.comments_email_empty_message,
        )

        mock_send_emails.assert_called_once_with(
            self.settings,
            self.settings.comments_email_recipients,
            self.subject_template.render(
                date_from=self.date_from, date_to=self.date_to, error=False
            ),
            expected_body,
        )

    @patch("src.comments_report.send_smtp_emails")
    @patch("src.comments_report.render_email_body")
    @patch("src.comments_report.build_comments_email_tables")
    @patch(
        "src.settings.Settings.comments_email_subject_template",
        new_callable=PropertyMock,
    )
    @patch(
        "src.settings.Settings.comments_email_body_template", new_callable=PropertyMock
    )
    def test_send_flagged_comments_email_with_empty_data(
        self,
        mock_body_template,
        mock_subject_template,
        mock_build_tables,
        mock_render_body,
        mock_send_emails,
    ):
        mock_subject_template.return_value = self.subject_template
        mock_body_template.return_value = self.body_template
        mock_build_tables.return_value = []
        expected_body = "Test Body"
        mock_render_body.return_value = expected_body

        send_flagged_comments_email(
            self.settings, pd.DataFrame(), self.date_from, self.date_to
        )

        assert mock_build_tables.call_count == 1
        assert mock_build_tables.call_args[0][0].empty

        mock_render_body.assert_called_once_with(
            self.body_template,
            [],
            self.settings.comments_email_error_message,
        )

        mock_send_emails.assert_called_once_with(
            self.settings,
            self.settings.comments_email_recipients,
            self.subject_template.render(
                date_from=self.date_from, date_to=self.date_to, error=True
            ),
            expected_body,
        )
