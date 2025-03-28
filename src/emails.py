import email.utils
import logging
import re
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP

import pandas as pd
from jinja2 import Template
from pydantic import EmailStr, validate_call
from tqdm import tqdm

from src.custom_logging import log_call
from src.pydantic_models import EmailTable
from src.settings import Settings

log = logging.getLogger(__name__)


@validate_call
def sanitize_html_links(html: str):
    """Replace HTML link targets with simple URL text"""
    return re.sub('target="_blank".+?>', "> URL </a>", html)


@log_call
@validate_call(config=dict(arbitrary_types_allowed=True))
def render_email_body(template: Template, tables: list[EmailTable], empty_message: str):
    """Render HTML email body from template and tables"""
    html = template.render(tables=tables, empty_message=empty_message)
    html = sanitize_html_links(html)
    return html


@log_call
@validate_call
def send_smtp_emails(s: Settings, recipients: list[EmailStr], subject: str, body: str):
    """Send HTML emails via SMTP to multiple recipients"""
    with SMTP(s.smtp_host, s.smtp_port) as server:
        server.set_debuglevel(s.smtp_debug_level)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(s.smtp_username, s.smtp_password.get_secret_value())
        for recipient in tqdm(recipients, desc="Sending emails"):
            log.info(f"Sending email to {recipient}")
            message = MIMEMultipart("alternative")
            message["Subject"] = subject
            message["From"] = email.utils.formataddr((s.sender_name, s.sender_email))
            message["To"] = recipient
            message.attach(MIMEText(body, "html"))
            server.sendmail(s.sender_email, recipient, message.as_string())


if __name__ == "__main__":
    # run with "python -m src.emails"
    from dotenv import load_dotenv
    from jinja2 import Template

    load_dotenv()

    settings = Settings()

    # Dummy test data
    data = {
        "upper taxa": ["Insecta", "Plantae", "Mollusca", "Other"],
        "name": ["Species A", "Species B", "Species C", "Species D"],
        "link": ["http://a.com", "http://b.com", "http://c.com", "http://d.com"],
    }

    df = pd.DataFrame(data)
    html = df.to_html(index=False, escape=False)
    tables = [EmailTable(title="Test Table", html=html)]
    template = Template(
        "{{ tables[0].title }}<br>{{ tables[0].html }}", autoescape=True
    )
    body = render_email_body(template, tables, empty_message="No data.")

    print("Email Body:")
    print(body)
