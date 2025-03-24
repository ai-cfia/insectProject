import email.utils
import logging
import re
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP

import pandas as pd
from jinja2 import Environment, Template
from pydantic import validate_call

from src.custom_logging import log_call
from src.dates import get_yesterday
from src.preprocess import group_by_taxa
from src.settings import Settings

log = logging.getLogger(__name__)


@log_call
@validate_call
def make_email_title(settings: Settings, error: bool) -> str:
    date_str = get_yesterday()
    template = Template(settings.email_title_template, autoescape=True)
    return template.render(
        prefix=settings.email_title_prefix,
        date=date_str,
        error=error,
        error_msg=settings.email_maintenance_msg,
    )


@log_call
@validate_call(config=dict(arbitrary_types_allowed=True))
def build_email_tables(
    s: Settings, df_regulated: pd.DataFrame, df_regulated_us: pd.DataFrame
):
    categories = s.upper_taxa + [s.other_taxa_label]
    all_dfs = [
        ("iconic_taxa", group_by_taxa(s, df_regulated)),
        ("US iconic_taxa", group_by_taxa(s, df_regulated_us)),
    ]
    return [
        (f"{prefix}: {name}", df.to_html(render_links=True, justify="center"))
        for prefix, dfs in all_dfs
        for df, name in zip(dfs, categories)
        if df is not None and not df.empty
    ]


@validate_call
def sanitize_html_links(html: str):
    return re.sub('target="_blank".+>', "> URL </a>", html)


@log_call
@validate_call(config=dict(arbitrary_types_allowed=True))
def render_html(tables, template_env: Environment, template_name: str):
    template = template_env.get_template(template_name)
    html = template.render(tables=tables)
    html = sanitize_html_links(html)
    return html


@log_call
@validate_call
def send_smtp_email(settings: Settings, html: str, subject: str):
    with SMTP(settings.smtp_host, settings.smtp_port) as server:
        server.set_debuglevel(settings.smtp_debug_level)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(settings.smtp_username, settings.smtp_password.get_secret_value())
        for recipient in settings.recipient_emails:
            log.info(f"Sending email to {recipient}")
            message = MIMEMultipart("alternative")
            message["Subject"] = subject
            message["From"] = email.utils.formataddr(
                (settings.sender_name, settings.sender_email)
            )
            message["To"] = recipient
            message.attach(MIMEText(html, "html"))
            server.sendmail(settings.sender_email, recipient, message.as_string())


@log_call
@validate_call(config=dict(arbitrary_types_allowed=True))
def send_email(
    settings: Settings,
    df_regulated: pd.DataFrame,
    df_regulated_us: pd.DataFrame,
    subject: str = "Invasive Species Report",
):
    try:
        tables = build_email_tables(settings, df_regulated, df_regulated_us)
        html = render_html(tables, settings.template_env, settings.email_template_name)
        send_smtp_email(settings, html, subject)
    except Exception as e:
        import traceback

        log.error(f"Error during email sending: {e}")
        log.error(traceback.format_exc())
    else:
        log.info("All emails sent successfully.")


if __name__ == "__main__":
    # run with "python -m src.emails"
    from dotenv import load_dotenv

    load_dotenv()

    settings = Settings()

    # Dummy test data
    data = {
        "upper taxa": ["Insecta", "Plantae", "Mollusca", "Other"],
        "name": ["Species A", "Species B", "Species C", "Species D"],
        "link": ["http://a.com", "http://b.com", "http://c.com", "http://d.com"],
    }

    df = pd.DataFrame(data)
    send_email(settings, df, df)
