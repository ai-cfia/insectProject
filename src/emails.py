import email.utils
import re
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP

import pandas as pd
from jinja2 import Environment

from src.preprocess import group_by_taxa
from src.settings import Settings


def build_email_tables(
    settings: Settings, df_regulated: pd.DataFrame, df_regulated_us: pd.DataFrame
):
    categories = settings.upper_taxa + [settings.other_taxa_label]
    all_dfs = [
        ("iconic_taxa", group_by_taxa(df_regulated, settings.upper_taxa)),
        ("US iconic_taxa", group_by_taxa(df_regulated_us, settings.upper_taxa)),
    ]
    return [
        (f"{prefix}: {name}", df.to_html(render_links=True, justify="center"))
        for prefix, dfs in all_dfs
        for df, name in zip(dfs, categories)
        if df is not None and not df.empty
    ]


# TODO: might not be best for all situations
def sanitize_html_links(html: str):
    return re.sub('target="_blank".+>', "> URL </a>", html)


def render_html(tables, template_env: Environment, template_name: str):
    template = template_env.get_template(template_name)
    html = template.render(tables=tables)
    html = sanitize_html_links(html)
    return html


def send_smtp_email(settings: Settings, html: str, subject: str):
    with SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as server:
        server.set_debuglevel(settings.smtp_debug_level)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(settings.smtp_username, settings.smtp_password.get_secret_value())
        for recipient in settings.recipient_emails:
            print(f"Sending email to {recipient}")
            message = MIMEMultipart("alternative")
            message["Subject"] = subject
            message["From"] = email.utils.formataddr(
                (settings.sender_name, settings.sender_email)
            )
            message["To"] = recipient
            message.attach(MIMEText(html, "html"))
            server.sendmail(settings.sender_email, recipient, message.as_string())


def send_email(
    settings: Settings,
    df_regulated: pd.DataFrame,
    df_regulated_us: pd.DataFrame,
    subject="Invasive Species Report",
):
    try:
        tables = build_email_tables(settings, df_regulated, df_regulated_us)
        html = render_html(tables, settings.template_env, settings.email_template_name)
        send_smtp_email(settings, html, subject)
    except Exception as e:
        import traceback

        print("Error during email sending:", e)
        print(traceback.format_exc())
    else:
        print("All emails sent successfully.")


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
