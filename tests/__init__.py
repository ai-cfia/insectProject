from src.settings import Settings

settings = Settings(
    smtp_host="localhost",
    smtp_port=1025,
    smtp_username="user",
    smtp_password="secret",
    sender_email="noreply@example.com",
    observations_email_recipients=["a@example.com", "b@example.com"],
    comments_email_recipients=["x@example.com", "y@example.com"],
)
