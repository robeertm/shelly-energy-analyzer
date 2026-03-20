"""email_sender.py – SMTP email sending for Shelly Energy Analyzer reports.

Uses only Python standard-library modules (smtplib, ssl, email.*).
"""
from __future__ import annotations

import logging
import smtplib
import ssl
import tempfile
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import List, Optional, Tuple

_log = logging.getLogger(__name__)


def parse_recipients(recipients_str: str) -> List[str]:
    """Parse a comma-separated string of email addresses into a list."""
    return [r.strip() for r in (recipients_str or "").split(",") if r.strip()]


def send_email(
    *,
    smtp_host: str,
    smtp_port: int,
    use_tls: bool,
    use_ssl: bool,
    username: str,
    password: str,
    sender: str,
    recipients: List[str],
    subject: str,
    body: str,
    attachments: Optional[List[Path]] = None,
) -> Tuple[bool, str]:
    """Send an email via SMTP.

    Priority: use_ssl (implicit TLS on port 465) > use_tls (STARTTLS on port 587).
    Returns (ok, error_message).
    """
    if not smtp_host:
        return False, "SMTP host not configured"
    if not recipients:
        return False, "No recipients configured"
    if not sender:
        return False, "Sender address not configured"

    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    for path in (attachments or []):
        try:
            data = Path(path).read_bytes()
            part = MIMEBase("application", "octet-stream")
            part.set_payload(data)
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{Path(path).name}"',
            )
            msg.attach(part)
        except Exception as e:
            _log.warning("email_sender: could not attach %s: %s", path, e)

    try:
        if use_ssl:
            ctx = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ctx, timeout=20) as server:
                if username:
                    server.login(username, password)
                server.sendmail(sender, recipients, msg.as_bytes())
        else:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
                if use_tls:
                    ctx = ssl.create_default_context()
                    server.starttls(context=ctx)
                if username:
                    server.login(username, password)
                server.sendmail(sender, recipients, msg.as_bytes())
        return True, ""
    except Exception as e:
        _log.warning("email_sender: send failed: %s", e)
        return False, str(e)


def send_email_from_cfg(cfg_ui, subject: str, body: str, attachments: Optional[List[Path]] = None) -> Tuple[bool, str]:
    """Convenience wrapper that reads SMTP settings from a UiConfig object."""
    return send_email(
        smtp_host=str(getattr(cfg_ui, "email_smtp_host", "") or ""),
        smtp_port=int(getattr(cfg_ui, "email_smtp_port", 587) or 587),
        use_tls=bool(getattr(cfg_ui, "email_smtp_use_tls", True)),
        use_ssl=bool(getattr(cfg_ui, "email_smtp_use_ssl", False)),
        username=str(getattr(cfg_ui, "email_smtp_username", "") or ""),
        password=str(getattr(cfg_ui, "email_smtp_password", "") or ""),
        sender=str(getattr(cfg_ui, "email_smtp_sender", "") or ""),
        recipients=parse_recipients(str(getattr(cfg_ui, "email_recipients", "") or "")),
        subject=subject,
        body=body,
        attachments=attachments,
    )
