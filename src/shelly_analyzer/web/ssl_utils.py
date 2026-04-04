"""SSL utilities for the Flask web server."""
from __future__ import annotations

import logging
import socket
from pathlib import Path
from typing import Tuple


def _ensure_ssl_cert(cert_dir: Path) -> Tuple[Path, Path]:
    """Generate a self-signed TLS certificate if none exists.

    Returns (cert_path, key_path).  Tries the ``cryptography`` library first
    (cross-platform), then falls back to ``openssl`` CLI.
    """
    cert_dir.mkdir(parents=True, exist_ok=True)
    cert_path = cert_dir / "server.crt"
    key_path = cert_dir / "server.key"
    if cert_path.exists() and key_path.exists():
        return cert_path, key_path

    logger = logging.getLogger(__name__)
    logger.info("Generating self-signed TLS certificate for HTTPS …")

    # Try 1: pure-Python via cryptography library (works on all platforms)
    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        import datetime

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Shelly Energy Analyzer")])
        cert = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.utcnow())
            .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=3650))
            .sign(key, hashes.SHA256())
        )
        key_path.write_bytes(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ))
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        logger.info("TLS certificate created (cryptography lib): %s", cert_path)
        return cert_path, key_path
    except ImportError:
        logger.debug("cryptography library not available, trying openssl CLI")
    except Exception as e:
        logger.debug("cryptography cert generation failed: %s", e)

    # Try 2: openssl CLI (available on macOS, Linux, some Windows)
    import shutil
    import subprocess

    if not shutil.which("openssl"):
        raise RuntimeError(
            "Cannot generate TLS certificate: neither 'cryptography' library "
            "nor 'openssl' CLI found. Install one of them or provide your own "
            "certificate files."
        )
    try:
        subprocess.run(
            [
                "openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", str(key_path),
                "-out", str(cert_path),
                "-days", "3650",
                "-nodes",
                "-subj", "/CN=Shelly Energy Analyzer",
            ],
            check=True,
            capture_output=True,
            timeout=30,
        )
        logger.info("TLS certificate created (openssl CLI): %s", cert_path)
    except Exception as e:
        logger.warning("Failed to generate TLS certificate: %s", e)
        raise
    return cert_path, key_path


def _local_ip_guess() -> str:
    """Best-effort LAN IP discovery (no external calls)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Doesn't send packets; used only to pick a route.
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"
