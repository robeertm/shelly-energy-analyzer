"""SSL utilities for the Flask web server.

Provides certificate generation, expiry inspection and auto-renewal for the
self-signed cert served by the built-in web server.
"""
from __future__ import annotations

import datetime as _dt
import logging
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class CertInfo:
    """Parsed metadata about a TLS certificate on disk."""
    exists: bool
    path: str
    subject: str = ""
    issuer: str = ""
    not_before: Optional[_dt.datetime] = None
    not_after: Optional[_dt.datetime] = None
    days_remaining: Optional[int] = None
    is_self_signed: bool = False
    sha256: str = ""
    error: str = ""


def _utcnow() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def _discover_local_hostnames_ips() -> tuple:
    """Collect every DNS name and IP address that this host answers to.

    Returned as (dns_names: list[str], ip_addresses: list[str]).
    Always includes ``localhost`` + ``127.0.0.1``. On Linux/macOS it shells
    out to ``ip``/``hostname``/``ifconfig`` to enumerate every network
    interface (LAN, Tailscale, Docker bridges, …) so the self-signed cert
    is valid for every address a browser / widget might use.
    """
    import socket
    import subprocess
    import re as _re

    dns: set = {"localhost"}
    ips: set = {"127.0.0.1"}

    try:
        host = socket.gethostname()
        if host:
            dns.add(host)
            # Short name without domain
            short = host.split(".")[0]
            if short and short != host:
                dns.add(short)
    except Exception:
        pass

    try:
        fqdn = socket.getfqdn()
        if fqdn:
            dns.add(fqdn)
    except Exception:
        pass

    # Linux: hostname -I returns all non-loopback IPs
    try:
        out = subprocess.check_output(
            ["hostname", "-I"], stderr=subprocess.DEVNULL, timeout=3
        ).decode()
        for ip in out.split():
            if _re.match(r"^\d+\.\d+\.\d+\.\d+$", ip):
                ips.add(ip)
    except Exception:
        pass

    # Linux: `ip -4 addr` (covers all interfaces incl. tailscale0)
    try:
        out = subprocess.check_output(
            ["ip", "-4", "addr"], stderr=subprocess.DEVNULL, timeout=3
        ).decode()
        for m in _re.finditer(r"inet (\d+\.\d+\.\d+\.\d+)", out):
            ips.add(m.group(1))
    except Exception:
        pass

    # macOS / BSD: ifconfig
    try:
        out = subprocess.check_output(
            ["ifconfig"], stderr=subprocess.DEVNULL, timeout=3
        ).decode()
        for m in _re.finditer(r"inet (\d+\.\d+\.\d+\.\d+)", out):
            ips.add(m.group(1))
    except Exception:
        pass

    # Also resolve own hostname to catch anything else
    try:
        host = socket.gethostname()
        for info in socket.getaddrinfo(host, None, socket.AF_INET):
            ip = info[4][0]
            if _re.match(r"^\d+\.\d+\.\d+\.\d+$", ip):
                ips.add(ip)
    except Exception:
        pass

    # Filter reverse-DNS PTR names (e.g. "1.0.168.192.in-addr.arpa") — never
    # useful as a browsable hostname, only adds noise to the cert.
    dns = {d for d in dns if not d.endswith(".arpa")}

    return sorted(dns), sorted(ips)


def _generate_self_signed(cert_path: Path, key_path: Path, *, days: int = 3650, extra_dns: list = None, extra_ips: list = None) -> None:
    """Generate a fresh self-signed cert/key pair at the given paths.

    Prefers the pure-Python ``cryptography`` library; falls back to the
    ``openssl`` CLI when the library is unavailable.

    The cert includes a SubjectAlternativeName extension with every local
    DNS name and IP address (LAN, Tailscale, loopback, hostname) — so it
    validates correctly when reached via any of them. iOS in particular
    (Scriptable widgets, Safari) rejects certs without proper SANs.
    """
    cert_path.parent.mkdir(parents=True, exist_ok=True)

    dns_names, ip_addrs = _discover_local_hostnames_ips()
    for d in (extra_dns or []):
        if d and d not in dns_names:
            dns_names.append(d)
    for i in (extra_ips or []):
        if i and i not in ip_addrs:
            ip_addrs.append(i)

    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        import ipaddress as _ipaddr

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        # Use a proper hostname as CN (first DNS name, falls back to the generic product name)
        import socket as _socket
        try:
            cn_value = _socket.gethostname() or "shelly-energy-analyzer"
        except Exception:
            cn_value = "shelly-energy-analyzer"
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn_value)])

        san_entries = [x509.DNSName(d) for d in dns_names]
        for ip in ip_addrs:
            try:
                san_entries.append(x509.IPAddress(_ipaddr.ip_address(ip)))
            except Exception:
                pass
        san = x509.SubjectAlternativeName(san_entries)

        now = _utcnow()
        cert = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + _dt.timedelta(days=days))
            .add_extension(san, critical=False)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .sign(key, hashes.SHA256())
        )
        key_path.write_bytes(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        try:
            key_path.chmod(0o600)
        except Exception:
            pass
        logger.info(
            "TLS certificate created (cryptography lib): %s (SAN: %d DNS + %d IPs)",
            cert_path, len(dns_names), len(ip_addrs),
        )
        return
    except ImportError:
        logger.debug("cryptography library not available, trying openssl CLI")
    except Exception as e:
        logger.debug("cryptography cert generation failed: %s", e)

    import shutil
    import subprocess

    if not shutil.which("openssl"):
        raise RuntimeError(
            "Cannot generate TLS certificate: neither 'cryptography' library "
            "nor 'openssl' CLI found. Install one of them or provide your own "
            "certificate files."
        )

    # Build -addext subjectAltName=... string for the openssl CLI fallback.
    import socket as _socket
    try:
        cn_value = _socket.gethostname() or "shelly-energy-analyzer"
    except Exception:
        cn_value = "shelly-energy-analyzer"
    san_parts = [f"DNS:{d}" for d in dns_names] + [f"IP:{i}" for i in ip_addrs]
    san_arg = "subjectAltName=" + ",".join(san_parts)

    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", str(key_path),
            "-out", str(cert_path),
            "-days", str(int(days)),
            "-nodes",
            "-subj", f"/CN={cn_value}",
            "-addext", san_arg,
        ],
        check=True,
        capture_output=True,
        timeout=30,
    )
    try:
        key_path.chmod(0o600)
    except Exception:
        pass
    logger.info(
        "TLS certificate created (openssl CLI): %s (SAN: %d DNS + %d IPs)",
        cert_path, len(dns_names), len(ip_addrs),
    )


def _inspect_with_cryptography(cert_path: Path, info: CertInfo) -> bool:
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes
    except ImportError:
        return False
    try:
        pem = cert_path.read_bytes()
        cert = x509.load_pem_x509_certificate(pem)
        try:
            nb = cert.not_valid_before_utc  # cryptography >= 42
            na = cert.not_valid_after_utc
        except AttributeError:
            nb = cert.not_valid_before.replace(tzinfo=_dt.timezone.utc)
            na = cert.not_valid_after.replace(tzinfo=_dt.timezone.utc)
        info.not_before = nb
        info.not_after = na
        info.days_remaining = int((na - _utcnow()).total_seconds() // 86400)
        try:
            info.subject = cert.subject.rfc4514_string()
            info.issuer = cert.issuer.rfc4514_string()
            info.is_self_signed = info.subject == info.issuer
        except Exception:
            pass
        try:
            fp = cert.fingerprint(hashes.SHA256())
            info.sha256 = ":".join(f"{b:02x}" for b in fp)
        except Exception:
            pass
        return True
    except Exception as e:
        info.error = f"parse error: {e}"
        return True  # we "handled" it — no fallback needed


def _inspect_with_openssl(cert_path: Path, info: CertInfo) -> bool:
    import shutil
    import subprocess

    if not shutil.which("openssl"):
        return False
    try:
        out = subprocess.run(
            ["openssl", "x509", "-in", str(cert_path), "-noout",
             "-subject", "-issuer", "-startdate", "-enddate", "-fingerprint", "-sha256"],
            capture_output=True, text=True, check=True, timeout=10,
        ).stdout
    except Exception as e:
        info.error = f"openssl parse failed: {e}"
        return True

    def _parse_date(s: str) -> Optional[_dt.datetime]:
        # openssl prints e.g. "notBefore=Apr 10 12:00:00 2026 GMT"
        s = s.split("=", 1)[1].strip() if "=" in s else s.strip()
        for fmt in ("%b %d %H:%M:%S %Y %Z", "%b %d %H:%M:%S %Y GMT"):
            try:
                return _dt.datetime.strptime(s, fmt).replace(tzinfo=_dt.timezone.utc)
            except ValueError:
                continue
        return None

    for line in out.splitlines():
        line = line.strip()
        if line.startswith("subject="):
            info.subject = line[len("subject="):].strip()
        elif line.startswith("issuer="):
            info.issuer = line[len("issuer="):].strip()
        elif line.startswith("notBefore="):
            info.not_before = _parse_date(line)
        elif line.startswith("notAfter="):
            info.not_after = _parse_date(line)
        elif line.startswith("sha256 Fingerprint="):
            info.sha256 = line.split("=", 1)[1].strip().lower()
    if info.not_after is not None:
        info.days_remaining = int((info.not_after - _utcnow()).total_seconds() // 86400)
    if info.subject and info.issuer:
        info.is_self_signed = info.subject == info.issuer
    return True


def inspect_cert(cert_path: Path) -> CertInfo:
    """Return parsed metadata about a certificate file (best-effort).

    Prefers the ``cryptography`` library and falls back to the ``openssl`` CLI
    so that systems without the library still get full cert details.
    """
    info = CertInfo(exists=cert_path.exists(), path=str(cert_path))
    if not info.exists:
        return info
    if _inspect_with_cryptography(cert_path, info):
        return info
    if _inspect_with_openssl(cert_path, info):
        return info
    info.error = "Neither 'cryptography' library nor 'openssl' CLI available to inspect the certificate"
    return info


def _backup_existing(cert_path: Path, key_path: Path) -> None:
    """Move existing cert+key to .bak siblings so a fresh pair can replace them."""
    ts = _utcnow().strftime("%Y%m%d%H%M%S")
    for p in (cert_path, key_path):
        if p.exists():
            try:
                bak = p.with_suffix(p.suffix + f".bak.{ts}")
                p.replace(bak)
                logger.info("Backed up %s → %s", p.name, bak.name)
            except Exception as e:
                logger.warning("Failed to back up %s: %s", p, e)


def ensure_ssl_cert(
    cert_dir: Path,
    *,
    auto_renew: bool = True,
    renew_days: int = 30,
    cert_lifetime_days: int = 3650,
) -> Tuple[Path, Path, CertInfo]:
    """Ensure a valid self-signed cert/key pair exists under *cert_dir*.

    - Generates a fresh pair if none exists.
    - If ``auto_renew`` is true, inspects the existing cert and regenerates
      when fewer than ``renew_days`` days remain (or the cert is already
      expired). The old pair is backed up to ``.bak.<timestamp>`` siblings.

    Returns ``(cert_path, key_path, CertInfo of the effective cert)``.
    """
    cert_dir.mkdir(parents=True, exist_ok=True)
    cert_path = cert_dir / "server.crt"
    key_path = cert_dir / "server.key"

    if not cert_path.exists() or not key_path.exists():
        logger.info("Generating self-signed TLS certificate …")
        _generate_self_signed(cert_path, key_path, days=cert_lifetime_days)
        return cert_path, key_path, inspect_cert(cert_path)

    info = inspect_cert(cert_path)
    if auto_renew and info.days_remaining is not None and info.days_remaining < int(renew_days):
        logger.warning(
            "TLS certificate expires in %d days (< %d) — regenerating",
            info.days_remaining, int(renew_days),
        )
        _backup_existing(cert_path, key_path)
        _generate_self_signed(cert_path, key_path, days=cert_lifetime_days)
        info = inspect_cert(cert_path)
    elif info.days_remaining is not None and info.days_remaining < 0:
        logger.warning(
            "TLS certificate expired %d days ago — regenerating (auto_renew=%s)",
            -info.days_remaining, auto_renew,
        )
        if auto_renew:
            _backup_existing(cert_path, key_path)
            _generate_self_signed(cert_path, key_path, days=cert_lifetime_days)
            info = inspect_cert(cert_path)

    return cert_path, key_path, info


def force_regenerate(cert_dir: Path, *, cert_lifetime_days: int = 3650) -> Tuple[Path, Path, CertInfo]:
    """Unconditionally back up + regenerate the self-signed cert pair."""
    cert_dir.mkdir(parents=True, exist_ok=True)
    cert_path = cert_dir / "server.crt"
    key_path = cert_dir / "server.key"
    _backup_existing(cert_path, key_path)
    _generate_self_signed(cert_path, key_path, days=cert_lifetime_days)
    return cert_path, key_path, inspect_cert(cert_path)


# ── Back-compat alias ──────────────────────────────────────────────────────
def _ensure_ssl_cert(cert_dir: Path) -> Tuple[Path, Path]:
    """Deprecated shim: returns just (cert, key) without inspection.

    Kept so callers that imported the private name still work while we roll
    the blueprint/API out.
    """
    cert, key, _ = ensure_ssl_cert(cert_dir)
    return cert, key


def _local_ip_guess() -> str:
    """Best-effort LAN IP discovery (no external calls)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"
