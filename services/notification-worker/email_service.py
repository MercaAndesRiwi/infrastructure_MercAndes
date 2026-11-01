"""
Email notification service for MercaAndes.
Provides exact email template formatting and resilient SMTP/AWS SES dispatch.
"""

from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional

# Load environment variables strictly from .env if present
try:
    from dotenv import load_dotenv
    cur = Path(__file__).resolve().parent
    for _ in range(4):
        candidate = cur / ".env"
        if candidate.exists():
            load_dotenv(dotenv_path=candidate)
            break
        if cur.parent == cur:
            break
        cur = cur.parent
except Exception:
    pass


def format_currency_cop(amount: float | int) -> str:
    """Format currency matching Colombian/MercaAndes standard ($XX.XXX)."""
    return f"${int(amount):,}".replace(",", ".")


def format_purchase_email(
    customer_name: str,
    order_id: str,
    purchase_date: str,
    products: List[Dict[str, Any]],
    total_paid: float | int,
    status: str = "Confirmado / En preparación",
    company: str = "MercaAndes",
) -> str:
    product_lines = []
    for item in products:
        qty = item.get("quantity", 1)
        name = item.get("name", "Producto")
        subtotal = item.get("subtotal")
        if subtotal is None:
            unit_price = item.get("unit_price", 0)
            subtotal = unit_price * qty
        formatted_price = format_currency_cop(subtotal)
        product_lines.append(f"  * {qty}x {name} - {formatted_price}")

    products_text = "\n".join(product_lines)
    total_formatted = format_currency_cop(total_paid)

    body = (
        f"¡Hola, {customer_name}!\n\n"
        f"¡Buenas noticias! Tu pedido ha sido procesado con éxito.\n\n"
        f"Conoce los detalles de tu compra:\n"
        f"- Empresa: {company}\n"
        f"- N° de Pedido: {order_id}\n"
        f"- Estado: {status}\n"
        f"- Fecha de Compra: {purchase_date}\n"
        f"- Productos:\n"
        f"{products_text}\n"
        f"- Total pagado: {total_formatted}\n\n"
        f"Puedes realizar el seguimiento de tu envío ingresando a Mis Compras > Detalle de Pedido.\n\n"
        f"¡Gracias por comprar en MercaAndes!\n"
        f"Atentamente,\n"
        f"Equipo de Atención al Cliente | MercaAndes"
    )
    return body


def send_email(
    to_email: str,
    subject: str,
    body_text: str,
    from_email: Optional[str] = None,
) -> bool:
    """
    Sends email via SMTP (AWS SES or standard SMTP, Port 587 STARTTLS).
    If SMTP_HOST is not configured, runs in resilient Mock/Dry-run mode,
    logging the message to console without raising an exception.
    """
    # Strictly read SMTP settings from environment variables without hardcoded defaults
    smtp_host = (os.environ.get("SMTP_HOST") or "").strip()
    smtp_port_raw = os.environ.get("SMTP_PORT")
    smtp_port = int(smtp_port_raw) if smtp_port_raw else 587
    smtp_user = (os.environ.get("SMTP_USER") or "").strip()
    smtp_password = (os.environ.get("SMTP_PASSWORD") or "").strip()
    smtp_use_tls_raw = os.environ.get("SMTP_USE_TLS")
    use_tls = smtp_use_tls_raw.lower() in ("true", "1", "yes") if smtp_use_tls_raw else False
    sender = from_email or os.environ.get("EMAIL_FROM")

    # Safe Fallback / Dry-Run (Local Development or AWS before SES credentials)
    if not smtp_host:
        print("\n" + "=" * 70)
        print("[MOCK / SIMULACIÓN EMAIL] Servidor SMTP no configurado (Modo Dry-Run)")
        print(f"Para: {to_email}")
        print(f"De: {sender}")
        print(f"Asunto: {subject}")
        print("-" * 70)
        print(body_text)
        print("=" * 70 + "\n")
        return True

    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = to_email
        msg.set_content(body_text)

        print(f"[SMTP] Conectando a {smtp_host}:{smtp_port}...")
        with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
            if use_tls:
                server.starttls()
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.send_message(msg)

        print(f"[SMTP] Correo enviado exitosamente a {to_email}")
        return True
    except Exception as exc:
        print(f"[ERROR SMTP] No se pudo enviar el correo a {to_email}: {exc}")
        raise
