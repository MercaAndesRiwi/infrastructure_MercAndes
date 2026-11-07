"""
Email notification service for MercaAndes.
Provides exact email template formatting, responsive HTML layout, and resilient SMTP/AWS SES dispatch.
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


def format_purchase_email_html(
    customer_name: str,
    order_id: str,
    purchase_date: str,
    products: List[Dict[str, Any]],
    total_paid: float | int,
    status: str = "Confirmado / En preparación",
    company: str = "MercaAndes",
) -> str:
    """Generates a responsive and modern HTML email template for order confirmation."""
    product_rows_html = ""
    for item in products:
        name = item.get("name", "Producto")
        qty = item.get("quantity", 1)
        subtotal = item.get("subtotal")
        if subtotal is None:
            unit_price = item.get("unit_price", 0)
            subtotal = unit_price * qty
        formatted_subtotal = format_currency_cop(subtotal)
        product_rows_html += f"""
        <tr style="border-bottom: 1px solid #edf2f7;">
            <td style="padding: 14px 16px; font-size: 14px; color: #2d3748; font-weight: 500;">
                {name}
            </td>
            <td style="padding: 14px 16px; font-size: 14px; color: #718096; text-align: center;">
                <span style="background: #edf2f7; padding: 4px 10px; border-radius: 12px; font-weight: 600;">{qty}</span>
            </td>
            <td style="padding: 14px 16px; font-size: 14px; color: #2d3748; text-align: right; font-weight: 600;">
                {formatted_subtotal}
            </td>
        </tr>
        """

    total_formatted = format_currency_cop(total_paid)

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Confirmación de Pedido - {company}</title>
</head>
<body style="margin: 0; padding: 0; background-color: #f7fafc; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
    <table width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color: #f7fafc; padding: 30px 15px;">
        <tr>
            <td align="center">
                <table width="100%" border="0" cellspacing="0" cellpadding="0" style="max-width: 600px; background-color: #ffffff; border-radius: 16px; overflow: hidden; box-shadow: 0 10px 25px rgba(0,0,0,0.06); border: 1px solid #e2e8f0;">
                    <tr>
                        <td style="background: linear-gradient(135deg, #1B4D3E 0%, #0d2820 100%); padding: 35px 30px; text-align: center;">
                            <div style="font-size: 28px; font-weight: 800; color: #ffffff; letter-spacing: 1px;">
                                🛒 {company}
                            </div>
                            <div style="color: #D4AF37; font-size: 13px; font-weight: 600; text-transform: uppercase; letter-spacing: 2px; margin-top: 6px;">
                                Plataforma Omnicanal Andina
                            </div>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 35px 35px 20px 35px; text-align: center;">
                            <div style="display: inline-block; background-color: #def7ec; color: #03543f; padding: 8px 18px; border-radius: 30px; font-size: 13px; font-weight: 700; margin-bottom: 18px; letter-spacing: 0.5px;">
                                ✓ {status}
                            </div>
                            <h1 style="margin: 0 0 10px 0; font-size: 24px; color: #1a202c; font-weight: 700;">
                                ¡Gracias por tu compra, {customer_name}!
                            </h1>
                            <p style="margin: 0; color: #4a5568; font-size: 15px; line-height: 1.6;">
                                Tu pedido ha sido confirmado y está siendo preparado por nuestros sellers aliados con la mayor dedicación.
                            </p>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 0 35px 25px 35px;">
                            <table width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color: #f8fafc; border-radius: 12px; padding: 18px 20px; border: 1px solid #edf2f7;">
                                <tr>
                                    <td style="width: 50%; vertical-align: top;">
                                        <div style="font-size: 11px; text-transform: uppercase; color: #a0aec0; font-weight: 700; letter-spacing: 0.5px;">Número de Pedido</div>
                                        <div style="font-size: 16px; font-weight: 700; color: #1B4D3E; margin-top: 4px;">{order_id}</div>
                                    </td>
                                    <td style="width: 50%; vertical-align: top; text-align: right;">
                                        <div style="font-size: 11px; text-transform: uppercase; color: #a0aec0; font-weight: 700; letter-spacing: 0.5px;">Fecha de Compra</div>
                                        <div style="font-size: 15px; font-weight: 600; color: #2d3748; margin-top: 4px;">{purchase_date}</div>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 0 35px 30px 35px;">
                            <div style="font-size: 14px; font-weight: 700; color: #2d3748; margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.5px;">
                                Resumen de Productos
                            </div>
                            <table width="100%" border="0" cellspacing="0" cellpadding="0" style="border-collapse: collapse; border: 1px solid #edf2f7; border-radius: 10px; overflow: hidden;">
                                <thead>
                                    <tr style="background-color: #f1f5f9;">
                                        <th style="padding: 12px 16px; font-size: 12px; font-weight: 700; color: #475569; text-align: left; text-transform: uppercase;">Producto</th>
                                        <th style="padding: 12px 16px; font-size: 12px; font-weight: 700; color: #475569; text-align: center; text-transform: uppercase; width: 60px;">Cant.</th>
                                        <th style="padding: 12px 16px; font-size: 12px; font-weight: 700; color: #475569; text-align: right; text-transform: uppercase;">Subtotal</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {product_rows_html}
                                    <tr style="background-color: #f0fdf4;">
                                        <td colspan="2" style="padding: 16px; font-size: 16px; font-weight: 800; color: #1B4D3E;">
                                            Total Pagado:
                                        </td>
                                        <td style="padding: 16px; font-size: 18px; font-weight: 800; color: #1B4D3E; text-align: right;">
                                            {total_formatted}
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 0 35px 35px 35px; text-align: center;">
                            <a href="http://3.132.51.65:8003/docs" style="display: inline-block; background: #1B4D3E; color: #ffffff; text-decoration: none; padding: 14px 32px; border-radius: 8px; font-size: 15px; font-weight: 700; letter-spacing: 0.5px; box-shadow: 0 4px 12px rgba(27,77,62,0.25);">
                                Rastrear Mi Pedido en {company} &rarr;
                            </a>
                        </td>
                    </tr>
                    <tr>
                        <td style="background-color: #f8fafc; padding: 25px 35px; text-align: center; border-top: 1px solid #edf2f7;">
                            <p style="margin: 0 0 8px 0; color: #718096; font-size: 13px;">
                                ¿Tienes alguna pregunta sobre tu pedido? Escríbenos a <a href="mailto:atencion@mercaandes.com" style="color: #1B4D3E; font-weight: 600;">atencion@mercaandes.com</a>
                            </p>
                            <p style="margin: 0; color: #a0aec0; font-size: 11px;">
                                © 2026 {company} S.A. • Red de Comercio Minorista Omnicanal (Colombia, Perú, Ecuador, Bolivia, Chile)
                            </p>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>"""
    return html


def send_email(
    to_email: str,
    subject: str,
    body_text: str,
    html_content: Optional[str] = None,
    from_email: Optional[str] = None,
) -> bool:
    """
    Sends email via SMTP (AWS SES or standard SMTP, Port 587 STARTTLS).
    If SMTP_HOST is not configured, runs in resilient Mock/Dry-run mode,
    logging the message to console without raising an exception.
    """
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
        if html_content:
            msg.add_alternative(html_content, subtype="html")

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
