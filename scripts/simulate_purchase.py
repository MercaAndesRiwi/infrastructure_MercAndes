"""
Simulador de Compras y Generador de Notificaciones de Pedido para MercaAndes.

Uso:
    python simulate_purchase.py
    python simulate_purchase.py --customer "Nombre del Cliente" --email "cliente@mercaandes.com"
    python simulate_purchase.py --order-id "MA-2026-89412" --email "tu-correo@gmail.com"
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Añadir el path del servicio de notificaciones para reutilizar email_service
current_dir = Path(__file__).resolve().parent
service_dir = current_dir.parent / "services" / "notification-worker"
if str(service_dir) not in sys.path:
    sys.path.insert(0, str(service_dir))

try:
    from email_service import format_currency_cop, format_purchase_email, send_email
except ImportError:
    # Fallback si se ejecuta de forma aislada
    def format_currency_cop(amount):
        return f"${int(amount):,}".replace(",", ".")

    def format_purchase_email(customer_name, order_id, purchase_date, products, total_paid, status="Confirmado / En preparación", company="MercaAndes"):
        lines = []
        for p in products:
            lines.append(f"  * {p.get('quantity', 1)}x {p.get('name')} - {format_currency_cop(p.get('subtotal', 0))}")
        return (
            f"¡Hola, {customer_name}!\n\n"
            f"¡Buenas noticias! Tu pedido ha sido procesado con éxito.\n\n"
            f"Conoce los detalles de tu compra:\n"
            f"- Empresa: {company}\n"
            f"- N° de Pedido: {order_id}\n"
            f"- Estado: {status}\n"
            f"- Fecha de Compra: {purchase_date}\n"
            f"- Productos:\n" + "\n".join(lines) + "\n"
            f"- Total pagado: {format_currency_cop(total_paid)}\n\n"
            f"Puedes realizar el seguimiento de tu envío ingresando a Mis Compras > Detalle de Pedido.\n\n"
            f"¡Gracias por comprar en MercaAndes!\n"
            f"Atentamente,\n"
            f"Equipo de Atención al Cliente | MercaAndes"
        )

    def send_email(to_email, subject, body_text):
        print(body_text)
        return True


def publish_to_rabbitmq(payload: dict) -> bool:
    """Publica el evento de compra simulada a la cola de RabbitMQ"""
    try:
        import pika
    except ImportError:
        print("[RABBITMQ INFO] Librería 'pika' no instalada en este entorno local. Se continuará con el envío directo.")
        return False

    rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
    rabbitmq_port = int(os.getenv("RABBITMQ_PORT", "5672"))
    rabbitmq_user = os.getenv("RABBITMQ_USER", os.getenv("RABBITMQ_USERNAME", "guest_user"))
    rabbitmq_password = os.getenv("RABBITMQ_PASSWORD", "change_me_to_a_secure_password")
    rabbitmq_vhost = os.getenv("RABBITMQ_VHOST", "/")
    queue_name = os.getenv("RABBITMQ_QUEUE_PURCHASE_NOTIFICATIONS", "mercaandes.notifications.purchase")

    try:
        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq_host,
                port=rabbitmq_port,
                virtual_host=rabbitmq_vhost,
                credentials=credentials,
                connection_attempts=2,
                retry_delay=1,
            )
        )
        ch = conn.channel()
        ch.queue_declare(queue=queue_name, durable=True)
        ch.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=json.dumps(payload, ensure_ascii=False),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Mensaje persistente
                content_type="application/json",
            ),
        )
        conn.close()
        print(f"[RABBITMQ] Evento publicado exitosamente en la cola: '{queue_name}'")
        return True
    except Exception as exc:
        print(f"[RABBITMQ INFO] No se pudo conectar a RabbitMQ ({exc}). Se continuará con el envío directo.")
        return False


def simulate_purchase(
    customer_name: str = "Nombre del Cliente",
    customer_email: str = "cliente@mercaandes.com",
    order_id: str = "MA-2026-89412",
    purchase_date: str = "28-10-2026",
    send_mq: bool = True,
    direct_send: bool = True,
):
    """Ejecuta la simulación de compra con los datos del pedido."""
    # Productos del pedido solicitado
    products = [
        {
            "quantity": 1,
            "name": "Aceite de Oliva Extra Virgen (500ml)",
            "unit_price": 35000,
            "subtotal": 35000,
        },
        {
            "quantity": 2,
            "name": "Café Molido Especial (500g)",
            "unit_price": 24000,
            "subtotal": 48000,
        },
    ]
    total_paid = sum(p["subtotal"] for p in products)

    payload = {
        "event_type": "PURCHASE_CONFIRMED",
        "company": "MercaAndes",
        "order_id": order_id,
        "status": "Confirmado / En preparación",
        "purchase_date": purchase_date,
        "customer_name": customer_name,
        "customer_email": customer_email,
        "products": products,
        "total_paid": total_paid,
        "simulated_at": datetime.now(timezone.utc).isoformat(),
    }

    print("\n" + "=" * 70)
    print("      SIMULADOR DE COMPRA MERCANDES - PROCESANDO PEDIDO")
    print("=" * 70)
    print(f"Pedido: {order_id} | Cliente: {customer_name} ({customer_email}) | Total: {format_currency_cop(total_paid)}")

    # 1. Enviar evento a RabbitMQ para el worker asíncrono
    mq_success = False
    if send_mq:
        mq_success = publish_to_rabbitmq(payload)

    # 2. Despachar directamente por SMTP / Fallback si se solicita o si RabbitMQ no estaba activo
    if direct_send or not mq_success:
        email_body = format_purchase_email(
            customer_name=customer_name,
            order_id=order_id,
            purchase_date=purchase_date,
            products=products,
            total_paid=total_paid,
            status="Confirmado / En preparación",
            company="MercaAndes",
        )
        subject = f"Confirmación de tu pedido {order_id} - MercaAndes"
        send_email(to_email=customer_email, subject=subject, body_text=email_body)

    print("[ÉXITO] Simulación de compra completada.")
    return payload


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simula una compra en MercaAndes y envía el email de confirmación.")
    parser.add_argument("--customer", default="Nombre del Cliente", help="Nombre del cliente")
    parser.add_argument("--email", default="cliente@mercaandes.com", help="Email de destino")
    parser.add_argument("--order-id", default="MA-2026-89412", help="Identificador del pedido (ej. MA-2026-89412)")
    parser.add_argument("--date", default="28-10-2026", help="Fecha de compra (DD-MM-YYYY)")
    parser.add_argument("--no-rabbitmq", action="store_true", help="Desactiva publicación en RabbitMQ")
    parser.add_argument("--no-direct", action="store_true", help="Desactiva envío directo")

    args = parser.parse_args()

    simulate_purchase(
        customer_name=args.customer,
        customer_email=args.email,
        order_id=args.order_id,
        purchase_date=args.date,
        send_mq=not args.no_rabbitmq,
        direct_send=not args.no_direct,
    )
