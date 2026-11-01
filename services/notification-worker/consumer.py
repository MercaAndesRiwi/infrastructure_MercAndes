"""
RabbitMQ Consumer Worker for MercaAndes Purchase Notifications.
Runs as an Asynchronous Worker in ECS Fargate (AWS Architecture Phase 4).
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from pathlib import Path

import pika

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

from email_service import format_purchase_email, send_email

# Strictly read environment variables without fallback defaults
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST")
RABBITMQ_PORT_RAW = os.environ.get("RABBITMQ_PORT")
RABBITMQ_PORT = int(RABBITMQ_PORT_RAW) if RABBITMQ_PORT_RAW else None
RABBITMQ_USER = os.environ.get("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.environ.get("RABBITMQ_PASSWORD")
RABBITMQ_VHOST = os.environ.get("RABBITMQ_VHOST")
QUEUE_NAME = os.environ.get("RABBITMQ_QUEUE_PURCHASE_NOTIFICATIONS")

missing_rabbit = [name for name, val in [
    ("RABBITMQ_HOST", RABBITMQ_HOST),
    ("RABBITMQ_PORT", RABBITMQ_PORT),
    ("RABBITMQ_USER", RABBITMQ_USER),
    ("RABBITMQ_PASSWORD", RABBITMQ_PASSWORD),
    ("RABBITMQ_VHOST", RABBITMQ_VHOST),
    ("RABBITMQ_QUEUE_PURCHASE_NOTIFICATIONS", QUEUE_NAME),
] if not val]

if missing_rabbit:
    raise EnvironmentError(
        f"Missing required RabbitMQ environment variables: {', '.join(missing_rabbit)}. "
        "Strictly reading from environment variables without defaults."
    )

_RUNNING = True


def _handle_shutdown(signum, frame):
    global _RUNNING
    print(f"\n[WORKER] Señal {signum} recibida. Deteniendo consumidor limpiamente...")
    _RUNNING = False


def process_message(ch, method, properties, body):
    """Processes a simulated purchase message and dispatches the email."""
    try:
        data = json.loads(body.decode("utf-8"))
        customer_name = data.get("customer_name", "Cliente")
        customer_email = data.get("customer_email")
        order_id = data.get("order_id", "MA-2026-00000")
        purchase_date = data.get("purchase_date", "28-10-2026")
        status = data.get("status", "Confirmado / En preparación")
        company = data.get("company", "MercaAndes")
        products = data.get("products", [])
        total_paid = data.get("total_paid", 0)

        if not customer_email:
            print(f"[WORKER] Advertencia: Pedido {order_id} no contiene customer_email. Omitiendo envío.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        email_body = format_purchase_email(
            customer_name=customer_name,
            order_id=order_id,
            purchase_date=purchase_date,
            products=products,
            total_paid=total_paid,
            status=status,
            company=company,
        )

        subject = f"Confirmación de tu pedido {order_id} - {company}"
        send_email(to_email=customer_email, subject=subject, body_text=email_body)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[WORKER] Notificación procesada y confirmada para pedido {order_id} ({customer_email})")
    except Exception as exc:
        print(f"[WORKER ERROR] Error procesando mensaje de compra: {exc}")
        # En caso de error inesperado, reintentamos o nack según política
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def run_consumer():
    """Main consumer loop with automatic reconnection."""
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
        heartbeat=60,
    )

    print(f"[WORKER] Iniciando Notification Consumer Worker en {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
    print(f"[WORKER] Escuchando en la cola: '{QUEUE_NAME}'")

    while _RUNNING:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            # Declaramos la cola durable para alta disponibilidad
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(
                queue=QUEUE_NAME,
                on_message_callback=process_message,
                auto_ack=False,
            )

            print("[WORKER] Conexión establecida con RabbitMQ. Esperando eventos de compra...")
            while _RUNNING and channel.is_open:
                connection.process_data_events(time_limit=1)

            if connection.is_open:
                connection.close()

        except pika.exceptions.AMQPConnectionError as exc:
            if not _RUNNING:
                break
            print(f"[WORKER] RabbitMQ no disponible ({exc}). Reintentando conexión en 5 segundos...")
            time.sleep(5)
        except Exception as exc:
            if not _RUNNING:
                break
            print(f"[WORKER] Excepción no controlada: {exc}. Reintentando en 5 segundos...")
            time.sleep(5)

    print("[WORKER] Consumidor de notificaciones finalizado.")


if __name__ == "__main__":
    run_consumer()
