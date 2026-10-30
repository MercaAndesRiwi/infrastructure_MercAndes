"""Queue names and retry settings for operational messaging."""

OPERATIONAL_EXCHANGE = "mercaandes.operational"
DEAD_LETTER_EXCHANGE = "mercaandes.operational.dlx"

DATA_QUALITY_QUEUE = "alertas.calidad_datos"
STOCKOUT_QUEUE = "alertas.riesgo_quiebre"
REPORT_QUEUE = "tareas.reportes"

ALERT_QUEUES = (DATA_QUALITY_QUEUE, STOCKOUT_QUEUE)
ALL_QUEUES = (*ALERT_QUEUES, REPORT_QUEUE)

MAX_RETRIES = 3
RETRY_DELAY_MS = 5000


def retry_queue_name(queue: str) -> str:
    return f"{queue}.retry"


def dead_letter_queue_name(queue: str) -> str:
    return f"{queue}.dlq"
