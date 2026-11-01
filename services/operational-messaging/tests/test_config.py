from operational_messaging.config import (
    ALL_QUEUES,
    DATA_QUALITY_QUEUE,
    REPORT_QUEUE,
    STOCKOUT_QUEUE,
    dead_letter_queue_name,
    retry_queue_name,
)


def test_required_queue_names_match_the_user_story():
    assert DATA_QUALITY_QUEUE == "alertas.calidad_datos"
    assert STOCKOUT_QUEUE == "alertas.riesgo_quiebre"
    assert REPORT_QUEUE == "tareas.reportes"
    assert set(ALL_QUEUES) == {DATA_QUALITY_QUEUE, STOCKOUT_QUEUE, REPORT_QUEUE}


def test_retry_and_dead_letter_names_are_derived_from_the_queue():
    assert retry_queue_name(STOCKOUT_QUEUE) == "alertas.riesgo_quiebre.retry"
    assert dead_letter_queue_name(STOCKOUT_QUEUE) == "alertas.riesgo_quiebre.dlq"
