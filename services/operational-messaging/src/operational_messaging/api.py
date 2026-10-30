"""HTTP endpoint that queues report requests and returns immediately."""

from __future__ import annotations

import logging
from typing import Literal

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from operational_messaging.config import REPORT_QUEUE
from operational_messaging.messages import new_message
from operational_messaging.rabbitmq import publish_message

app = FastAPI(title="MercaAndes Reports API")
logger = logging.getLogger(__name__)


class ReportRequest(BaseModel):
    report_type: Literal["stockout_risk"] = "stockout_risk"


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/reports", status_code=status.HTTP_202_ACCEPTED)
def request_report(request: ReportRequest) -> dict[str, str]:
    message = new_message("REPORT_REQUESTED", request.model_dump())
    try:
        publish_message(REPORT_QUEUE, message)
    except Exception as exc:
        logger.exception("Could not publish report request to RabbitMQ")
        raise HTTPException(status_code=503, detail="Report queue is unavailable") from exc
    return {
        "status": "accepted",
        "job_id": message["message_id"],
        "queue": REPORT_QUEUE,
    }
