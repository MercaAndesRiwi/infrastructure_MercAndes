"""Small helper for the operational message envelope."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def new_message(message_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "message_id": str(uuid4()),
        "message_type": message_type,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
