import json

import pytest

from operational_messaging.alerts import process_alert


def test_process_alert_appends_a_json_line(tmp_path, monkeypatch):
    log_path = tmp_path / "processed.jsonl"
    monkeypatch.setenv("ALERT_LOG_PATH", str(log_path))

    process_alert(
        {
            "message_id": "m-1",
            "message_type": "STOCKOUT_RISK",
            "payload": {"store_id": "S-1"},
        }
    )

    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["message_id"] == "m-1"


@pytest.mark.parametrize("payload", ["invalid-payload", [], 42, None])
def test_process_alert_rejects_a_payload_that_is_not_an_object(payload, tmp_path, monkeypatch):
    monkeypatch.setenv("ALERT_LOG_PATH", str(tmp_path / "processed.jsonl"))

    with pytest.raises(ValueError, match="object payload"):
        process_alert({"message_type": "DURABILITY_TEST", "payload": payload})


def test_process_alert_requires_a_message_type(tmp_path, monkeypatch):
    monkeypatch.setenv("ALERT_LOG_PATH", str(tmp_path / "processed.jsonl"))

    with pytest.raises(ValueError, match="message_type"):
        process_alert({"payload": {"store_id": "S-1"}})
