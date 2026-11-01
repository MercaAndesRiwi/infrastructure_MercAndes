from operational_messaging.messages import new_message


def test_new_message_has_the_expected_envelope():
    message = new_message("STOCKOUT_RISK", {"store_id": "S-1"})

    assert message["message_type"] == "STOCKOUT_RISK"
    assert message["payload"] == {"store_id": "S-1"}
    assert message["message_id"]
    assert message["created_at"]
