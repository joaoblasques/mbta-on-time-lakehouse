"""Unit tests for the failure-monitor decision logic — pure, no network/env."""

from src.monitor.run_monitor import decide


def R(lcs, rs=None):
    return {"state": {"life_cycle_state": lcs, "result_state": rs}}


def test_ok_when_latest_success():
    assert decide([R("TERMINATED", "SUCCESS")]) == "ok"


def test_retry_on_fresh_failure():
    assert decide([R("TERMINATED", "FAILED"), R("TERMINATED", "SUCCESS")]) == "retry"


def test_escalate_after_retry_exhausted():
    assert decide([R("TERMINATED", "FAILED"), R("INTERNAL_ERROR")]) == "escalate"


def test_ok_when_run_in_flight():
    assert decide([R("RUNNING"), R("TERMINATED", "FAILED")]) == "ok"


def test_canceled_does_not_count_as_failure():
    assert decide([R("TERMINATED", "CANCELED"), R("TERMINATED", "SUCCESS")]) == "ok"


def test_internal_error_is_a_failure():
    assert decide([R("INTERNAL_ERROR")]) == "retry"


def test_higher_retry_limit_delays_escalation():
    runs = [R("TERMINATED", "FAILED"), R("TERMINATED", "FAILED")]
    assert decide(runs, retry_limit=1) == "escalate"
    assert decide(runs, retry_limit=2) == "retry"
