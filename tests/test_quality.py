"""Pruebas sobre la logica de severidad/agregacion de quality.py (sin I/O de Delta)."""

from saas_pipeline.quality import CheckResult, QualityRunResult


def test_check_passed_when_no_failures():
    r = CheckResult("uniqueness", "critical", records_checked=100, records_failed=0)
    assert r.passed is True


def test_check_failed_when_records_failed_positive():
    r = CheckResult("uniqueness", "critical", records_checked=100, records_failed=3)
    assert r.passed is False


def test_any_critical_failed_true_when_a_critical_check_fails():
    run = QualityRunResult(
        results=[
            CheckResult("check_a", "warning", 10, 5),
            CheckResult("check_b", "critical", 10, 1),
        ]
    )
    assert run.any_critical_failed is True


def test_any_critical_failed_false_when_only_warnings_fail():
    run = QualityRunResult(
        results=[
            CheckResult("check_a", "warning", 10, 5),
            CheckResult("check_b", "info", 10, 2),
            CheckResult("check_c", "critical", 10, 0),
        ]
    )
    assert run.any_critical_failed is False


def test_any_critical_failed_false_when_no_results():
    run = QualityRunResult(results=[])
    assert run.any_critical_failed is False
