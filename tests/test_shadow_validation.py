from decimal import Decimal

from validation.shadow_validation import compare


def rows(**values):
    return {("202604", "26000"): {m: Decimal(str(v)) for m, v in values.items()}}


BASE = dict(qtd_lancamentos=10, valor_empenhado=1000, valor_liquidado=800, valor_pago=500)


def test_identical_sides_are_approved():
    result = compare(rows(**BASE), rows(**BASE))
    assert result.passed(0.995)
    assert result.coverage == 1.0
    assert all(value == 1.0 for value in result.adherence.values())


def test_small_divergence_within_threshold_is_approved():
    new = dict(BASE, valor_pago=Decimal("499.00"))  # 0.2% off
    result = compare(rows(**BASE), rows(**new))
    assert result.adherence["valor_pago"] == 0.998
    assert result.passed(0.995)


def test_divergence_above_threshold_is_rejected_and_reported():
    new = dict(BASE, valor_pago=450)  # 10% off
    result = compare(rows(**BASE), rows(**new))
    assert not result.passed(0.995)
    (key, metric, old, cur) = result.top_divergences[0]
    assert key == ("202604", "26000") and metric == "valor_pago"
    assert (old, cur) == (Decimal(500), Decimal(450))


def test_missing_keys_reduce_coverage():
    legacy = {**rows(**BASE), ("202604", "36000"): {m: Decimal(1) for m in BASE}}
    result = compare(legacy, rows(**BASE))
    assert result.coverage == 0.5
    assert result.only_legacy == [("202604", "36000")]
    assert not result.passed(0.995)
