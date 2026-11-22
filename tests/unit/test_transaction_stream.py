
import pytest

from src.transaction_generator.stream import generate_continuously


def test_continuous_generator_count():
    transactions = list(
        generate_continuously(
            rate_per_second=100,
            max_events=10,
            seed=42
        )
    )

    assert len(transactions) == 10


def test_continuous_generator_unique_ids():
    transactions = list(
        generate_continuously(
            rate_per_second=100,
            max_events=20,
            seed=42
        )
    )

    ids = [t.transaction_id for t in transactions]

    assert len(ids) == len(set(ids))


def test_invalid_rate_rejected():
    with pytest.raises(ValueError):
        generate_continuously(rate_per_second=0)


def test_invalid_max_events_rejected():
    with pytest.raises(ValueError):
        generate_continuously(max_events=0)
