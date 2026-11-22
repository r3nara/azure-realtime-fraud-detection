
import random
import time

from src.transaction_generator.generator import generate_transaction


def generate_continuously(
    rate_per_second=2,
    max_events=None,
    seed=None
):
    # Validate immediately when the function is called
    if rate_per_second <= 0:
        raise ValueError("Rate must be greater than zero")

    if max_events is not None and max_events < 1:
        raise ValueError("max_events must be positive or None")

    # Inner generator handles transaction generation
    def _stream():
        rng = random.Random(seed)

        interval = 1.0 / rate_per_second
        next_emit = time.monotonic()
        count = 0

        while max_events is None or count < max_events:

            now = time.monotonic()

            if now < next_emit:
                time.sleep(next_emit - now)

            transaction = generate_transaction(rng)

            yield transaction

            count += 1
            next_emit += interval

            # Avoid catch-up bursts if execution falls behind
            if time.monotonic() > next_emit + interval:
                next_emit = time.monotonic() + interval

    return _stream()
