
import json

from src.transaction_generator.generator import generate_batch

transactions = generate_batch(10, seed=42)

for transaction in transactions:

    print(
        json.dumps(
            transaction.model_dump(mode="json"),
            indent=2
        )
    )
