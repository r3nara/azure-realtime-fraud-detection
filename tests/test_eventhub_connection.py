
import json
from datetime import datetime, timezone

from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import AzureCliCredential

NAMESPACE = "evhbns-fraud-dev.servicebus.windows.net"
EVENT_HUB = "transactions"


def main():
    credential = AzureCliCredential()

    try:
        producer = EventHubProducerClient(
            fully_qualified_namespace=NAMESPACE,
            eventhub_name=EVENT_HUB,
            credential=credential
        )

        with producer:
            batch = producer.create_batch()

            test_event = {
                "event_type": "connectivity_test",
                "transaction_id": "PHASE1-CONNECTIVITY-001",
                "message": "Python successfully connected to Azure Event Hubs",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            batch.add(EventData(json.dumps(test_event)))
            producer.send_batch(batch)

        print("SUCCESS: Event sent to Azure Event Hubs!")
        print("Event Hub:", EVENT_HUB)
        print("Transaction ID:", test_event["transaction_id"])

    finally:
        credential.close()


if __name__ == "__main__":
    main()
