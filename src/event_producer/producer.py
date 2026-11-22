
import json
import logging

from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import AzureCliCredential

NAMESPACE = "evhbns-fraud-dev.servicebus.windows.net"
EVENT_HUB_NAME = "transactions"

logger = logging.getLogger(__name__)


class TransactionProducer:

    def __init__(self):

        self.credential = AzureCliCredential()

        self.producer = EventHubProducerClient(
            fully_qualified_namespace=NAMESPACE,
            eventhub_name=EVENT_HUB_NAME,
            credential=self.credential
        )

        logger.info("Event Hubs producer initialized")

    def send_transaction(self, transaction):

        batch = self.producer.create_batch(
            partition_key=transaction.customer_id
        )

        event = EventData(transaction.model_dump_json())
        batch.add(event)

        self.producer.send_batch(batch)

        logger.info(
            "Transaction sent: transaction_id=%s customer_id=%s",
            transaction.transaction_id,
            transaction.customer_id
        )

    def send_batch(self, transactions):

        customer_groups = {}

        for transaction in transactions:
            customer_groups.setdefault(
                transaction.customer_id, []
            ).append(transaction)

        total_sent = 0

        for customer_id, customer_transactions in customer_groups.items():

            batch = self.producer.create_batch(
                partition_key=customer_id
            )

            for transaction in customer_transactions:

                event = EventData(
                    transaction.model_dump_json()
                )

                try:
                    batch.add(event)

                except ValueError:
                    # Send the full batch
                    self.producer.send_batch(batch)

                    total_sent += len(batch)

                    logger.info(
                        "Batch sent for customer %s",
                        customer_id
                    )

                    # Start a new batch for the same customer
                    batch = self.producer.create_batch(
                        partition_key=customer_id
                    )

                    # Add the event that did not fit
                    batch.add(event)

            if len(batch) > 0:
                batch_count = len(batch)
                self.producer.send_batch(batch)
                total_sent += batch_count

        logger.info(
            "Batch publishing completed. Total events sent: %d",
            total_sent
        )

        return total_sent

    def close(self):

        try:
            self.producer.close()
            logger.info("Event Hubs producer closed")

        finally:
            self.credential.close()
