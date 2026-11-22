
import json
import logging
import threading

from json import JSONDecodeError

from pydantic import ValidationError

from azure.eventhub import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient

from src.common.transaction_schema import Transaction


# Event Hubs configuration
NAMESPACE = "evhbns-fraud-dev.servicebus.windows.net"
EVENT_HUB_NAME = "transactions"
CONSUMER_GROUP = "cg-fraud-detection"

# Storage configuration
STORAGE_ACCOUNT_URL = "https://stfrauddev2026.blob.core.windows.net"
CHECKPOINT_CONTAINER = "eventhub-checkpoints"
DATA_CONTAINER = "fraud-data"


logger = logging.getLogger(__name__)


class TransactionConsumer:

    def __init__(self):

        self.credential = AzureCliCredential()
        self._closed = False

        self.checkpoint_store = BlobCheckpointStore(
            blob_account_url=STORAGE_ACCOUNT_URL,
            container_name=CHECKPOINT_CONTAINER,
            credential=self.credential
        )

        self.blob_service_client = BlobServiceClient(
            account_url=STORAGE_ACCOUNT_URL,
            credential=self.credential
        )

        self.quarantine_container = (
            self.blob_service_client.get_container_client(
                DATA_CONTAINER
            )
        )

        self.consumer = EventHubConsumerClient(
            fully_qualified_namespace=NAMESPACE,
            eventhub_name=EVENT_HUB_NAME,
            consumer_group=CONSUMER_GROUP,
            credential=self.credential,
            checkpoint_store=self.checkpoint_store
        )

        logger.info("Checkpoint-enabled consumer initialized")

    def quarantine_event(
        self,
        partition_context,
        event,
        raw_body,
        reason
    ):

        blob_name = (
            f"quarantine/eventhub/"
            f"partition-{partition_context.partition_id}/"
            f"sequence-{event.sequence_number}.json"
        )

        quarantine_record = {
            "partition_id": partition_context.partition_id,
            "sequence_number": event.sequence_number,
            "offset": event.offset,
            "reason": str(reason),
            "raw_event": raw_body
        }

        blob_client = self.quarantine_container.get_blob_client(
            blob_name
        )

        blob_client.upload_blob(
            json.dumps(quarantine_record, default=str, indent=2),
            overwrite=True
        )

        logger.warning(
            "Event quarantined: partition=%s sequence=%s blob=%s",
            partition_context.partition_id,
            event.sequence_number,
            blob_name
        )

    def receive_events(self, max_events=10):

        if max_events < 1:
            raise ValueError("max_events must be greater than zero")

        received_count = 0
        lock = threading.Lock()
        stop_event = threading.Event()
        receiver_errors = []

        def on_event(partition_context, event):

            nonlocal received_count

            if event is None:
                return

            with lock:

                if stop_event.is_set():
                    return

                try:

                    raw_body = event.body_as_str(
                        encoding="UTF-8"
                    )

                    try:
                        event_data = json.loads(raw_body)

                        transaction = Transaction.model_validate(
                            event_data
                        )

                        # Placeholder for fraud detection.
                        logger.info(
                            "Validated transaction_id=%s customer_id=%s amount=%s",
                            transaction.transaction_id,
                            transaction.customer_id,
                            transaction.amount
                        )

                        result = "PROCESSED"

                    except (JSONDecodeError, ValidationError) as exc:

                        # Invalid or non-transaction event.
                        # Save it before advancing the checkpoint.
                        self.quarantine_event(
                            partition_context,
                            event,
                            raw_body,
                            exc
                        )

                        result = "QUARANTINED"

                    # Advance checkpoint only after successful
                    # processing or successful quarantine.
                    partition_context.update_checkpoint(event)

                    received_count += 1

                    print(
                        f"{result} {received_count}/{max_events} | "
                        f"Partition: {partition_context.partition_id} | "
                        f"Sequence: {event.sequence_number}"
                    )

                    if received_count >= max_events:
                        stop_event.set()

                except Exception as exc:

                    logger.exception(
                        "Event handling failed. "
                        "Checkpoint was not advanced."
                    )

                    receiver_errors.append(exc)
                    stop_event.set()

        def receive_worker():

            try:

                self.consumer.receive(
                    on_event=on_event,
                    starting_position="-1",
                    max_wait_time=5
                )

            except Exception as exc:

                receiver_errors.append(exc)
                stop_event.set()

        receiver_thread = threading.Thread(
            target=receive_worker,
            name="eventhub-receiver",
            daemon=True
        )

        try:

            print("Starting checkpoint-enabled consumer")
            print(f"Consumer group: {CONSUMER_GROUP}")
            print(f"Checkpoint container: {CHECKPOINT_CONTAINER}")
            print(f"Quarantine path: {DATA_CONTAINER}/quarantine")
            print(f"Maximum events: {max_events}")
            print("-" * 60)

            receiver_thread.start()

            # Wait until the requested count is reached,
            # a fatal error occurs, or the user interrupts.
            stop_event.wait()

        except KeyboardInterrupt:

            print("\nConsumer interrupted by user")

        finally:

            # First stop Event Hubs receiving.
            self.consumer.close()

            # Wait until the receive thread has finished.
            if receiver_thread.is_alive():
                receiver_thread.join()

            # Only now close storage and credentials.
            self.close_storage()

        print("-" * 60)
        print(f"Total events handled: {received_count}")

        if receiver_errors:
            raise receiver_errors[0]

        return received_count

    def close_storage(self):

        if self._closed:
            return

        self._closed = True

        try:
            self.checkpoint_store.close()

        finally:
            try:
                self.blob_service_client.close()

            finally:
                self.credential.close()

        logger.info("Checkpoint and storage resources closed")
