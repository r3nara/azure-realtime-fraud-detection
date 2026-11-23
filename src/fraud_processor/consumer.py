
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
from src.fraud_processor.durable_store import DurableFraudStore
from src.fraud_processor.stateful_processor import StatefulFraudProcessor


# ============================================================
# Event Hubs Configuration
# ============================================================

NAMESPACE = "evhbns-fraud-dev.servicebus.windows.net"
EVENT_HUB_NAME = "transactions"
CONSUMER_GROUP = "cg-fraud-detection"


# ============================================================
# Azure Storage Configuration
# ============================================================

STORAGE_ACCOUNT_URL = "https://stfrauddev2026.blob.core.windows.net"

CHECKPOINT_CONTAINER = "eventhub-checkpoints"
DATA_CONTAINER = "fraud-data"


logger = logging.getLogger(__name__)


class TransactionConsumer:

    def __init__(self):

        self.credential = AzureCliCredential()
        self._closed = False

        # Checkpoint storage
        self.checkpoint_store = BlobCheckpointStore(
            blob_account_url=STORAGE_ACCOUNT_URL,
            container_name=CHECKPOINT_CONTAINER,
            credential=self.credential
        )

        # Blob Storage client
        self.blob_service_client = BlobServiceClient(
            account_url=STORAGE_ACCOUNT_URL,
            credential=self.credential
        )

        self.quarantine_container = (
            self.blob_service_client.get_container_client(
                DATA_CONTAINER
            )
        )

        # Durable fraud storage
        self.durable_store = DurableFraudStore(
            blob_service_client=self.blob_service_client,
            container_name=DATA_CONTAINER,
            history_window_minutes=60
        )

        # Stateful fraud processor
        self.fraud_processor = StatefulFraudProcessor(
            durable_store=self.durable_store,
            history_window_minutes=60
        )

        # Event Hubs consumer
        self.consumer = EventHubConsumerClient(
            fully_qualified_namespace=NAMESPACE,
            eventhub_name=EVENT_HUB_NAME,
            consumer_group=CONSUMER_GROUP,
            credential=self.credential,
            checkpoint_store=self.checkpoint_store
        )

        logger.info(
            "Durable fraud detection consumer initialized"
        )

    # ========================================================
    # Quarantine Invalid Events
    # ========================================================

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
            json.dumps(
                quarantine_record,
                default=str,
                indent=2
            ),
            overwrite=True
        )

        logger.warning(
            "Event quarantined: partition=%s sequence=%s",
            partition_context.partition_id,
            event.sequence_number
        )

    # ========================================================
    # Receive Events
    # ========================================================

    def receive_events(
        self,
        target_transaction_ids=None,
        max_events=1000,
        timeout_seconds=180
    ):

        if max_events < 1:
            raise ValueError(
                "max_events must be greater than zero"
            )

        if timeout_seconds < 1:
            raise ValueError(
                "timeout_seconds must be greater than zero"
            )

        targets = set(target_transaction_ids or [])

        if not targets:
            raise ValueError(
                "Provide at least one target transaction ID"
            )

        received_count = 0
        matched_ids = set()

        lock = threading.Lock()
        stop_event = threading.Event()
        receiver_errors = []

        # ----------------------------------------------------
        # Event callback
        # ----------------------------------------------------

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

                        # Process transaction using durable processor
                        fraud_result = (
                            self.fraud_processor.process_transaction(
                                transaction
                            )
                        )

                        transaction_id = transaction.transaction_id

                        if fraud_result["status"] == "DUPLICATE":

                            logger.warning(
                                "Duplicate transaction: %s",
                                transaction_id
                            )

                            result = "DUPLICATE"

                        else:

                            logger.info(
                                "FRAUD RESULT | "
                                "transaction_id=%s | "
                                "customer_id=%s | "
                                "amount=%s | "
                                "risk_score=%s | "
                                "risk_level=%s | "
                                "triggered_rules=%s",
                                fraud_result["transaction_id"],
                                fraud_result["customer_id"],
                                transaction.amount,
                                fraud_result["risk_score"],
                                fraud_result["risk_level"],
                                fraud_result["triggered_rules"]
                            )

                            result = fraud_result["risk_level"]

                        # Track only the requested transaction IDs
                        if transaction_id in targets:
                            matched_ids.add(transaction_id)

                            print(
                                f"TARGET FOUND: {transaction_id}"
                            )

                    except (
                        JSONDecodeError,
                        ValidationError
                    ) as exc:

                        self.quarantine_event(
                            partition_context,
                            event,
                            raw_body,
                            exc
                        )

                        result = "QUARANTINED"

                    # Checkpoint only after successful processing
                    # or successful quarantine
                    partition_context.update_checkpoint(event)

                    received_count += 1

                    print(
                        f"{result} | "
                        f"Events handled: {received_count} | "
                        f"Partition: {partition_context.partition_id} | "
                        f"Sequence: {event.sequence_number} | "
                        f"Targets found: {len(matched_ids)}/{len(targets)}"
                    )

                    # Stop when every target has been found
                    if matched_ids == targets:
                        stop_event.set()

                    # Safety limit
                    elif received_count >= max_events:
                        logger.warning(
                            "Maximum event limit reached"
                        )
                        stop_event.set()

                except Exception as exc:

                    logger.exception(
                        "Event processing failed. "
                        "Checkpoint was not advanced."
                    )

                    receiver_errors.append(exc)
                    stop_event.set()

        # ----------------------------------------------------
        # Event Hubs receiving worker
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # Start receiving
        # ----------------------------------------------------

        try:

            print("Starting targeted fraud detection consumer")
            print(f"Namespace: {NAMESPACE}")
            print(f"Event Hub: {EVENT_HUB_NAME}")
            print(f"Consumer group: {CONSUMER_GROUP}")
            print(f"Target transactions: {len(targets)}")
            print(f"Maximum events: {max_events}")
            print(f"Timeout: {timeout_seconds} seconds")
            print("-" * 60)

            receiver_thread.start()

            # Wait for targets, safety limit, or timeout
            stop_event.wait(timeout=timeout_seconds)

        except KeyboardInterrupt:

            print("\nConsumer interrupted by user")

        finally:

            # Stop receiving
            self.consumer.close()

            # Wait for receiver thread
            if receiver_thread.is_alive():
                receiver_thread.join()

            # Close Azure resources
            self.close_storage()

        missing_ids = targets - matched_ids

        print("-" * 60)
        print(f"Total events handled: {received_count}")
        print(f"Target transactions found: {len(matched_ids)}/{len(targets)}")

        if missing_ids:
            print("Transactions not yet found:")
            for transaction_id in sorted(missing_ids):
                print(f"  {transaction_id}")

        if receiver_errors:
            raise receiver_errors[0]

        return {
            "received_count": received_count,
            "matched_ids": matched_ids,
            "missing_ids": missing_ids
        }

    # ========================================================
    # Resource Cleanup
    # ========================================================

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

        logger.info(
            "Checkpoint and storage resources closed"
        )
