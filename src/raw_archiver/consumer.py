
import argparse
import json
import logging
import threading
from datetime import datetime, timezone

from azure.eventhub import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from azure.identity import AzureCliCredential
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient


# ============================================================
# Event Hubs configuration
# ============================================================

NAMESPACE = "evhbns-fraud-dev.servicebus.windows.net"
EVENT_HUB_NAME = "transactions"
CONSUMER_GROUP = "cg-raw-archive"


# ============================================================
# Azure Storage configuration
# ============================================================

STORAGE_ACCOUNT_URL = "https://stfrauddev2026.blob.core.windows.net"

CHECKPOINT_CONTAINER = "eventhub-checkpoints"
DATA_CONTAINER = "fraud-data"

BRONZE_PREFIX = "raw/transactions"


logger = logging.getLogger(__name__)


class RawArchiveConsumer:

    def __init__(self):

        self.credential = AzureCliCredential()
        self._closed = False

        # ----------------------------------------------------
        # Checkpoint store
        # ----------------------------------------------------

        self.checkpoint_store = BlobCheckpointStore(
            blob_account_url=STORAGE_ACCOUNT_URL,
            container_name=CHECKPOINT_CONTAINER,
            credential=self.credential
        )

        # ----------------------------------------------------
        # ADLS / Blob Storage client
        # ----------------------------------------------------

        self.blob_service_client = BlobServiceClient(
            account_url=STORAGE_ACCOUNT_URL,
            credential=self.credential
        )

        self.container_client = (
            self.blob_service_client.get_container_client(
                DATA_CONTAINER
            )
        )

        # ----------------------------------------------------
        # Event Hubs consumer
        # ----------------------------------------------------

        self.consumer = EventHubConsumerClient(
            fully_qualified_namespace=NAMESPACE,
            eventhub_name=EVENT_HUB_NAME,
            consumer_group=CONSUMER_GROUP,
            credential=self.credential,
            checkpoint_store=self.checkpoint_store
        )

        logger.info("Raw archive consumer initialized.")

    # ========================================================
    # Build deterministic Bronze file path
    # ========================================================

    @staticmethod
    def build_blob_name(partition_id, sequence_number):

        return (
            f"{BRONZE_PREFIX}/"
            f"partition-{partition_id}/"
            f"sequence-{sequence_number}.json"
        )

    # ========================================================
    # Archive one original Event Hubs event
    # ========================================================

    def archive_event(self, partition_context, event):

        raw_body = event.body_as_str(encoding="UTF-8")

        partition_id = partition_context.partition_id
        sequence_number = event.sequence_number

        blob_name = self.build_blob_name(
            partition_id,
            sequence_number
        )

        enqueued_time = event.enqueued_time

        if enqueued_time is not None:
            enqueued_time = enqueued_time.isoformat()

        archive_record = {
            "source": "azure_event_hubs",
            "namespace": NAMESPACE,
            "event_hub": EVENT_HUB_NAME,
            "consumer_group": CONSUMER_GROUP,
            "partition_id": partition_id,
            "sequence_number": sequence_number,
            "offset": str(event.offset),
            "enqueued_time": enqueued_time,
            "archived_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "raw_event": raw_body
        }

        blob_client = self.container_client.get_blob_client(
            blob_name
        )

        # ----------------------------------------------------
        # Idempotent archive write
        # ----------------------------------------------------

        try:

            blob_client.upload_blob(
                json.dumps(
                    archive_record,
                    ensure_ascii=False,
                    indent=2
                ),
                overwrite=False
            )

            archive_status = "ARCHIVED"

        except ResourceExistsError:

            # The deterministic file already exists.
            # Treat this as an already archived event.

            archive_status = "ALREADY_ARCHIVED"

        logger.info(
            "%s | partition=%s | sequence=%s | blob=%s",
            archive_status,
            partition_id,
            sequence_number,
            blob_name
        )

        return archive_status

    # ========================================================
    # Receive and archive a controlled number of events
    # ========================================================

    def receive_events(
        self,
        max_events=5,
        timeout_seconds=120
    ):

        if max_events < 1:
            raise ValueError("max_events must be greater than zero.")

        if timeout_seconds < 1:
            raise ValueError("timeout_seconds must be greater than zero.")

        received_count = 0
        archived_count = 0
        duplicate_count = 0

        lock = threading.Lock()
        stop_event = threading.Event()
        receiver_errors = []

        # ----------------------------------------------------
        # Event callback
        # ----------------------------------------------------

        def on_event(partition_context, event):

            nonlocal received_count
            nonlocal archived_count
            nonlocal duplicate_count

            if event is None:
                return

            with lock:

                if stop_event.is_set():
                    return

                try:

                    archive_status = self.archive_event(
                        partition_context,
                        event
                    )

                    # Checkpoint only after a successful write
                    # or confirmation that the file already exists.
                    partition_context.update_checkpoint(event)

                    received_count += 1

                    if archive_status == "ARCHIVED":
                        archived_count += 1
                    else:
                        duplicate_count += 1

                    print(
                        f"{archive_status} | "
                        f"Partition: {partition_context.partition_id} | "
                        f"Sequence: {event.sequence_number} | "
                        f"Events handled: {received_count}/{max_events}"
                    )

                    if received_count >= max_events:
                        stop_event.set()

                except Exception as exc:

                    logger.exception(
                        "Archival failed. "
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

                if not stop_event.is_set():
                    receiver_errors.append(exc)

                stop_event.set()

        receiver_thread = threading.Thread(
            target=receive_worker,
            name="raw-archive-receiver",
            daemon=True
        )

        try:

            print("\n========== BRONZE RAW ARCHIVER ==========")
            print(f"Namespace: {NAMESPACE}")
            print(f"Event Hub: {EVENT_HUB_NAME}")
            print(f"Consumer group: {CONSUMER_GROUP}")
            print(f"Storage account: {STORAGE_ACCOUNT_URL}")
            print(f"Container: {DATA_CONTAINER}")
            print(f"Bronze path: {BRONZE_PREFIX}")
            print(f"Maximum events: {max_events}")
            print(f"Timeout: {timeout_seconds} seconds")
            print("-" * 60)

            receiver_thread.start()

            completed = stop_event.wait(
                timeout=timeout_seconds
            )

            if not completed:
                print("\nReceive timeout reached.")

        except KeyboardInterrupt:

            print("\nArchive consumer interrupted.")

        finally:

            stop_event.set()

            self.consumer.close()

            if receiver_thread.is_alive():
                receiver_thread.join(timeout=15)

            self.close()

        print("\n========== ARCHIVE SUMMARY ==========")
        print(f"Events handled: {received_count}")
        print(f"New files archived: {archived_count}")
        print(f"Already archived: {duplicate_count}")

        if receiver_errors:
            raise receiver_errors[0]

        return {
            "received_count": received_count,
            "archived_count": archived_count,
            "duplicate_count": duplicate_count
        }

    # ========================================================
    # Close Azure resources
    # ========================================================

    def close(self):

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

        logger.info("Raw archive Azure resources closed.")


# ============================================================
# Command-line entry point
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description="Archive Event Hubs transactions to ADLS Bronze."
    )

    parser.add_argument(
        "--max-events",
        type=int,
        default=5,
        help="Maximum number of events to archive."
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="Receive timeout in seconds."
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    archive_consumer = RawArchiveConsumer()

    archive_consumer.receive_events(
        max_events=args.max_events,
        timeout_seconds=args.timeout
    )


if __name__ == "__main__":
    main()
