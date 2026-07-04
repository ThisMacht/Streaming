"""Kafka producer specialized for CPG events."""

from confluent_kafka import KafkaError, Message, Producer

from src.common.logging_utils import get_logger
from src.common.schemas import EdgeEvent, ErrorEvent, MetadataEvent, NodeEvent, to_json_bytes

logger = get_logger(__name__)


def _delivery_report(error: KafkaError | None, message: Message) -> None:
    """Log asynchronous Kafka delivery failures."""
    if error is not None:
        logger.error(
            "Kafka delivery failed: topic=%s key=%r error=%s",
            message.topic(),
            message.key(),
            error,
        )


class CpgKafkaProducer:
    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})

    def _send(
        self, topic: str, key: str, event: NodeEvent | EdgeEvent | MetadataEvent | ErrorEvent
    ) -> None:
        # poll(0) serves queued delivery callbacks and prevents queue growth.
        self._producer.produce(
            topic,
            key=key.encode(),
            value=to_json_bytes(event),
            on_delivery=_delivery_report,
        )
        self._producer.poll(0)

    def send_node(self, topic: str, event: NodeEvent) -> None:
        self._send(topic, event.node_id, event)

    def send_edge(self, topic: str, event: EdgeEvent) -> None:
        self._send(topic, event.edge_id, event)

    def send_metadata(self, topic: str, event: MetadataEvent) -> None:
        self._send(topic, event.metadata_id, event)

    def send_error(self, topic: str, event: ErrorEvent) -> None:
        self._send(topic, f"{event.repo_name}:{event.file_path}", event)

    def flush(self) -> None:
        remaining = self._producer.flush()
        if remaining:
            raise RuntimeError(f"Failed to deliver {remaining} Kafka message(s)")
