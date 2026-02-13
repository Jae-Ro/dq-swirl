from dataclasses import dataclass


@dataclass(slots=True)
class ChatTaskPayload:
    """Payload for chat tasks"""

    user_id: str
    conversation_id: str
    model: str
    prompt: str
    pubsub_stream_id: str


@dataclass(slots=True)
class CleanupTaskPayload:
    """Payload for maintenance tasks"""

    older_than_days: int
