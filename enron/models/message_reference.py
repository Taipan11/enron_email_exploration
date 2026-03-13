from django.db import models

from .base import TimeStampedModel
from .message import Message


class MessageReference(TimeStampedModel):
    message = models.ForeignKey(
        Message,
        on_delete=models.CASCADE,
        related_name="outgoing_references",
    )
    referenced_message_id = models.CharField(max_length=500, db_index=True)

    class Meta:
        ordering = ["message", "referenced_message_id", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["message", "referenced_message_id"],
                name="uq_message_reference",
            )
        ]
        indexes = [
            models.Index(fields=["message"]),
            models.Index(fields=["referenced_message_id"]),
        ]

    def __str__(self) -> str:
        return f"{self.message_id} -> {self.referenced_message_id}"