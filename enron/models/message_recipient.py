from django.db import models

from .base import TimeStampedModel
from .message import Message
from .email_address import EmailAddress

from django.db import models

class MessageRecipient(TimeStampedModel):
    RECIPIENT_TYPES = [
        ("to", "To"),
        ("cc", "Cc"),
        ("bcc", "Bcc"),
    ]

    message = models.ForeignKey(
        Message,
        on_delete=models.CASCADE,
        related_name="recipients",
    )
    email_address = models.ForeignKey(
        EmailAddress,
        on_delete=models.CASCADE,
        related_name="received_message_links",
    )
    recipient_type = models.CharField(max_length=10, choices=RECIPIENT_TYPES)
    display_name = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        ordering = ["message", "recipient_type", "email_address", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["message", "email_address", "recipient_type"],
                name="uq_message_recipient_per_type",
            )
        ]
        indexes = [
            models.Index(fields=["message", "recipient_type"]),
            models.Index(fields=["email_address"]),
        ]

    def __str__(self) -> str:
        return f"{self.message_id}:{self.recipient_type}:{self.email_address_id}"