from django.db import models

from .base import TimeStampedModel
from .message import Message


class Attachment(TimeStampedModel):
    message = models.ForeignKey(
        Message,
        on_delete=models.CASCADE,
        related_name="attachments",
    )
    filename = models.CharField(max_length=500, null=True, blank=True)
    mime_type = models.CharField(max_length=255, null=True, blank=True)
    content_id = models.CharField(max_length=255, null=True, blank=True)
    size_bytes = models.BigIntegerField(null=True, blank=True)
    storage_path = models.CharField(max_length=1024, null=True, blank=True)
    sha256 = models.CharField(max_length=64, null=True, blank=True, db_index=True)

    class Meta:
        ordering = ["message", "id"]
        indexes = [
            models.Index(fields=["message"]),
            models.Index(fields=["sha256"]),
        ]

    def __str__(self) -> str:
        return self.filename or self.sha256 or f"attachment:{self.pk}"