from django.db import models

from .base import TimeStampedModel
from .message import Message
from .mailbox import Mailbox
from .folder import Folder

class MessageOccurrence(TimeStampedModel):
    message = models.ForeignKey(
        Message,
        on_delete=models.CASCADE,
        related_name="occurrences",
    )
    mailbox = models.ForeignKey(
        Mailbox,
        on_delete=models.CASCADE,
        related_name="message_occurrences",
    )
    folder = models.ForeignKey(
        Folder,
        on_delete=models.CASCADE,
        related_name="message_occurrences",
    )
    source_file = models.CharField(max_length=1024, unique=True)
    validation_is_valid = models.BooleanField(default=True)
    validation_errors = models.JSONField(default=list, blank=True)
    validation_warnings = models.JSONField(default=list, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["mailbox", "folder"]),
            models.Index(fields=["message"]),
            models.Index(fields=["mailbox", "message"]),
        ]