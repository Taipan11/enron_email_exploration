from django.db import models

from .base import TimeStampedModel
from .mailbox import Mailbox


class Folder(TimeStampedModel):
    mailbox = models.ForeignKey(
        Mailbox,
        on_delete=models.CASCADE,
        related_name="folders",
    )
    folder_path = models.CharField(max_length=500)
    folder_name = models.CharField(max_length=255)
    folder_type = models.CharField(max_length=50, null=True, blank=True)
    folder_topic = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        ordering = ["mailbox", "folder_path"]
        constraints = [
            models.UniqueConstraint(
                fields=["mailbox", "folder_path"],
                name="uq_folder_mailbox_path",
            )
        ]
        indexes = [
            models.Index(fields=["mailbox", "folder_type"]),
            models.Index(fields=["mailbox", "folder_name"]),
            models.Index(fields=["mailbox", "folder_path"]),
        ]

    def __str__(self) -> str:
        return f"{self.mailbox_id}:{self.folder_path}"