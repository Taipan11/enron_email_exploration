from django.db import models

from .base import TimeStampedModel
from .collaborator import Collaborator


class Mailbox(TimeStampedModel):
    mailbox_key = models.CharField(
        max_length=255,
        unique=True,
        db_index=True,
    )
    owner = models.ForeignKey(
        Collaborator,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="mailboxes",
    )
    source_root_path = models.CharField(
        max_length=1024,
        null=True,
        blank=True,
        help_text="Chemin racine du maildir source, ex: /app/data/enron/maildir/allen-p",
    )

    class Meta:
        ordering = ["mailbox_key"]
        indexes = [
            models.Index(fields=["mailbox_key"]),
            models.Index(fields=["owner"]),
        ]

    def __str__(self) -> str:
        return self.mailbox_key