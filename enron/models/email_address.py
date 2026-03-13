from django.db import models

from .base import TimeStampedModel
from .collaborator import Collaborator

from django.db import models

from .base import TimeStampedModel
from .collaborator import Collaborator


class EmailAddress(TimeStampedModel):
    email = models.EmailField(unique=True, db_index=True)
    local_part = models.CharField(max_length=255, db_index=True)
    domain = models.CharField(max_length=255, db_index=True)

    display_name = models.CharField(max_length=255, null=True, blank=True)

    collaborator = models.ForeignKey(
        Collaborator,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="email_addresses",
    )

    class Meta:
        ordering = ["email"]
        indexes = [
            models.Index(fields=["email"]),
            models.Index(fields=["domain"]),
            models.Index(fields=["local_part"]),
            models.Index(fields=["collaborator"]),
            models.Index(fields=["domain", "local_part"]),
        ]

    def __str__(self) -> str:
        return self.email