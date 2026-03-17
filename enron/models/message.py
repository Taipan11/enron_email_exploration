from django.db import models
from django.db.models.functions import Coalesce, Left
from django.contrib.postgres.indexes import GinIndex
from django.contrib.postgres.search import SearchVector

from .base import TimeStampedModel
from .email_address import EmailAddress


class Message(TimeStampedModel):
    message_id = models.CharField(max_length=500, unique=True, db_index=True)
    sender_email = models.CharField(max_length=320, null=True, blank=True, db_index=True)
    sender = models.ForeignKey(
        EmailAddress,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="sent_messages",
    )
    sent_at = models.DateTimeField(null=True, blank=True, db_index=True)

    in_reply_to = models.CharField(max_length=500, null=True, blank=True, db_index=True)
    subject_normalized = models.TextField(null=True, blank=True)

    body_clean = models.TextField(null=True, blank=True)
    signature = models.TextField(null=True, blank=True)

    mime_type = models.CharField(max_length=255, null=True, blank=True)
    content_type_header = models.CharField(max_length=500, null=True, blank=True)

    has_attachments = models.BooleanField(default=False)
    attachment_count = models.PositiveIntegerField(default=0)

    parse_ok = models.BooleanField(default=True)
    parse_error = models.TextField(null=True, blank=True)

    is_response = models.BooleanField(default=False)
    is_forward = models.BooleanField(default=False)
    response_to_message_id = models.CharField(max_length=500, null=True, blank=True)
    response_to_message_id_source = models.CharField(max_length=100, null=True, blank=True)
    thread_root_message_id = models.CharField(max_length=500, null=True, blank=True, db_index=True)
    references_depth = models.PositiveIntegerField(default=0)
    quoted_line_count = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ["-sent_at", "message_id"]
        indexes = [
            models.Index(fields=["sent_at"]),
            models.Index(fields=["sender_email"]),
            models.Index(fields=["thread_root_message_id"]),
            models.Index(fields=["in_reply_to"]),
            GinIndex(
                SearchVector(
                    Coalesce(
                        "subject_normalized",
                        models.Value("", output_field=models.TextField()),
                        output_field=models.TextField(),
                    ),
                    Coalesce(
                        Left("body_clean", 200000),
                        models.Value("", output_field=models.TextField()),
                        output_field=models.TextField(),
                    ),
                    config="english",
                ),
                name="message_fts_gin_idx",
            ),
        ]