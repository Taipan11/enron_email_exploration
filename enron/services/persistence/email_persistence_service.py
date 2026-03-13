from __future__ import annotations

from dataclasses import dataclass

from django.db import transaction

from enron.domain.email_payload import ParsedEmailPayload, ValidationResult
from enron.models.attachment import Attachment
from enron.models.email_address import EmailAddress
from enron.models.folder import Folder
from enron.models.mailbox import Mailbox
from enron.models.message import Message
from enron.models.message_occurrence import MessageOccurrence
from enron.models.message_recipient import MessageRecipient
from enron.models.message_reference import MessageReference


@dataclass(slots=True)
class PersistenceResult:
    message_id: int | None = None
    occurrence_id: int | None = None
    created: int = 0
    updated: int = 0


class EmailPersistenceService:
    """
    Persistance relationnelle d'un ParsedEmailPayload.
    """

    @transaction.atomic
    def save(
        self,
        parsed_email: ParsedEmailPayload,
        validation_result: ValidationResult | None = None,
    ) -> PersistenceResult:
        result = PersistenceResult()

        mailbox = self._get_or_create_mailbox(parsed_email)
        folder = self._get_or_create_folder(parsed_email, mailbox)
        sender = self._get_or_create_sender_email_address(parsed_email)

        message, message_created = self._save_message(parsed_email, sender)
        if message_created:
            result.created += 1
        else:
            result.updated += 1

        self._replace_recipients(message, parsed_email)
        self._replace_references(message, parsed_email)
        self._replace_attachments(message, parsed_email)

        occurrence, occurrence_created = self._save_occurrence(
            parsed_email=parsed_email,
            message=message,
            folder=folder,
            mailbox=mailbox,
            validation_result=validation_result,
        )
        if occurrence_created:
            result.created += 1
        else:
            result.updated += 1

        result.message_id = message.pk
        result.occurrence_id = occurrence.pk
        return result

    def _get_or_create_mailbox(self, parsed_email: ParsedEmailPayload) -> Mailbox:
        folder_payload = parsed_email.occurrence.folder
        mailbox_key = folder_payload.mailbox_key

        mailbox, _ = Mailbox.objects.get_or_create(
            mailbox_key=mailbox_key,
            defaults={},
        )
        return mailbox

    def _get_or_create_folder(
        self,
        parsed_email: ParsedEmailPayload,
        mailbox: Mailbox,
    ) -> Folder:
        folder_payload = parsed_email.occurrence.folder

        folder, created = Folder.objects.get_or_create(
            mailbox=mailbox,
            folder_path=folder_payload.folder_path,
            defaults={
                "folder_name": folder_payload.folder_name or folder_payload.folder_path,
                "folder_type": folder_payload.folder_type,
                "folder_topic": folder_payload.folder_topic,
            },
        )

        has_changes = False

        if folder.folder_name != (folder_payload.folder_name or folder.folder_name):
            folder.folder_name = folder_payload.folder_name or folder.folder_name
            has_changes = True

        if folder.folder_type != folder_payload.folder_type:
            folder.folder_type = folder_payload.folder_type
            has_changes = True

        if folder.folder_topic != folder_payload.folder_topic:
            folder.folder_topic = folder_payload.folder_topic
            has_changes = True

        if has_changes and not created:
            folder.save(update_fields=["folder_name", "folder_type", "folder_topic", "updated_at"])

        return folder

    def _get_or_create_sender_email_address(
        self,
        parsed_email: ParsedEmailPayload,
    ) -> EmailAddress | None:
        sender_payload = parsed_email.message.sender
        if not sender_payload or not sender_payload.email:
            return None

        email_address, created = EmailAddress.objects.get_or_create(
            email=sender_payload.email,
            defaults={
                "local_part": sender_payload.local_part or "",
                "domain": sender_payload.domain or "",
                "display_name": sender_payload.display_name,
            },
        )

        has_changes = False

        if not email_address.local_part and sender_payload.local_part:
            email_address.local_part = sender_payload.local_part
            has_changes = True

        if not email_address.domain and sender_payload.domain:
            email_address.domain = sender_payload.domain
            has_changes = True

        if not email_address.display_name and sender_payload.display_name:
            email_address.display_name = sender_payload.display_name
            has_changes = True

        if has_changes and not created:
            email_address.save(update_fields=["local_part", "domain", "display_name", "updated_at"])

        return email_address

    def _save_message(
        self,
        parsed_email: ParsedEmailPayload,
        sender: EmailAddress | None,
    ) -> tuple[Message, bool]:
        payload = parsed_email.message

        defaults = {
            "sender": sender,
            "sender_email": payload.sender.email if payload.sender else None,
            "sent_at": payload.sent_at,
            "in_reply_to": payload.in_reply_to,
            "subject_normalized": payload.subject_normalized,
            "body_clean": payload.body_clean,
            "signature": payload.signature,
            "mime_type": payload.mime_type,
            "content_type_header": payload.content_type_header,
            "has_attachments": payload.has_attachments,
            "attachment_count": payload.attachment_count,
            "parse_ok": payload.parse_ok,
            "parse_error": payload.parse_error,
            "is_response": payload.is_response,
            "is_forward": payload.is_forward,
            "response_to_message_id": payload.response_to_message_id,
            "response_to_message_id_source": payload.response_to_message_id_source,
            "thread_root_message_id": payload.thread_root_message_id,
            "references_depth": payload.references_depth,
            "quoted_line_count": payload.quoted_line_count,
        }

        message, created = Message.objects.update_or_create(
            message_id=payload.message_id,
            defaults=defaults,
        )
        return message, created

    def _replace_recipients(
        self,
        message: Message,
        parsed_email: ParsedEmailPayload,
    ) -> None:
        MessageRecipient.objects.filter(message=message).delete()

        for recipient_payload in parsed_email.message.recipients:
            if not recipient_payload.email_address or not recipient_payload.email_address.email:
                continue

            email_address = self._get_or_create_email_address(
                recipient_payload.email_address
            )

            MessageRecipient.objects.create(
                message=message,
                email_address=email_address,
                recipient_type=recipient_payload.recipient_type,
                display_name=recipient_payload.display_name,
            )

    def _replace_references(
        self,
        message: Message,
        parsed_email: ParsedEmailPayload,
    ) -> None:
        MessageReference.objects.filter(message=message).delete()

        for reference_payload in parsed_email.message.references:
            if not reference_payload.referenced_message_id:
                continue

            MessageReference.objects.create(
                message=message,
                referenced_message_id=reference_payload.referenced_message_id,
            )

    def _replace_attachments(
        self,
        message: Message,
        parsed_email: ParsedEmailPayload,
    ) -> None:
        Attachment.objects.filter(message=message).delete()

        for attachment_payload in parsed_email.message.attachments:
            Attachment.objects.create(
                message=message,
                filename=attachment_payload.filename,
                mime_type=attachment_payload.mime_type,
                content_id=attachment_payload.content_id,
                size_bytes=attachment_payload.size_bytes,
                storage_path=attachment_payload.storage_path,
                sha256=attachment_payload.sha256,
            )

    def _save_occurrence(
        self,
        *,
        parsed_email: ParsedEmailPayload,
        message: Message,
        mailbox: Mailbox,
        folder: Folder,
        validation_result: ValidationResult | None,
    ) -> tuple[MessageOccurrence, bool]:
        defaults = {
            "message": message,
            "mailbox": mailbox,
            "folder": folder,
        }

        if validation_result is not None:
            defaults["validation_is_valid"] = validation_result.is_valid
            defaults["validation_errors"] = list(validation_result.errors)
            defaults["validation_warnings"] = list(validation_result.warnings)

        occurrence, created = MessageOccurrence.objects.update_or_create(
            source_file=parsed_email.occurrence.source_file,
            defaults=defaults,
        )
        return occurrence, created

    def _get_or_create_email_address(self, payload) -> EmailAddress:
        email_address, created = EmailAddress.objects.get_or_create(
            email=payload.email,
            defaults={
                "local_part": payload.local_part or "",
                "domain": payload.domain or "",
                "display_name": payload.display_name,
            },
        )

        has_changes = False

        if not email_address.local_part and payload.local_part:
            email_address.local_part = payload.local_part
            has_changes = True

        if not email_address.domain and payload.domain:
            email_address.domain = payload.domain
            has_changes = True

        if not email_address.display_name and payload.display_name:
            email_address.display_name = payload.display_name
            has_changes = True

        if has_changes and not created:
            email_address.save(update_fields=["local_part", "domain", "display_name", "updated_at"])

        return email_address