from __future__ import annotations

from enron.domain.email_payload import (
    AttachmentPayload,
    MessagePayload,
    MessageRecipientPayload,
    MessageReferencePayload,
    ParsedEmailPayload,
    ValidationResult,
)


class EmailValidationService:
    """
    Service de validation d'un ParsedEmailPayload.

    Objectifs :
    - détecter les incohérences bloquantes
    - remonter des warnings utiles
    - rester simple et lisible
    """

    def validate(self, payload: ParsedEmailPayload) -> ValidationResult:
        errors: list[str] = []
        warnings: list[str] = []

        self._validate_parse_status(payload.message, errors)
        self._validate_required_context(payload, errors)
        self._validate_message_fields(payload.message, warnings)
        self._validate_recipients(payload.message.recipients, errors, warnings)
        self._validate_references(payload.message.references, errors, warnings)
        self._validate_attachments(payload.message, errors, warnings)
        self._validate_thread_fields(payload.message, errors, warnings)

        return ValidationResult(
            is_valid=not errors,
            errors=errors,
            warnings=warnings,
        )

    def _validate_parse_status(
        self,
        message: MessagePayload,
        errors: list[str],
    ) -> None:
        if not message.parse_ok:
            detail = f" Détail: {message.parse_error}" if message.parse_error else ""
            errors.append(f"Le parsing a échoué.{detail}")

    def _validate_required_context(
        self,
        payload: ParsedEmailPayload,
        errors: list[str],
    ) -> None:
        occurrence = payload.occurrence
        folder = occurrence.folder

        if not occurrence.source_file:
            errors.append("occurrence.source_file est requis.")

        if not folder.mailbox_key:
            errors.append("occurrence.folder.mailbox_key est requis.")

        if not folder.folder_path:
            errors.append("occurrence.folder.folder_path est requis.")

    def _validate_message_fields(
        self,
        message: MessagePayload,
        warnings: list[str],
    ) -> None:
        if not message.sender or not message.sender.email:
            warnings.append("message.sender.email est manquant.")

        if not message.message_id:
            warnings.append("message.message_id est manquant.")

        if not message.sent_at:
            warnings.append("message.sent_at est manquant.")

        if not message.subject_normalized:
            warnings.append("message.subject_normalized est manquant.")

        if not message.body_clean:
            warnings.append("message.body_clean est vide ou manquant.")

        if message.in_reply_to and not message.is_response:
            warnings.append(
                "message.in_reply_to est présent mais message.is_response vaut False."
            )

    def _validate_recipients(
        self,
        recipients: list[MessageRecipientPayload],
        errors: list[str],
        warnings: list[str],
    ) -> None:
        allowed_types = {"to", "cc", "bcc"}
        seen_email_keys: set[tuple[str, str]] = set()

        for index, recipient in enumerate(recipients):
            if recipient.recipient_type not in allowed_types:
                errors.append(
                    f"message.recipients[{index}].recipient_type invalide : "
                    f"{recipient.recipient_type}"
                )

            email_value = recipient.email_address.email if recipient.email_address else None

            if recipient.display_name is None and email_value is None:
                errors.append(
                    f"message.recipients[{index}] ne contient ni display_name ni email."
                )

            if email_value:
                key = (recipient.recipient_type, email_value)
                if key in seen_email_keys:
                    warnings.append(
                        f"message.recipients[{index}] dupliqué pour "
                        f"{recipient.recipient_type}:{email_value}"
                    )
                else:
                    seen_email_keys.add(key)

        if not recipients:
            warnings.append("Aucun destinataire extrait.")

    def _validate_references(
        self,
        references: list[MessageReferencePayload],
        errors: list[str],
        warnings: list[str],
    ) -> None:
        seen: set[str] = set()

        for index, reference in enumerate(references):
            ref = reference.referenced_message_id

            if ref is None:
                errors.append(
                    f"message.references[{index}].referenced_message_id est manquant."
                )
                continue

            if ref in seen:
                warnings.append(f"message.references[{index}] dupliquée : {ref}")
            else:
                seen.add(ref)

    def _validate_attachments(
        self,
        message: MessagePayload,
        errors: list[str],
        warnings: list[str],
    ) -> None:
        attachments = message.attachments

        if message.attachment_count != len(attachments):
            errors.append(
                "message.attachment_count est incohérent avec le nombre réel d'attachments."
            )

        if message.has_attachments != bool(attachments):
            errors.append(
                "message.has_attachments est incohérent avec la liste attachments."
            )

        for index, attachment in enumerate(attachments):
            self._validate_attachment(index, attachment, errors, warnings)

    def _validate_attachment(
        self,
        index: int,
        attachment: AttachmentPayload,
        errors: list[str],
        warnings: list[str],
    ) -> None:
        if attachment.size_bytes is not None and attachment.size_bytes < 0:
            errors.append(
                f"message.attachments[{index}].size_bytes ne peut pas être négatif."
            )

        if (
            attachment.filename is None
            and attachment.mime_type is None
            and attachment.content_id is None
        ):
            warnings.append(
                f"message.attachments[{index}] ne contient ni filename, "
                f"ni mime_type, ni content_id."
            )

    def _validate_thread_fields(
        self,
        message: MessagePayload,
        errors: list[str],
        warnings: list[str],
    ) -> None:
        if message.references_depth < 0:
            errors.append("message.references_depth ne peut pas être négatif.")

        if message.quoted_line_count < 0:
            errors.append("message.quoted_line_count ne peut pas être négatif.")

        if message.response_to_message_id and not message.is_response:
            warnings.append(
                "message.response_to_message_id est présent mais is_response vaut False."
            )

        if (
            message.response_to_message_id_source is not None
            and message.response_to_message_id is None
        ):
            warnings.append(
                "message.response_to_message_id_source est renseigné sans "
                "response_to_message_id."
            )

        if len(message.references) != message.references_depth:
            warnings.append(
                "message.references_depth diffère du nombre de références extraites."
            )

        if message.in_reply_to and message.response_to_message_id:
            if message.in_reply_to != message.response_to_message_id:
                warnings.append(
                    "message.in_reply_to diffère de message.response_to_message_id."
                )