from __future__ import annotations

from datetime import datetime, timezone
import hashlib

from enron.normalization.email_normalization_service import EmailNormalizationService


class EmailHashingService:
    """
    Construit des empreintes déterministes pour :
    - le contenu principal d'un email
    - l'identité canonique complète d'un message

    Règle métier :
    - canonical_hash représente un message strictement identique
    - si recipients, references, attachments, body, sujet, etc. diffèrent,
      le hash doit différer aussi
    """

    def __init__(self, normalizer: EmailNormalizationService) -> None:
        self.normalizer = normalizer

    def sha256_text(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _canonicalize_text(self, value: str | None) -> str:
        if not value:
            return ""
        return " ".join(value.lower().split())

    def _canonicalize_datetime(self, value: datetime | None) -> str:
        if value is None:
            return ""

        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc)

        value = value.replace(microsecond=0)
        return value.isoformat()

    def _canonicalize_recipient(
        self,
        *,
        recipient_type: str | None,
        email: str | None,
    ) -> str:
        recipient_type_norm = self._canonicalize_text(recipient_type)
        email_norm = self.normalizer.normalize_email_address(email) or ""
        return f"{recipient_type_norm}:{email_norm}"

    def _canonicalize_reference(self, referenced_message_id: str | None) -> str:
        return self.normalizer.normalize_message_id(referenced_message_id) or ""

    def _canonicalize_attachment(
        self,
        *,
        filename: str | None,
        mime_type: str | None,
        size_bytes: int | None,
        sha256: str | None,
    ) -> str:
        return "|".join([
            self._canonicalize_text(filename),
            self._canonicalize_text(mime_type),
            str(size_bytes or 0),
            self._canonicalize_text(sha256),
        ])

    def _sorted_join(self, items: list[str]) -> str:
        cleaned = [item for item in items if item]
        cleaned.sort()
        return "\n".join(cleaned)

    def _ordered_join(self, items: list[str]) -> str:
        return "\n".join([item for item in items if item])

    def build_content_hash(
        self,
        *,
        sender_email: str | None,
        subject_normalized: str | None,
        body_clean: str | None,
    ) -> str:
        payload = "\n".join([
            self.normalizer.normalize_email_address(sender_email) or "",
            self._canonicalize_text(subject_normalized),
            self._canonicalize_text(body_clean),
        ])
        return self.sha256_text(payload)

    def build_canonical_hash(
        self,
        *,
        sender_email: str | None,
        sent_at: datetime | None,
        subject_normalized: str | None,
        body_clean: str | None,
        recipients: list[dict] | None = None,
        references: list[str] | None = None,
        attachments: list[dict] | None = None,
    ) -> str:
        recipient_items = [
            self._canonicalize_recipient(
                recipient_type=item.get("recipient_type"),
                email=item.get("email"),
            )
            for item in (recipients or [])
        ]

        reference_items = [
            self._canonicalize_reference(item)
            for item in (references or [])
            if self._canonicalize_reference(item)
        ]

        attachment_items = [
            self._canonicalize_attachment(
                filename=item.get("filename"),
                mime_type=item.get("mime_type"),
                size_bytes=item.get("size_bytes"),
                sha256=item.get("sha256"),
            )
            for item in (attachments or [])
        ]

        payload = "\n\n".join([
            f"sender={self.normalizer.normalize_email_address(sender_email) or ''}",
            f"sent_at={self._canonicalize_datetime(sent_at)}",
            f"subject={self._canonicalize_text(subject_normalized)}",
            f"body={self._canonicalize_text(body_clean)}",
            f"recipients=\n{self._sorted_join(recipient_items)}",
            f"references=\n{self._ordered_join(reference_items)}",
            f"attachments=\n{self._sorted_join(attachment_items)}",
        ])

        return self.sha256_text(payload)