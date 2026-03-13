from __future__ import annotations

import re
from email import policy
from email.parser import BytesParser

from enron.domain.email_payload import AttachmentPayload
from enron.services.normalization.email_normalization_service import (
    EmailNormalizationService,
)
from enron.services.parser.email_header_parser import ParsedHeaderBlock


class EmailAttachmentParserService:
    """
    Service dédié à l'extraction des pièces jointes.

    Stratégie:
    - Méthode principale: parser le message MIME complet avec le module standard `email`.
    - Méthode fallback: détection heuristique depuis un bloc de headers.
    """

    _FILENAME_RE = re.compile(r'filename\*?="?([^";]+)"?', re.IGNORECASE)
    _NAME_RE = re.compile(r'name\*?="?([^";]+)"?', re.IGNORECASE)

    def __init__(self, normalizer: EmailNormalizationService) -> None:
        self.normalizer = normalizer

    def extract_attachments_from_raw_email(
        self,
        raw_email: str | bytes,
    ) -> list[AttachmentPayload]:
        """
        Extraction robuste des pièces jointes depuis le message MIME complet.

        Règles:
        - On ignore les conteneurs multipart.
        - On considère comme pièce jointe:
          * Content-Disposition == "attachment"
          * ou filename présent, sauf si disposition == "inline"
        - X-FileName n'est pas interprété comme une pièce jointe.
        """
        if not raw_email:
            return []

        try:
            raw_bytes = self._to_bytes(raw_email)
            message = BytesParser(policy=policy.default).parsebytes(raw_bytes)
        except Exception:
            return []

        attachments: list[AttachmentPayload] = []

        for part in message.walk():
            if part.is_multipart():
                continue

            content_disposition = part.get_content_disposition()  # attachment / inline / None
            filename = part.get_filename()
            content_type = self.normalizer.normalize_text(part.get_content_type())
            content_id = self.normalizer.normalize_text(part.get("Content-ID"))

            is_attachment = (
                content_disposition == "attachment"
                or (filename is not None and content_disposition != "inline")
            )
            if not is_attachment:
                continue

            size_bytes = self._extract_payload_size(part)

            attachments.append(
                AttachmentPayload(
                    filename=self.normalizer.normalize_text(filename),
                    mime_type=content_type,
                    content_id=content_id,
                    size_bytes=size_bytes,
                    sha256=None,
                    storage_path=None,
                )
            )

        return attachments

    def extract_attachments(self, headers: ParsedHeaderBlock) -> list[AttachmentPayload]:
        """
        Fallback simple depuis un bloc de headers.

        Limites:
        - Ne remplace pas un parsing MIME complet.
        - Utile seulement si on n'a pas accès au raw email complet.
        """
        content_type = self.normalizer.normalize_text(headers.get_first("Content-Type"))
        content_id = self.normalizer.normalize_text(headers.get_first("Content-ID"))
        content_disposition = self.normalizer.normalize_text(
            headers.get_first("Content-Disposition")
        )

        filename = (
            self._extract_filename(content_disposition)
            or self._extract_name_from_content_type(content_type)
        )

        disposition_lower = (content_disposition or "").lower()

        is_attachment = (
            "attachment" in disposition_lower
            or filename is not None
        )

        if not is_attachment:
            return []

        return [
            AttachmentPayload(
                filename=filename,
                mime_type=content_type,
                content_id=content_id,
                size_bytes=None,
                sha256=None,
                storage_path=None,
            )
        ]

    def _extract_filename(self, content_disposition: str | None) -> str | None:
        if not content_disposition:
            return None

        match = self._FILENAME_RE.search(content_disposition)
        if not match:
            return None

        return self.normalizer.normalize_text(match.group(1))

    def _extract_name_from_content_type(self, content_type: str | None) -> str | None:
        if not content_type:
            return None

        match = self._NAME_RE.search(content_type)
        if not match:
            return None

        return self.normalizer.normalize_text(match.group(1))

    def _to_bytes(self, raw_email: str | bytes) -> bytes:
        if isinstance(raw_email, bytes):
            return raw_email

        return raw_email.encode("utf-8", errors="replace")

    def _extract_payload_size(self, part: object) -> int | None:
        try:
            payload = part.get_payload(decode=True)
        except Exception:
            return None

        if payload is None:
            return None

        try:
            return len(payload)
        except Exception:
            return None