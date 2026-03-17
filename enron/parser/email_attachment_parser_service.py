from __future__ import annotations

import re
from email import policy
from email.message import Message
from email.parser import BytesParser

from enron.domain.email_payload import AttachmentPayload
from enron.normalization.email_normalization_service import (
    EmailNormalizationService,
)


class EmailAttachmentParserService:
    """
    Service dédié à l'extraction des pièces jointes.

    Stratégie:
    - Méthode principale: parser le message MIME complet avec le module standard `email`
    - Fallback 1: détection depuis les headers du raw email
    - Fallback 2: heuristique dans le contenu texte
    """

    _FILENAME_RE = re.compile(r'filename\*?="?([^";]+)"?', re.IGNORECASE)
    _NAME_RE = re.compile(r'name\*?="?([^";]+)"?', re.IGNORECASE)

    _HEADER_ATTACHMENT_HINTS = (
        "attachment",
        "inline",
        "filename=",
        "filename*=",
        "name=",
        "name*=",
        "x-attachment-id",
        "content-id",
    )

    _TEXT_ATTACHMENT_HINTS = (
        "attached",
        "attachment",
        "attachments",
        "pièce jointe",
        "pièces jointes",
        "pj",
        "ci-joint",
        "voir en pièce jointe",
        "find attached",
        "attached file",
        "attached files",
        "see attached",
        "please find attached",
        "enclosed",
    )

    _FILE_EXTENSION_RE = re.compile(
        r'\b([a-zA-Z0-9_\-() ]{1,200}\.(?:pdf|png|jpg|jpeg|gif|bmp|tif|tiff|csv|xls|xlsx|doc|docx|ppt|pptx|txt|rtf|zip|7z|tar|gz|xml|json|eml))\b',
        re.IGNORECASE,
    )

    def __init__(self, normalizer: EmailNormalizationService) -> None:
        self.normalizer = normalizer

    def extract_attachments_from_raw_email(
        self,
        raw_email: str | bytes,
    ) -> list[AttachmentPayload]:
        if not raw_email:
            return []

        message = self._parse_message(raw_email)
        if message is None:
            return []

        attachments: list[AttachmentPayload] = []

        for part in message.walk():
            if part.is_multipart():
                continue

            attachment = self._extract_attachment_from_part(part)
            if attachment is not None:
                attachments.append(attachment)

        return self._deduplicate_attachments(attachments)

    def extract_attachment_candidates_from_content(
        self,
        raw_email: str | bytes,
    ) -> list[AttachmentPayload]:
        if not raw_email:
            return []

        message = self._parse_message(raw_email)

        if message is not None:
            content_text = self._extract_text_content(message)
        else:
            content_text = self._decode_bytes_fallback(self._to_bytes(raw_email))

        normalized_text = self.normalizer.normalize_text(content_text)
        if not normalized_text:
            return []

        attachments: list[AttachmentPayload] = []
        found_filenames: set[str] = set()

        for match in self._FILE_EXTENSION_RE.finditer(normalized_text):
            filename = self.normalizer.normalize_text(match.group(1))
            if not filename:
                continue

            key = filename.lower()
            if key in found_filenames:
                continue

            found_filenames.add(key)
            attachments.append(
                AttachmentPayload(
                    filename=filename,
                    mime_type=self._guess_mime_type_from_filename(filename),
                    content_id=None,
                    size_bytes=None,
                    sha256=None,
                    storage_path=None,
                )
            )

        if attachments:
            return self._deduplicate_attachments(attachments)

        if self._contains_attachment_hint(normalized_text):
            return [
                AttachmentPayload(
                    filename=None,
                    mime_type=None,
                    content_id=None,
                    size_bytes=None,
                    sha256=None,
                    storage_path=None,
                )
            ]

        return []

    def extract_all_attachment_candidates(
        self,
        raw_email: str | bytes,
    ) -> list[AttachmentPayload]:
        attachments: list[AttachmentPayload] = []

        attachments.extend(self.extract_attachments_from_raw_email(raw_email))
        attachments.extend(self.extract_attachment_candidates_from_content(raw_email))

        return self._deduplicate_attachments(attachments)

    def has_attachment_headers_in_raw_email(
        self,
        raw_email: str | bytes,
    ) -> bool:
        message = self._parse_message(raw_email)
        if message is None:
            return False

        values = []

        for header_name in (
            "Content-Disposition",
            "Content-Type",
            "Content-ID",
            "X-Attachment-Id",
            "X-FileName",
        ):
            value = message.get(header_name)
            if value:
                values.append(str(value).lower())

        merged = " ".join(values)
        return any(hint in merged for hint in self._HEADER_ATTACHMENT_HINTS)

    def has_attachment_mention_in_content(
        self,
        raw_email: str | bytes,
    ) -> bool:
        candidates = self.extract_attachment_candidates_from_content(raw_email)
        return len(candidates) > 0

    def _parse_message(self, raw_email: str | bytes) -> Message | None:
        try:
            raw_bytes = self._to_bytes(raw_email)
            return BytesParser(policy=policy.default).parsebytes(raw_bytes)
        except Exception:
            return None

    def _extract_attachment_from_part(self, part: Message) -> AttachmentPayload | None:
        content_disposition = part.get_content_disposition()
        filename = part.get_filename()
        content_type = self.normalizer.normalize_text(part.get_content_type())
        content_id = self.normalizer.normalize_text(part.get("Content-ID"))

        is_attachment = (
            content_disposition == "attachment"
            or (filename is not None and content_disposition != "inline")
            or (
                filename is not None
                and content_disposition is None
                and content_type is not None
                and (
                    content_type.startswith("application/")
                    or content_type.startswith("image/")
                    or content_type.startswith("audio/")
                    or content_type.startswith("video/")
                )
            )
        )

        if not is_attachment:
            return None

        size_bytes = self._extract_payload_size(part)

        return AttachmentPayload(
            filename=self.normalizer.normalize_text(filename),
            mime_type=content_type,
            content_id=content_id,
            size_bytes=size_bytes,
            sha256=None,
            storage_path=None,
        )

    def _extract_text_content(self, message: Message) -> str:
        text_parts: list[str] = []

        for part in message.walk():
            if part.is_multipart():
                continue

            content_type = (part.get_content_type() or "").lower()
            if content_type not in {"text/plain", "text/html"}:
                continue

            try:
                payload = part.get_payload(decode=True)
            except Exception:
                payload = None

            if payload is None:
                continue

            charset = part.get_content_charset() or "utf-8"

            try:
                text = payload.decode(charset, errors="replace")
            except Exception:
                text = payload.decode("utf-8", errors="replace")

            if content_type == "text/html":
                text = self._strip_html(text)

            if text:
                text_parts.append(text)

        return "\n".join(text_parts)

    def _contains_attachment_hint(self, text: str) -> bool:
        lowered = text.lower()
        return any(hint in lowered for hint in self._TEXT_ATTACHMENT_HINTS)

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

    def _guess_mime_type_from_filename(self, filename: str | None) -> str | None:
        if not filename:
            return None

        lower_name = filename.lower()

        if lower_name.endswith(".pdf"):
            return "application/pdf"
        if lower_name.endswith(".png"):
            return "image/png"
        if lower_name.endswith(".jpg") or lower_name.endswith(".jpeg"):
            return "image/jpeg"
        if lower_name.endswith(".gif"):
            return "image/gif"
        if lower_name.endswith(".csv"):
            return "text/csv"
        if lower_name.endswith(".xls"):
            return "application/vnd.ms-excel"
        if lower_name.endswith(".xlsx"):
            return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        if lower_name.endswith(".doc"):
            return "application/msword"
        if lower_name.endswith(".docx"):
            return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        if lower_name.endswith(".ppt"):
            return "application/vnd.ms-powerpoint"
        if lower_name.endswith(".pptx"):
            return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        if lower_name.endswith(".txt"):
            return "text/plain"
        if lower_name.endswith(".zip"):
            return "application/zip"
        if lower_name.endswith(".xml"):
            return "application/xml"
        if lower_name.endswith(".json"):
            return "application/json"
        if lower_name.endswith(".eml"):
            return "message/rfc822"

        return None

    def _strip_html(self, html: str) -> str:
        if not html:
            return ""

        text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html)
        text = re.sub(r"(?s)<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _deduplicate_attachments(
        self,
        attachments: list[AttachmentPayload],
    ) -> list[AttachmentPayload]:
        deduplicated: list[AttachmentPayload] = []
        seen: set[tuple[str | None, str | None, str | None]] = set()

        for attachment in attachments:
            key = (
                (attachment.filename or "").lower() or None,
                (attachment.mime_type or "").lower() or None,
                (attachment.content_id or "").lower() or None,
            )

            if key in seen:
                continue

            seen.add(key)
            deduplicated.append(attachment)

        return deduplicated

    def _to_bytes(self, raw_email: str | bytes) -> bytes:
        if isinstance(raw_email, bytes):
            return raw_email
        return raw_email.encode("utf-8", errors="replace")

    def _decode_bytes_fallback(self, raw_bytes: bytes) -> str:
        try:
            return raw_bytes.decode("utf-8", errors="replace")
        except Exception:
            return ""

    def _extract_payload_size(self, part: Message) -> int | None:
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