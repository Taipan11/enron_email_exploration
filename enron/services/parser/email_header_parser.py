from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from email.utils import getaddresses, parseaddr
import re

from enron.domain.email_payload import (
    AttachmentPayload,
    EmailAddressPayload,
    MessageRecipientPayload,
    MessageReferencePayload,
)
from enron.services.normalization.email_normalization_service import (
    EmailNormalizationService,
)


@dataclass(slots=True)
class ParsedHeaderBlock:
    raw_items: list[tuple[str, str]]
    by_lower_name: dict[str, list[str]]

    def get_first(self, name: str) -> str | None:
        values = self.by_lower_name.get(name.lower(), [])
        return values[0] if values else None

    def get_all(self, name: str) -> list[str]:
        return list(self.by_lower_name.get(name.lower(), []))


class EmailHeaderParserService:
    """
    Tout ce qui concerne les headers doit vivre ici.
    """

    _HEADER_LINE_RE = re.compile(r"^([!-9;-~]+):(.*)$")
    _LINE_SPLIT_RE = re.compile(r"\r\n|\n|\r")
    _FILENAME_RE = re.compile(r'filename\*?="?([^";]+)"?', re.IGNORECASE)

    def __init__(self, normalizer: EmailNormalizationService) -> None:
        self.normalizer = normalizer

    def parse_header_block(self, raw_headers: str | None) -> ParsedHeaderBlock:
        if not raw_headers:
            return ParsedHeaderBlock(raw_items=[], by_lower_name={})

        lines = self._LINE_SPLIT_RE.split(raw_headers)

        unfolded: list[str] = []
        current: str | None = None

        for line in lines:
            if line[:1] in (" ", "\t"):
                if current is not None:
                    current += " " + line.strip()
                continue

            if current is not None:
                unfolded.append(current)

            current = line.strip()

        if current is not None:
            unfolded.append(current)

        raw_items: list[tuple[str, str]] = []
        by_lower_name: dict[str, list[str]] = {}

        for line in unfolded:
            match = self._HEADER_LINE_RE.match(line)
            if not match:
                continue

            name = match.group(1).strip()
            value = match.group(2).strip()

            raw_items.append((name, value))
            by_lower_name.setdefault(name.lower(), []).append(value)

        return ParsedHeaderBlock(raw_items=raw_items, by_lower_name=by_lower_name)

    def extract_sender_email(self, headers: ParsedHeaderBlock) -> str | None:
        from_value = headers.get_first("From")
        if not from_value:
            return None

        display_name, email = parseaddr(from_value)
        normalized_email = self.normalizer.normalize_email_address(email or from_value)
        if not normalized_email:
            return None

        return normalized_email

    def extract_sender(self, headers: ParsedHeaderBlock) -> EmailAddressPayload:
        from_value = headers.get_first("From")
        if not from_value:
            return EmailAddressPayload()

        display_name, email = parseaddr(from_value)
        return EmailAddressPayload(
            email=self.normalizer.normalize_email_address(email or from_value),
            display_name=self.normalizer.normalize_text(display_name),
        )

    def extract_sent_at(self, headers: ParsedHeaderBlock) -> datetime | None:
        return self.normalizer.parse_email_date(headers.get_first("Date"))

    def extract_message_id(self, headers: ParsedHeaderBlock) -> str | None:
        return self.normalizer.normalize_message_id(headers.get_first("Message-ID"))

    def extract_in_reply_to(self, headers: ParsedHeaderBlock) -> str | None:
        return self.normalizer.normalize_message_id(headers.get_first("In-Reply-To"))

    def extract_subject_normalized(self, headers: ParsedHeaderBlock) -> str | None:
        return self.normalizer.normalize_subject(headers.get_first("Subject"))

    def extract_content_type_header(self, headers: ParsedHeaderBlock) -> str | None:
        return self.normalizer.normalize_text(headers.get_first("Content-Type"))

    def extract_recipients(self, headers: ParsedHeaderBlock) -> list[MessageRecipientPayload]:
        recipients: list[MessageRecipientPayload] = []

        for header_name in ("To", "Cc", "Bcc"):
            header_value = headers.get_first(header_name)
            if not header_value:
                continue

            recipient_type = header_name.lower()
            parsed_addresses = getaddresses([header_value])

            for display_name, email in parsed_addresses:
                normalized_email = self.normalizer.normalize_email_address(email)
                if not normalized_email:
                    continue

                recipients.append(
                    MessageRecipientPayload(
                        recipient_type=recipient_type,
                        display_name=self.normalizer.normalize_text(display_name),
                        email_address=EmailAddressPayload(
                            email=normalized_email,
                            display_name=self.normalizer.normalize_text(display_name),
                        ),
                    )
                )

        return recipients

    def extract_references(self, headers: ParsedHeaderBlock) -> list[MessageReferencePayload]:
        results: list[MessageReferencePayload] = []

        for value in headers.get_all("References"):
            for ref in self.normalizer.parse_references_header(value):
                results.append(
                    MessageReferencePayload(
                        referenced_message_id=ref,
                    )
                )

        return results

    def extract_attachments(self, headers: ParsedHeaderBlock) -> list[AttachmentPayload]:
        """
        Extraction simple depuis les headers.
        Ici on ne parse pas le MIME complet.
        On expose seulement une base minimale si des infos existent dans les headers.
        """
        content_type = self.normalizer.normalize_text(headers.get_first("Content-Type"))
        content_id = self.normalizer.normalize_text(headers.get_first("Content-ID"))
        content_disposition = self.normalizer.normalize_text(
            headers.get_first("Content-Disposition")
        )

        if not content_disposition and not content_id:
            return []

        filename = self._extract_filename(content_disposition)

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
    
    