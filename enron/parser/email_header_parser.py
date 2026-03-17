from __future__ import annotations

from datetime import datetime
from email.utils import getaddresses, parseaddr
import re

from enron.domain.email_payload import (
    EmailAddressPayload,
    MessageRecipientPayload,
    MessageReferencePayload,
    ParsedMessageHeaders,
    ParsedHeaderMetadata
)
from enron.normalization.email_normalization_service import (
    EmailNormalizationService,
)


class EmailHeaderParserService:
    """
    Parser dédié uniquement aux headers.

    API publique:
    - parse(raw_headers) -> ParsedMessageHeaders

    Responsabilités:
    - parser un bloc brut de headers
    - extraire les champs métier liés aux headers
    - déduire quelques signaux simples de thread depuis les headers
    """

    _HEADER_LINE_RE = re.compile(r"^([!-9;-~]+):(.*)$")
    _LINE_SPLIT_RE = re.compile(r"\r\n|\n|\r")

    _RE_PREFIXES = ("re:", "aw:", "sv:")
    _FW_PREFIXES = ("fw:", "fwd:", "tr:")

    def __init__(self, normalizer: EmailNormalizationService) -> None:
        self.normalizer = normalizer

    def parse(self, raw_headers: str | None) -> ParsedMessageHeaders:
        header_map = self._parse_headers_to_map(raw_headers)

        sender = self._extract_sender(header_map)
        reply_to = self._extract_reply_to(header_map)
        sent_at = self._extract_sent_at(header_map)

        message_id = self._extract_message_id(header_map)
        in_reply_to = self._extract_in_reply_to(header_map)

        subject_raw = self._extract_subject_raw(header_map)
        subject_normalized = self._extract_subject_normalized(header_map)

        content_type_header = self._extract_content_type_header(header_map)
        mime_type = self._extract_mime_type(header_map)

        recipients = self._extract_recipients(header_map)
        references = self._extract_references(header_map)
        metadata = self._extract_metadata(header_map)

        looks_like_response = self._detect_looks_like_response(
            subject_raw=subject_raw,
            in_reply_to=in_reply_to,
            references=references,
        )
        is_response = self._detect_is_response(
            in_reply_to=in_reply_to,
            references=references,
        )
        is_forward = self._detect_is_forward(subject_raw=subject_raw)

        response_to_message_id, response_to_message_id_source = self._extract_response_target(
            in_reply_to=in_reply_to,
            references=references,
        )

        thread_root_message_id = self._extract_thread_root_message_id(
            in_reply_to=in_reply_to,
            references=references,
        )

        return ParsedMessageHeaders(
            sender=sender,
            reply_to=reply_to,
            sent_at=sent_at,
            message_id=message_id,
            in_reply_to=in_reply_to,
            subject_raw=subject_raw,
            subject_normalized=subject_normalized,
            mime_type=mime_type,
            content_type_header=content_type_header,
            recipients=recipients,
            references=references,
            is_response=is_response,
            looks_like_response=looks_like_response,
            is_forward=is_forward,
            response_to_message_id=response_to_message_id,
            response_to_message_id_source=response_to_message_id_source,
            thread_root_message_id=thread_root_message_id,
            references_depth=len(references),
            metadata=metadata,
        )

    def _parse_headers_to_map(self, raw_headers: str | None) -> dict[str, list[str]]:
        if not raw_headers:
            return {}

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

        by_lower_name: dict[str, list[str]] = {}

        for line in unfolded:
            match = self._HEADER_LINE_RE.match(line)
            if not match:
                continue

            name = match.group(1).strip().lower()
            value = match.group(2).strip()
            by_lower_name.setdefault(name, []).append(value)

        return by_lower_name

    def _get_first(self, headers: dict[str, list[str]], name: str) -> str | None:
        values = headers.get(name.lower(), [])
        return values[0] if values else None

    def _get_all(self, headers: dict[str, list[str]], name: str) -> list[str]:
        return list(headers.get(name.lower(), []))

    def _get_first_non_empty(self, headers: dict[str, list[str]], *names: str) -> str | None:
        for name in names:
            value = self._get_first(headers, name)
            if value and value.strip():
                return value
        return None

    def _extract_sender(self, headers: dict[str, list[str]]) -> EmailAddressPayload:
        from_value = self._get_first(headers, "From")
        x_from_value = self._get_first(headers, "X-From")

        preferred_value = from_value or x_from_value
        if not preferred_value:
            return EmailAddressPayload()

        display_name, email = parseaddr(preferred_value)
        normalized_email = self.normalizer.normalize_email_address(email or preferred_value)
        normalized_display_name = self.normalizer.normalize_text(display_name)

        if not normalized_display_name and x_from_value:
            x_display_name, _ = parseaddr(x_from_value)
            normalized_display_name = self.normalizer.normalize_text(x_display_name)

        return EmailAddressPayload(
            email=normalized_email,
            display_name=normalized_display_name,
        )

    def _extract_reply_to(self, headers: dict[str, list[str]]) -> EmailAddressPayload:
        reply_to_value = self._get_first(headers, "Reply-To")
        if not reply_to_value:
            return EmailAddressPayload()

        display_name, email = parseaddr(reply_to_value)
        return EmailAddressPayload(
            email=self.normalizer.normalize_email_address(email or reply_to_value),
            display_name=self.normalizer.normalize_text(display_name),
        )

    def _extract_sent_at(self, headers: dict[str, list[str]]) -> datetime | None:
        return self.normalizer.parse_email_date(self._get_first(headers, "Date"))

    def _extract_message_id(self, headers: dict[str, list[str]]) -> str | None:
        return self.normalizer.normalize_message_id(self._get_first(headers, "Message-ID"))

    def _extract_in_reply_to(self, headers: dict[str, list[str]]) -> str | None:
        return self.normalizer.normalize_message_id(self._get_first(headers, "In-Reply-To"))

    def _extract_subject_raw(self, headers: dict[str, list[str]]) -> str | None:
        return self.normalizer.normalize_text(self._get_first(headers, "Subject"))

    def _extract_subject_normalized(self, headers: dict[str, list[str]]) -> str | None:
        return self.normalizer.normalize_subject(self._get_first(headers, "Subject"))

    def _extract_content_type_header(self, headers: dict[str, list[str]]) -> str | None:
        return self.normalizer.normalize_text(self._get_first(headers, "Content-Type"))

    def _extract_mime_type(self, headers: dict[str, list[str]]) -> str | None:
        content_type = self._get_first(headers, "Content-Type")
        normalized = self.normalizer.normalize_text(content_type)
        if not normalized:
            return None

        mime_type = normalized.split(";", 1)[0].strip().lower()
        return mime_type or None

    def _extract_recipients(
        self,
        headers: dict[str, list[str]],
    ) -> list[MessageRecipientPayload]:
        recipients: list[MessageRecipientPayload] = []
        seen: set[tuple[str, str]] = set()

        for header_name in ("To", "Cc", "Bcc"):
            for header_value in self._get_all(headers, header_name):
                if not header_value:
                    continue

                for display_name, email in getaddresses([header_value]):
                    normalized_email = self.normalizer.normalize_email_address(email)
                    if not normalized_email:
                        continue

                    normalized_display_name = self.normalizer.normalize_text(display_name)
                    recipient_type = header_name.lower()
                    key = (recipient_type, normalized_email)

                    if key in seen:
                        continue
                    seen.add(key)

                    recipients.append(
                        MessageRecipientPayload(
                            recipient_type=recipient_type,
                            display_name=normalized_display_name,
                            email_address=EmailAddressPayload(
                                email=normalized_email,
                                display_name=normalized_display_name,
                            ),
                        )
                    )

        return recipients

    def _extract_references(
        self,
        headers: dict[str, list[str]],
    ) -> list[MessageReferencePayload]:
        results: list[MessageReferencePayload] = []
        seen: set[str] = set()

        for value in self._get_all(headers, "References"):
            for ref in self.normalizer.parse_references_header(value):
                if not ref or ref in seen:
                    continue

                seen.add(ref)
                results.append(
                    MessageReferencePayload(
                        referenced_message_id=ref,
                    )
                )

        return results

    def _extract_metadata(self, headers: dict[str, list[str]]) -> ParsedHeaderMetadata:
        return ParsedHeaderMetadata(
            x_from=self.normalizer.normalize_text(self._get_first(headers, "X-From")),
            x_to=self.normalizer.normalize_text(self._get_first(headers, "X-To")),
            x_cc=self.normalizer.normalize_text(self._get_first(headers, "X-cc")),
            x_bcc=self.normalizer.normalize_text(self._get_first(headers, "X-bcc")),
            x_folder=self.normalizer.normalize_text(self._get_first(headers, "X-Folder")),
            x_origin=self.normalizer.normalize_text(self._get_first(headers, "X-Origin")),
            x_filename=self.normalizer.normalize_text(self._get_first(headers, "X-FileName")),
            mime_version=self.normalizer.normalize_text(self._get_first(headers, "Mime-Version")),
            content_transfer_encoding=self.normalizer.normalize_text(
                self._get_first(headers, "Content-Transfer-Encoding")
            ),
        )

    def _detect_is_response(
        self,
        *,
        in_reply_to: str | None,
        references: list[MessageReferencePayload],
    ) -> bool:
        return bool(in_reply_to or references)

    def _detect_looks_like_response(
        self,
        *,
        subject_raw: str | None,
        in_reply_to: str | None,
        references: list[MessageReferencePayload],
    ) -> bool:
        if in_reply_to or references:
            return True

        subject_lower = (subject_raw or "").strip().lower()
        return subject_lower.startswith(self._RE_PREFIXES)

    def _detect_is_forward(
        self,
        *,
        subject_raw: str | None,
    ) -> bool:
        subject_lower = (subject_raw or "").strip().lower()
        return subject_lower.startswith(self._FW_PREFIXES)

    def _extract_response_target(
        self,
        *,
        in_reply_to: str | None,
        references: list[MessageReferencePayload],
    ) -> tuple[str | None, str | None]:
        if in_reply_to:
            return in_reply_to, "in_reply_to"

        if references:
            return references[-1].referenced_message_id, "references"

        return None, None

    def _extract_thread_root_message_id(
        self,
        *,
        in_reply_to: str | None,
        references: list[MessageReferencePayload],
    ) -> str | None:
        if references:
            return references[0].referenced_message_id

        if in_reply_to:
            return in_reply_to

        return None