from __future__ import annotations

import re
from email import policy
from email.message import Message
from email.parser import BytesParser

from enron.normalization.email_normalization_service import EmailNormalizationService
from enron.normalization.email_signature_service import EmailSignatureService

from enron.domain.email_payload import (
    ParsedMessageBody
)
class EmailBodyParserService:
    """
    Service dédié au parsing du corps du message.

    Responsabilités:
    - extraire le body depuis le MIME complet si possible
    - choisir text/plain en priorité
    - fallback sur text/html si nécessaire
    - nettoyer le texte / html
    - extraire la signature
    - détecter grossièrement les lignes citées
    - extraire un snippet et des mots-clés simples
    """

    _QUOTED_LINE_RE = re.compile(r"^\s*>+")
    _ON_DATE_WROTE_RE = re.compile(r"^on .+ wrote:\s*$", re.IGNORECASE)

    def __init__(
        self,
        normalizer: EmailNormalizationService,
        signature_service: EmailSignatureService,
    ) -> None:
        self.normalizer = normalizer
        self.signature_service = signature_service

    def parse(self, raw_email: str | bytes) -> ParsedMessageBody:
        if not raw_email:
            return ParsedMessageBody()

        raw_bytes = self._to_bytes(raw_email)

        try:
            message = BytesParser(policy=policy.default).parsebytes(raw_bytes)
            return self._parse_message(message)
        except Exception:
            fallback_text = self._fallback_extract_body_from_raw(raw_email)
            return self._build_from_plain_text(fallback_text)

    def _parse_message(self, message: Message) -> ParsedMessageBody:
        plain_parts: list[str] = []
        html_parts: list[str] = []

        for part in message.walk():
            if part.is_multipart():
                continue

            content_type = (part.get_content_type() or "").lower()
            content_disposition = (part.get_content_disposition() or "").lower()

            if content_disposition == "attachment":
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

            if not text:
                continue

            if content_type == "text/plain":
                plain_parts.append(text)
            elif content_type == "text/html":
                html_parts.append(text)

        if plain_parts:
            plain_text = "\n".join(plain_parts)
            return self._build_from_plain_text(
                plain_text,
                has_plain_text_body=True,
                has_html_body=bool(html_parts),
                html_source="\n".join(html_parts) if html_parts else None,
            )

        if html_parts:
            html_text = "\n".join(html_parts)
            return self._build_from_html(
                html_text,
                has_plain_text_body=False,
                has_html_body=True,
            )

        return ParsedMessageBody()

    def _build_from_plain_text(
        self,
        plain_text: str | None,
        *,
        has_plain_text_body: bool = True,
        has_html_body: bool = False,
        html_source: str | None = None,
    ) -> ParsedMessageBody:
        normalized_body = self.normalizer.clean_body_text(plain_text or "")
        body_without_signature, signature = self.signature_service.split_signature(
            normalized_body
        )

        quoted_text, quoted_line_count = self._extract_quoted_text(body_without_signature)
        snippet = self._build_snippet(body_without_signature)
        keywords = self._extract_keywords(body_without_signature)

        html_clean = None
        if html_source:
            html_clean = self.normalizer.normalize_html_body(html_source)

        return ParsedMessageBody(
            body_raw=plain_text,
            body_clean=body_without_signature,
            body_html_clean=html_clean,
            signature=signature,
            has_html_body=has_html_body,
            has_plain_text_body=has_plain_text_body,
            quoted_text=quoted_text,
            quoted_line_count=quoted_line_count,
            keywords=keywords,
            snippet=snippet,
        )

    def _build_from_html(
        self,
        html_text: str,
        *,
        has_plain_text_body: bool,
        has_html_body: bool,
    ) -> ParsedMessageBody:
        html_clean = self.normalizer.normalize_html_body(html_text)
        text_from_html = self.normalizer.clean_body_text(html_clean or "")
        body_without_signature, signature = self.signature_service.split_signature(
            text_from_html
        )

        quoted_text, quoted_line_count = self._extract_quoted_text(body_without_signature)
        snippet = self._build_snippet(body_without_signature)
        keywords = self._extract_keywords(body_without_signature)

        return ParsedMessageBody(
            body_raw=html_text,
            body_clean=body_without_signature,
            body_html_clean=html_clean,
            signature=signature,
            has_html_body=has_html_body,
            has_plain_text_body=has_plain_text_body,
            quoted_text=quoted_text,
            quoted_line_count=quoted_line_count,
            keywords=keywords,
            snippet=snippet,
        )

    def _extract_quoted_text(self, text: str | None) -> tuple[str | None, int]:
        if not text:
            return None, 0

        quoted_lines: list[str] = []
        count = 0

        for line in text.splitlines():
            stripped = line.strip()
            if self._QUOTED_LINE_RE.match(line) or self._ON_DATE_WROTE_RE.match(stripped):
                quoted_lines.append(line)
                count += 1

        if not quoted_lines:
            return None, 0

        return "\n".join(quoted_lines).strip() or None, count

    def _extract_keywords(self, text: str | None, limit: int = 10) -> list[str]:
        if not text:
            return []

        tokens = re.findall(r"\b[a-zA-Z]{4,}\b", text.lower())
        stopwords = {
            "this", "that", "with", "have", "from", "your", "will", "would",
            "there", "their", "about", "please", "thanks", "thank", "subject",
            "message", "enron", "forward", "regards",
        }

        freq: dict[str, int] = {}
        for token in tokens:
            if token in stopwords:
                continue
            freq[token] = freq.get(token, 0) + 1

        ranked = sorted(freq.items(), key=lambda item: (-item[1], item[0]))
        return [word for word, _ in ranked[:limit]]

    def _build_snippet(self, text: str | None, max_length: int = 240) -> str | None:
        if not text:
            return None

        normalized = re.sub(r"\s+", " ", text).strip()
        if not normalized:
            return None

        return normalized[:max_length].strip()

    def _fallback_extract_body_from_raw(self, raw_email: str | bytes) -> str:
        if isinstance(raw_email, bytes):
            text = raw_email.decode("utf-8", errors="replace")
        else:
            text = raw_email

        for separator in ("\r\n\r\n", "\n\n", "\r\r"):
            if separator in text:
                return text.split(separator, 1)[1]

        return ""

    def _to_bytes(self, raw_email: str | bytes) -> bytes:
        if isinstance(raw_email, bytes):
            return raw_email
        return raw_email.encode("utf-8", errors="replace")