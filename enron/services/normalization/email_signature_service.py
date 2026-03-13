from __future__ import annotations

import re

from enron.services.normalization.email_normalization_service import EmailNormalizationService


class EmailSignatureService:
    """
    Service dédié à l'extraction et à la suppression de signatures email.
    """

    _SIGNATURE_SEPARATOR_RE = re.compile(
        r"^\s*(--\s*$|__+\s*$|-{3,}\s*$|\*{3,}\s*$)",
        flags=re.IGNORECASE,
    )

    _SIGNATURE_CLOSING_RE = re.compile(
        r"""^\s*(
            thanks|thank you|thanks,|thank you,|
            regards|regards,|
            best|best,|
            best regards|best regards,|
            sincerely|sincerely,|
            cheers|cheers,|
            cordially|cordially,|
            cordialement|bien cordialement|
            many thanks|many thanks,
        )\s*$""",
        flags=re.IGNORECASE | re.VERBOSE,
    )

    _SIGNATURE_LINE_HINT_RE = re.compile(
        r"""(?ix)
        (
            \b(tel|telephone|phone|office|cell|mobile|mob|fax|pager)\b
            |
            \b[e-]?mail\b
            |
            @
            |
            \bwww\.
            |
            \bhttps?://
            |
            \benron\b
            |
            \b(
                assistant|manager|director|president|vice president|vp|
                analyst|specialist|chairman|trader
            )\b
        )
        """
    )

    _DISCLAIMER_START_RE = re.compile(
        r"""(?ix)^\s*(
            this\s+e-?mail
            |
            this\s+message
            |
            the\s+information\s+contained\s+in\s+this
            |
            this\s+communication
            |
            confidential(ity)?
            |
            privileged\s+and\s+confidential
        )"""
    )

    def __init__(self, normalizer: EmailNormalizationService | None = None) -> None:
        self.normalizer = normalizer or EmailNormalizationService()

    def _looks_like_signature_line(self, line: str) -> bool:
        line = line.strip()
        if not line:
            return False

        if self._SIGNATURE_SEPARATOR_RE.match(line):
            return True

        if self._SIGNATURE_CLOSING_RE.match(line):
            return True

        if self._DISCLAIMER_START_RE.match(line):
            return True

        if self._SIGNATURE_LINE_HINT_RE.search(line):
            return True

        if len(line) <= 40 and not line.endswith((".", "!", "?", ":")):
            words = line.split()
            if 1 <= len(words) <= 6:
                return True

        return False

    def _score_signature_block(self, lines: list[str]) -> int:
        score = 0

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            if self._SIGNATURE_SEPARATOR_RE.match(stripped):
                score += 4

            if self._SIGNATURE_CLOSING_RE.match(stripped):
                score += 4

            if self._DISCLAIMER_START_RE.match(stripped):
                score += 4

            if self._SIGNATURE_LINE_HINT_RE.search(stripped):
                score += 2

            if len(stripped) <= 40 and not stripped.endswith((".", "!", "?", ":")):
                words = stripped.split()
                if 1 <= len(words) <= 6:
                    score += 1

        return score

    def extract_signature(self, value: str | None) -> str | None:
        value = self.normalizer.normalize_body_text(value)
        if not value:
            return None

        lines = value.split("\n")
        if len(lines) < 2:
            return None

        max_scan = min(12, len(lines))
        tail = lines[-max_scan:]

        for idx, line in enumerate(tail):
            if self._SIGNATURE_SEPARATOR_RE.match(line.strip()):
                signature = "\n".join(tail[idx:]).strip()
                return signature or None

        for idx, line in enumerate(tail):
            if self._DISCLAIMER_START_RE.match(line.strip()):
                signature = "\n".join(tail[idx:]).strip()
                return signature or None

        best_signature: str | None = None
        best_score = 0

        for start in range(len(tail)):
            candidate = tail[start:]
            non_empty = [line for line in candidate if line.strip()]
            if not non_empty:
                continue

            score = self._score_signature_block(candidate)

            first_non_empty = next((line.strip() for line in candidate if line.strip()), "")
            if self._SIGNATURE_CLOSING_RE.match(first_non_empty):
                score += 3

            long_sentence_count = sum(
                1
                for line in non_empty
                if len(line.strip()) > 60 and line.strip().endswith((".", "!", "?"))
            )
            score -= long_sentence_count * 2

            if score >= 4 and score > best_score:
                best_score = score
                best_signature = "\n".join(candidate).strip()

        return best_signature or None

    def remove_signature(self, value: str | None) -> str | None:
        value = self.normalizer.normalize_body_text(value)
        if not value:
            return None

        signature = self.extract_signature(value)
        if not signature:
            return value

        if value.endswith(signature):
            cleaned = value[: -len(signature)].rstrip()
            return cleaned or None

        idx = value.rfind(signature)
        if idx != -1:
            cleaned = value[:idx].rstrip()
            return cleaned or None

        return value

    def split_signature(self, value: str | None) -> tuple[str | None, str | None]:
        normalized = self.normalizer.normalize_body_text(value)
        if not normalized:
            return None, None

        signature = self.extract_signature(normalized)
        if not signature:
            return normalized, None

        cleaned = self.remove_signature(normalized)
        return cleaned, signature