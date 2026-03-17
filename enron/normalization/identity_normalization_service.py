from __future__ import annotations

import re


class IdentityNormalizationService:
    GENERIC_LOCALPARTS: set[str] = {
        "admin",
        "administrator",
        "info",
        "team",
        "office",
        "mail",
        "contact",
        "support",
        "noreply",
        "no-reply",
        "system",
        "corp",
        "service",
        "services",
        "group",
        "list",
        "newsletter",
    }

    _TITLE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
        (re.compile(r"\b(ceo)\b", re.I), "CEO"),
        (re.compile(r"\b(cfo)\b", re.I), "CFO"),
        (re.compile(r"\b(coo)\b", re.I), "COO"),
        (re.compile(r"\b(president)\b", re.I), "President"),
        (re.compile(r"\b(vice president|vp)\b", re.I), "Vice President"),
        (re.compile(r"\b(director)\b", re.I), "Director"),
        (re.compile(r"\b(manager)\b", re.I), "Manager"),
        (re.compile(r"\b(analyst)\b", re.I), "Analyst"),
        (re.compile(r"\b(trader)\b", re.I), "Trader"),
        (re.compile(r"\b(attorney|lawyer|counsel)\b", re.I), "Legal Counsel"),
        (re.compile(r"\b(assistant)\b", re.I), "Assistant"),
    ]

    def tokenize_alpha(self, value: str | None) -> set[str]:
        if not value:
            return set()

        return {
            token
            for token in re.findall(r"[a-z]+", value.lower())
            if len(token) >= 2
        }

    def join_pipe(self, values: list[str]) -> str:
        cleaned = [value.strip() for value in values if value and value.strip()]
        return " | ".join(cleaned)

    def split_name_parts(self, display_name: str | None) -> tuple[str | None, str | None]:
        if not display_name:
            return None, None

        name = display_name.strip()

        if "," in name:
            parts = [p.strip() for p in name.split(",", 1)]
            if len(parts) == 2:
                name = f"{parts[1]} {parts[0]}"

        tokens = [t for t in re.split(r"\s+", name) if t]
        tokens = [t for t in tokens if len(t) > 1]

        if not tokens:
            return None, None

        if len(tokens) == 1:
            return tokens[0].title(), None

        return tokens[0].title(), tokens[-1].title()

    def classify_identity(
        self,
        candidate_name: str | None,
        candidate_email: str | None,
        email_service,
    ) -> str:
        localpart = (email_service.extract_email_localpart(candidate_email) or "").lower()

        if not candidate_name and not candidate_email:
            return "unknown"

        if localpart in self.GENERIC_LOCALPARTS:
            return "system"

        if any(token in localpart for token in ("newsletter", "announce", "announcement", "listserv")):
            return "external_bulk"

        if any(token in localpart for token in ("team", "group", "office", "corp", "committee")):
            return "corporate"

        name_tokens = self.tokenize_alpha(candidate_name)
        if len(name_tokens) >= 2 and localpart not in self.GENERIC_LOCALPARTS:
            return "person"

        if "." in localpart and len(self.tokenize_alpha(localpart)) >= 2:
            return "likely_person"

        return "unknown"

    def identity_priority(self, identity_type: str | None) -> int:
        priorities = {
            "person": 0,
            "likely_person": 1,
            "unknown": 2,
            "corporate": 3,
            "system": 4,
            "external_bulk": 5,
            None: 6,
        }
        return priorities.get(identity_type, 99)

    def extract_title_candidates(self, *values: str | None) -> list[str]:
        found: list[str] = []

        for value in values:
            if not value:
                continue

            for pattern, label in self._TITLE_PATTERNS:
                if pattern.search(value):
                    found.append(label)

        seen: set[str] = set()
        result: list[str] = []
        for item in found:
            if item not in seen:
                seen.add(item)
                result.append(item)

        return result