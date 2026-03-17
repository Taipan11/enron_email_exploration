from __future__ import annotations

from enron.domain.email_payload import (
    MessageReferencePayload,
    MessageThreadInferencePayload,
)
from enron.normalization.email_normalization_service import (
    EmailNormalizationService,
)


class EmailThreadInferenceService:
    """
    Service métier chargé d'inférer la relation de thread d'un message.

    Ce service ne parse ni headers ni MIME.
    Il consomme seulement des champs déjà extraits et normalisés.

    Sémantique:
    - is_response: réponse confirmée par un parent explicite
      (in_reply_to ou references)
    - looks_like_response: réponse probable selon signaux faibles
      (sujet, body, lignes citées)
    - is_forward: transfert probable détecté via sujet/body
    """

    def __init__(self, normalizer: EmailNormalizationService) -> None:
        self.normalizer = normalizer

    def infer(
        self,
        *,
        subject_raw: str | None,
        message_id: str | None,
        in_reply_to: str | None,
        references: list[MessageReferencePayload],
        body_clean: str | None,
    ) -> MessageThreadInferencePayload:
        reference_ids = self._extract_reference_ids(references)
        normalized_subject = self.normalizer.normalize_subject_for_threading(subject_raw)

        in_reply_to_normalized = self.normalizer.normalize_message_id(in_reply_to)
        message_id_normalized = self.normalizer.normalize_message_id(message_id)

        is_reply_by_subject = self.normalizer.is_reply_subject(subject_raw)
        is_forward_by_subject = self.normalizer.is_forward_subject(subject_raw)

        quoted_line_count = self.normalizer.count_quoted_lines(body_clean)
        body_looks_like_reply = self.normalizer.body_looks_like_reply(body_clean)
        body_looks_like_forward = self.normalizer.body_looks_like_forward(body_clean)
        quoted_header_lines = self.normalizer.extract_quoted_header_lines(body_clean)

        response_to_message_id: str | None = None
        response_to_message_id_source: str | None = None
        thread_root_message_id: str | None = None
        decision_notes: list[str] = []

        if in_reply_to_normalized:
            response_to_message_id = in_reply_to_normalized
            response_to_message_id_source = "in_reply_to"
            decision_notes.append("parent resolved from in_reply_to")
        elif reference_ids:
            response_to_message_id = reference_ids[-1]
            response_to_message_id_source = "references"
            decision_notes.append("parent resolved from last reference")

        if reference_ids:
            thread_root_message_id = reference_ids[0]
            decision_notes.append("thread root resolved from first reference")
        elif in_reply_to_normalized:
            thread_root_message_id = in_reply_to_normalized
            decision_notes.append("thread root fallback to in_reply_to")
        elif message_id_normalized:
            thread_root_message_id = message_id_normalized
            decision_notes.append("thread root defaulted to current message_id")

        is_response = response_to_message_id is not None

        looks_like_response = bool(
            is_response
            or is_reply_by_subject
            or body_looks_like_reply
            or quoted_line_count > 0
            or bool(quoted_header_lines)
        )

        is_forward = bool(
            is_forward_by_subject
            or body_looks_like_forward
        )

        references_depth = len(reference_ids)

        confidence = self._compute_confidence(
            is_response=is_response,
            has_in_reply_to=bool(in_reply_to_normalized),
            references_depth=references_depth,
            is_reply_by_subject=is_reply_by_subject,
            quoted_line_count=quoted_line_count,
            body_looks_like_reply=body_looks_like_reply,
            has_quoted_header_lines=bool(quoted_header_lines),
            is_forward=is_forward,
        )

        if is_reply_by_subject:
            decision_notes.append("reply subject prefix detected")
        if is_forward_by_subject:
            decision_notes.append("forward subject prefix detected")
        if body_looks_like_reply:
            decision_notes.append("reply pattern detected in body")
        if body_looks_like_forward:
            decision_notes.append("forward pattern detected in body")
        if quoted_line_count > 0:
            decision_notes.append(f"quoted lines detected: {quoted_line_count}")
        if quoted_header_lines:
            decision_notes.append(
                f"quoted header lines detected: {len(quoted_header_lines)}"
            )
        if normalized_subject:
            decision_notes.append(f"normalized subject: {normalized_subject}")

        return MessageThreadInferencePayload(
            is_response=is_response,
            looks_like_response=looks_like_response,
            is_forward=is_forward,
            response_to_message_id=response_to_message_id,
            response_to_message_id_source=response_to_message_id_source,
            thread_root_message_id=thread_root_message_id,
            references_depth=references_depth,
            quoted_line_count=quoted_line_count,
            normalized_subject=normalized_subject,
            confidence=confidence,
            decision_notes=decision_notes,
        )

    def _extract_reference_ids(
        self,
        references: list[MessageReferencePayload],
    ) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()

        for item in references:
            referenced_message_id = self.normalizer.normalize_message_id(
                item.referenced_message_id
            )
            if not referenced_message_id:
                continue
            if referenced_message_id in seen:
                continue

            seen.add(referenced_message_id)
            result.append(referenced_message_id)

        return result

    def _compute_confidence(
        self,
        *,
        is_response: bool,
        has_in_reply_to: bool,
        references_depth: int,
        is_reply_by_subject: bool,
        quoted_line_count: int,
        body_looks_like_reply: bool,
        has_quoted_header_lines: bool,
        is_forward: bool,
    ) -> float:
        score = 0.0

        if has_in_reply_to:
            score += 0.70

        if references_depth > 0:
            score += min(0.20, 0.08 * references_depth)

        if is_reply_by_subject:
            score += 0.05

        if body_looks_like_reply:
            score += 0.10

        if quoted_line_count > 0:
            score += min(0.10, quoted_line_count * 0.01)

        if has_quoted_header_lines:
            score += 0.05

        if not is_response and (is_reply_by_subject or body_looks_like_reply):
            score = max(score, 0.35)

        if is_forward and not is_response:
            score = min(score, 0.40)

        return min(1.0, round(score, 3))