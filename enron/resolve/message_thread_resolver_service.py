from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from django.db import transaction

from enron.models import Message, MessageRecipient, MessageReference


@dataclass(slots=True)
class ThreadResolutionStats:
    total_messages: int = 0
    updated_messages: int = 0

    resolved_by_references: int = 0
    resolved_by_in_reply_to: int = 0
    resolved_by_heuristics: int = 0

    self_rooted_messages: int = 0
    unresolved_messages: int = 0

    heuristic_candidates_evaluated: int = 0
    heuristic_matches_accepted: int = 0
    heuristic_matches_rejected: int = 0


class MessageThreadResolverService:
    """
    Reconstruit les relations de thread après import.

    Ordre de résolution :
    1. References
    2. In-Reply-To
    3. Heuristique :
       - sujet normalisé identique
       - candidat antérieur dans le temps
       - compatibilité participants
    4. Fallback :
       - racine = le message lui-même

    Notes :
    - la résolution heuristique ne s'applique qu'aux messages qui ressemblent
      à des réponses (`is_response=True`) ou, à défaut, qui ont un sujet normalisé.
    - `response_to_message_id_source` est renseigné explicitement :
      - "references"
      - "in_reply_to"
      - "heuristic_subject_time_participants"
    """

    HEURISTIC_MAX_LOOKBACK_DAYS = 30
    HEURISTIC_MIN_SCORE = 0.45

    def rebuild_all(self, batch_size: int = 1000) -> ThreadResolutionStats:
        stats = ThreadResolutionStats()

        messages = self._load_messages()
        stats.total_messages = len(messages)

        if not messages:
            return stats

        message_id_to_message = {
            message.message_id: message
            for message in messages
            if message.message_id
        }

        references_by_message_pk = self._load_references()
        recipients_by_message_pk = self._load_recipient_emails()
        messages_by_subject = self._group_messages_by_subject(messages)

        to_update: list[Message] = []

        for message in messages:
            resolution = self._resolve_message(
                message=message,
                message_id_to_message=message_id_to_message,
                references_by_message_pk=references_by_message_pk,
                recipients_by_message_pk=recipients_by_message_pk,
                messages_by_subject=messages_by_subject,
                stats=stats,
            )

            old_parent = message.response_to_message_id
            old_source = message.response_to_message_id_source
            old_root = message.thread_root_message_id
            old_depth = message.references_depth

            message.response_to_message_id = resolution.parent_id
            message.response_to_message_id_source = resolution.parent_source
            message.thread_root_message_id = resolution.root_id
            message.references_depth = resolution.depth

            if resolution.kind == "references":
                stats.resolved_by_references += 1
            elif resolution.kind == "in_reply_to":
                stats.resolved_by_in_reply_to += 1
            elif resolution.kind == "heuristics":
                stats.resolved_by_heuristics += 1
            elif resolution.kind == "self_rooted":
                stats.self_rooted_messages += 1
            else:
                stats.unresolved_messages += 1

            if (
                old_parent != message.response_to_message_id
                or old_source != message.response_to_message_id_source
                or old_root != message.thread_root_message_id
                or old_depth != message.references_depth
            ):
                to_update.append(message)

        if to_update:
            with transaction.atomic():
                for start in range(0, len(to_update), batch_size):
                    chunk = to_update[start:start + batch_size]
                    Message.objects.bulk_update(
                        chunk,
                        [
                            "response_to_message_id",
                            "response_to_message_id_source",
                            "thread_root_message_id",
                            "references_depth",
                        ],
                        batch_size=batch_size,
                    )

        stats.updated_messages = len(to_update)
        return stats

    def _load_messages(self) -> list[Message]:
        return list(
            Message.objects.all().only(
                "id",
                "message_id",
                "in_reply_to",
                "response_to_message_id",
                "response_to_message_id_source",
                "thread_root_message_id",
                "references_depth",
                "subject_normalized",
                "sent_at",
                "sender_email",
                "is_response",
                "is_forward",
            )
        )

    def _resolve_message(
        self,
        *,
        message: Message,
        message_id_to_message: dict[str, Message],
        references_by_message_pk: dict[int, list[str]],
        recipients_by_message_pk: dict[int, set[str]],
        messages_by_subject: dict[str, list[Message]],
        stats: ThreadResolutionStats,
    ) -> "_Resolution":
        reference_chain = references_by_message_pk.get(message.id, [])
        resolved_refs = [
            referenced_message_id
            for referenced_message_id in reference_chain
            if referenced_message_id in message_id_to_message
        ]

        resolution = self._resolve_by_references(resolved_refs)
        if resolution is not None:
            return resolution

        resolution = self._resolve_by_in_reply_to(
            message=message,
            message_id_to_message=message_id_to_message,
        )
        if resolution is not None:
            return resolution

        resolution = self._resolve_by_heuristics(
            message=message,
            recipients_by_message_pk=recipients_by_message_pk,
            messages_by_subject=messages_by_subject,
            stats=stats,
        )
        if resolution is not None:
            stats.heuristic_matches_accepted += 1
            return resolution

        stats.heuristic_matches_rejected += 1

        if message.message_id:
            return _Resolution(
                parent_id=None,
                parent_source=None,
                root_id=message.message_id,
                depth=0,
                kind="self_rooted",
            )

        return _Resolution(
            parent_id=None,
            parent_source=None,
            root_id=None,
            depth=0,
            kind="unresolved",
        )

    def _resolve_by_references(self, resolved_refs: list[str]) -> "_Resolution | None":
        if not resolved_refs:
            return None

        return _Resolution(
            parent_id=resolved_refs[-1],
            parent_source="references",
            root_id=resolved_refs[0],
            depth=len(resolved_refs),
            kind="references",
        )

    def _resolve_by_in_reply_to(
        self,
        *,
        message: Message,
        message_id_to_message: dict[str, Message],
    ) -> "_Resolution | None":
        if not message.in_reply_to:
            return None

        parent = message_id_to_message.get(message.in_reply_to)
        if parent is None or not parent.message_id:
            return None

        return _Resolution(
            parent_id=parent.message_id,
            parent_source="in_reply_to",
            root_id=parent.thread_root_message_id or parent.message_id,
            depth=1,
            kind="in_reply_to",
        )

    def _resolve_by_heuristics(
        self,
        *,
        message: Message,
        recipients_by_message_pk: dict[int, set[str]],
        messages_by_subject: dict[str, list[Message]],
        stats: ThreadResolutionStats,
    ) -> "_Resolution | None":
        if not self._is_heuristic_eligible(message):
            return None

        candidates = messages_by_subject.get(message.subject_normalized or "", [])
        if not candidates:
            return None

        message_recipients = recipients_by_message_pk.get(message.id, set())

        best_candidate: Message | None = None
        best_score = 0.0

        for candidate in candidates:
            if candidate.id == message.id:
                continue

            if not candidate.message_id:
                continue

            if not candidate.sent_at or not message.sent_at:
                continue

            if candidate.sent_at >= message.sent_at:
                continue

            if message.sent_at - candidate.sent_at > timedelta(days=self.HEURISTIC_MAX_LOOKBACK_DAYS):
                continue

            stats.heuristic_candidates_evaluated += 1

            score = self._score_candidate(
                message=message,
                candidate=candidate,
                message_recipients=message_recipients,
                candidate_recipients=recipients_by_message_pk.get(candidate.id, set()),
            )

            if score > best_score:
                best_score = score
                best_candidate = candidate

        if best_candidate is None or best_score < self.HEURISTIC_MIN_SCORE:
            return None

        return _Resolution(
            parent_id=best_candidate.message_id,
            parent_source="heuristic_subject_time_participants",
            root_id=best_candidate.thread_root_message_id or best_candidate.message_id,
            depth=1,
            kind="heuristics",
        )

    def _is_heuristic_eligible(self, message: Message) -> bool:
        if not message.subject_normalized:
            return False

        if not message.sent_at:
            return False

        if message.is_forward:
            return False

        return bool(message.is_response)

    def _score_candidate(
        self,
        *,
        message: Message,
        candidate: Message,
        message_recipients: set[str],
        candidate_recipients: set[str],
    ) -> float:
        """
        Score heuristique borné implicitement entre 0.0 et 1.0+.

        Pondération :
        - sujet identique : 0.45
        - sender du candidat dans les destinataires du message : 0.25
        - sender du message dans les destinataires du candidat : 0.20
        - proximité temporelle : jusqu'à 0.10
        """
        score = 0.0

        score += 0.45

        if candidate.sender_email and candidate.sender_email in message_recipients:
            score += 0.25

        if message.sender_email and message.sender_email in candidate_recipients:
            score += 0.20

        if message.sent_at and candidate.sent_at:
            delta_seconds = (message.sent_at - candidate.sent_at).total_seconds()

            if delta_seconds <= 3600:
                score += 0.10
            elif delta_seconds <= 86400:
                score += 0.06
            elif delta_seconds <= 7 * 86400:
                score += 0.03

        return score

    def _load_references(self) -> dict[int, list[str]]:
        references = (
            MessageReference.objects
            .all()
            .only("message_id", "referenced_message_id")
            .order_by("message_id", "id")
        )

        result: dict[int, list[str]] = {}

        for reference in references:
            if not reference.referenced_message_id:
                continue
            result.setdefault(reference.message_id, []).append(reference.referenced_message_id)

        return result

    def _load_recipient_emails(self) -> dict[int, set[str]]:
        rows = (
            MessageRecipient.objects
            .select_related("email_address")
            .only("message_id", "email_address__email")
        )

        result: dict[int, set[str]] = {}

        for row in rows:
            email = row.email_address.email if row.email_address else None
            if not email:
                continue
            result.setdefault(row.message_id, set()).add(email)

        return result

    def _group_messages_by_subject(
        self,
        messages: list[Message],
    ) -> dict[str, list[Message]]:
        result: dict[str, list[Message]] = {}

        for message in messages:
            if not message.subject_normalized:
                continue
            result.setdefault(message.subject_normalized, []).append(message)

        for subject_messages in result.values():
            subject_messages.sort(
                key=lambda message: (message.sent_at is None, message.sent_at, message.id)
            )

        return result


@dataclass(slots=True)
class _Resolution:
    parent_id: str | None
    parent_source: str | None
    root_id: str | None
    depth: int
    kind: str