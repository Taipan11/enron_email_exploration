from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from django.db import transaction

from enron.models import Message, MessageReference


@dataclass(slots=True)
class ThreadResolutionStats:
    total_messages: int = 0
    updated_messages: int = 0
    resolved_by_references: int = 0
    resolved_by_in_reply_to: int = 0
    self_rooted_messages: int = 0
    unresolved_messages: int = 0


class MessageThreadResolverService:
    """
    Reconstruit les relations de thread après import.

    Stratégie :
    1. Si References existe :
       - parent = dernier message référencé trouvé en base
       - racine = premier message référencé trouvé en base
    2. Sinon si In-Reply-To existe :
       - parent = message correspondant
       - racine = racine du parent si disponible, sinon parent lui-même
    3. Sinon :
       - pas de parent
       - racine = le message lui-même
    """

    def rebuild_all(self, batch_size: int = 1000) -> ThreadResolutionStats:
        stats = ThreadResolutionStats()

        messages = list(
            Message.objects.all().only(
                "id",
                "message_id",
                "in_reply_to",
                "response_to_message_id",
                "response_to_message_id_source",
                "thread_root_message_id",
                "references_depth",
            )
        )
        stats.total_messages = len(messages)

        if not messages:
            return stats

        message_id_to_message = {
            m.message_id: m
            for m in messages
            if m.message_id
        }

        references_by_message_id = self._load_references()

        to_update: list[Message] = []

        for message in messages:
            old_parent = message.response_to_message_id
            old_source = message.response_to_message_id_source
            old_root = message.thread_root_message_id
            old_depth = message.references_depth

            parent_id = None
            parent_source = None
            root_id = None
            depth = 0

            reference_chain = references_by_message_id.get(message.id, [])

            if reference_chain:
                resolved_refs = [
                    ref for ref in reference_chain
                    if ref in message_id_to_message
                ]
                depth = len(resolved_refs)

                if resolved_refs:
                    root_id = resolved_refs[0]
                    parent_id = resolved_refs[-1]
                    parent_source = "references"
                    stats.resolved_by_references += 1

            if not parent_id and message.in_reply_to:
                parent = message_id_to_message.get(message.in_reply_to)
                if parent:
                    parent_id = parent.message_id
                    parent_source = "in_reply_to"
                    root_id = parent.thread_root_message_id or parent.message_id
                    depth = max(depth, 1)
                    stats.resolved_by_in_reply_to += 1

            if not root_id and message.message_id:
                root_id = message.message_id
                stats.self_rooted_messages += 1

            if not parent_id and message.in_reply_to and not reference_chain:
                stats.unresolved_messages += 1

            message.response_to_message_id = parent_id
            message.response_to_message_id_source = parent_source
            message.thread_root_message_id = root_id
            message.references_depth = depth

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

    def _load_references(self) -> dict[int, list[str]]:
        references = (
            MessageReference.objects
            .all()
            .only("message_id", "referenced_message_id")
            .order_by("message_id", "id")
        )

        result: dict[int, list[str]] = {}

        for ref in references:
            result.setdefault(ref.message_id, []).append(ref.referenced_message_id)

        return result