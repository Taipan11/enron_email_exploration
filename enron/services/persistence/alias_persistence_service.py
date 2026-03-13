from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd
from django.db import transaction

from enron.models import Collaborator, EmailAddress
from enron.services.normalization.email_folder_normalization_service import (
    EmailFolderNormalizationService,
)
from enron.services.normalization.email_normalization_service import (
    EmailNormalizationService,
)


@dataclass(slots=True)
class CollaboratorAliasPersistenceStats:
    processed_rows: int = 0
    unique_candidate_emails: int = 0
    collaborators_found: int = 0
    email_addresses_found: int = 0
    email_addresses_linked: int = 0
    email_addresses_already_linked: int = 0
    skipped_missing_mailbox_key: int = 0
    skipped_missing_collaborator: int = 0
    skipped_missing_email: int = 0
    skipped_unknown_email_address: int = 0


class CollaboratorAliasPersistenceService:
    """
    Persiste les alias email trouvés pour les collaborateurs.

    Règle simple :
    - on part du DataFrame d'alias
    - on filtre sur les labels autorisés
    - on retrouve le Collaborator via mailbox_owner -> mailbox_key
    - on lie EmailAddress.collaborator au Collaborator
    """

    DEFAULT_ACCEPTED_ALIAS_LABELS = {"strong_alias"}

    def __init__(
        self,
        email_normalization_service: EmailNormalizationService | None = None,
        folder_normalization_service: EmailFolderNormalizationService | None = None,
    ) -> None:
        self.email_service = email_normalization_service or EmailNormalizationService()
        self.folder_service = folder_normalization_service or EmailFolderNormalizationService()

    @transaction.atomic
    def persist_aliases_from_dataframe(
        self,
        alias_df: pd.DataFrame,
        *,
        accepted_alias_labels: set[str] | None = None,
    ) -> dict[str, int]:
        if alias_df is None or alias_df.empty:
            return self._stats_to_dict(CollaboratorAliasPersistenceStats())

        accepted_labels = accepted_alias_labels or self.DEFAULT_ACCEPTED_ALIAS_LABELS
        stats = CollaboratorAliasPersistenceStats()

        filtered_df = alias_df[alias_df["alias_label"].isin(accepted_labels)].copy()
        rows = filtered_df.to_dict(orient="records")

        seen_candidate_emails: set[str] = set()

        for row in rows:
            stats.processed_rows += 1

            mailbox_owner = self.email_service.normalize_text(row.get("mailbox_owner"))
            candidate_email = self.email_service.normalize_email_address(
                row.get("candidate_email")
            )

            if candidate_email:
                seen_candidate_emails.add(candidate_email)

            mailbox_key = self.folder_service.normalize_mailbox_key(mailbox_owner)
            if not mailbox_key:
                stats.skipped_missing_mailbox_key += 1
                continue

            if not candidate_email:
                stats.skipped_missing_email += 1
                continue

            collaborator = self._get_collaborator_by_mailbox_key(mailbox_key)
            if collaborator is None:
                stats.skipped_missing_collaborator += 1
                continue

            stats.collaborators_found += 1

            email_address = self._get_email_address(candidate_email)
            if email_address is None:
                stats.skipped_unknown_email_address += 1
                continue

            stats.email_addresses_found += 1

            if email_address.collaborator_id == collaborator.id:
                stats.email_addresses_already_linked += 1
                continue

            email_address.collaborator = collaborator
            email_address.save(update_fields=["collaborator", "updated_at"])
            stats.email_addresses_linked += 1

        stats.unique_candidate_emails = len(seen_candidate_emails)
        return self._stats_to_dict(stats)

    def _get_collaborator_by_mailbox_key(self, mailbox_key: str) -> Collaborator | None:
        try:
            return Collaborator.objects.get(employee_key=mailbox_key)
        except Collaborator.DoesNotExist:
            return None

    def _get_email_address(self, email: str) -> EmailAddress | None:
        try:
            return EmailAddress.objects.get(email=email)
        except EmailAddress.DoesNotExist:
            return None

    def _stats_to_dict(
        self,
        stats: CollaboratorAliasPersistenceStats,
    ) -> dict[str, int]:
        return {
            "processed_rows": stats.processed_rows,
            "unique_candidate_emails": stats.unique_candidate_emails,
            "collaborators_found": stats.collaborators_found,
            "email_addresses_found": stats.email_addresses_found,
            "email_addresses_linked": stats.email_addresses_linked,
            "email_addresses_already_linked": stats.email_addresses_already_linked,
            "skipped_missing_mailbox_key": stats.skipped_missing_mailbox_key,
            "skipped_missing_collaborator": stats.skipped_missing_collaborator,
            "skipped_missing_email": stats.skipped_missing_email,
            "skipped_unknown_email_address": stats.skipped_unknown_email_address,
        }