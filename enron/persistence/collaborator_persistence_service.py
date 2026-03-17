from __future__ import annotations

from dataclasses import dataclass

from django.db import transaction

from enron.domain.collaborator_payloads import CollaboratorInferenceResult
from enron.models import Collaborator, EmailAddress, Mailbox


@dataclass(slots=True)
class CollaboratorPersistenceResult:
    collaborator: Collaborator
    collaborator_created: bool
    primary_email_address: EmailAddress | None
    email_linked: bool
    mailbox_updated: bool


class CollaboratorPersistenceService:
    """
    Persiste les résultats d'inférence collaborateur dans la base.
    """

    @transaction.atomic
    def save(
        self,
        inference_result: CollaboratorInferenceResult,
    ) -> CollaboratorPersistenceResult:
        collaborator, collaborator_created = self._upsert_collaborator(inference_result)
        email_address, email_linked = self._link_primary_email(
            collaborator=collaborator,
            normalized_email=inference_result.inferred_primary_email,
        )
        mailbox_updated = self._link_mailbox(
            collaborator=collaborator,
            mailbox_key=inference_result.mailbox_key,
        )

        return CollaboratorPersistenceResult(
            collaborator=collaborator,
            collaborator_created=collaborator_created,
            primary_email_address=email_address,
            email_linked=email_linked,
            mailbox_updated=mailbox_updated,
        )

    def save_many(
        self,
        inference_results: list[CollaboratorInferenceResult],
    ) -> dict[str, int]:
        stats: dict[str, int] = {
            "processed": 0,
            "collaborators_created": 0,
            "emails_linked": 0,
            "mailboxes_updated": 0,
        }

        for result in inference_results:
            persistence_result = self.save(result)
            stats["processed"] += 1

            if persistence_result.collaborator_created:
                stats["collaborators_created"] += 1

            if persistence_result.email_linked:
                stats["emails_linked"] += 1

            if persistence_result.mailbox_updated:
                stats["mailboxes_updated"] += 1

        return stats

    def _upsert_collaborator(
        self,
        inference_result: CollaboratorInferenceResult,
    ) -> tuple[Collaborator, bool]:
        employee_key = inference_result.mailbox_key.strip().lower()
        if not employee_key:
            raise ValueError("mailbox_key est obligatoire pour persister un Collaborator")

        defaults: dict[str, object] = {
            "display_name": inference_result.inferred_display_name,
            "first_name": inference_result.inferred_first_name,
            "last_name": inference_result.inferred_last_name,
            "position_title": inference_result.inferred_position_title,
            "is_enron_employee": not inference_result.is_corporate_mailbox_candidate,
            "notes": self._build_notes(inference_result),
        }

        collaborator, created = Collaborator.objects.update_or_create(
            employee_key=employee_key,
            defaults=defaults,
        )
        return collaborator, created

    def _link_primary_email(
        self,
        *,
        collaborator: Collaborator,
        normalized_email: str | None,
    ) -> tuple[EmailAddress | None, bool]:
        if not normalized_email:
            return None, False

        normalized_email = normalized_email.strip().lower()
        if not normalized_email:
            return None, False

        try:
            email_address = EmailAddress.objects.get(email=normalized_email)
        except EmailAddress.DoesNotExist:
            return None, False

        if email_address.collaborator_id == collaborator.id:
            return email_address, False

        email_address.collaborator = collaborator
        email_address.save(update_fields=["collaborator", "updated_at"])
        return email_address, True

    def _link_mailbox(
        self,
        *,
        collaborator: Collaborator,
        mailbox_key: str,
    ) -> bool:
        try:
            mailbox = Mailbox.objects.get(mailbox_key=mailbox_key)
        except Mailbox.DoesNotExist:
            return False

        if mailbox.owner_id == collaborator.id:
            return False

        mailbox.owner = collaborator
        mailbox.save(update_fields=["owner", "updated_at"])
        return True
    
    
    def _build_notes(
        self,
        inference_result: CollaboratorInferenceResult,
    ) -> str:
        parts: list[str] = []

        if inference_result.owner_confidence_label:
            parts.append(f"confidence_label={inference_result.owner_confidence_label}")

        parts.append(f"confidence_score={inference_result.owner_confidence_score}")

        if inference_result.inferred_identity_type:
            parts.append(f"identity_type={inference_result.inferred_identity_type}")

        if inference_result.dominant_sender_email:
            parts.append(f"dominant_sender_email={inference_result.dominant_sender_email}")

        if inference_result.owner_vs_sender_mismatch:
            parts.append("owner_vs_sender_mismatch=true")

        if inference_result.all_emails:
            parts.append(f"all_emails={', '.join(inference_result.all_emails[:10])}")

        if inference_result.all_display_names:
            parts.append(
                f"all_display_names={', '.join(inference_result.all_display_names[:10])}"
            )

        parts.append(f"top_from_email_count={inference_result.top_from_email_count}")
        parts.append(f"top_name_count={inference_result.top_name_count}")

        return " | ".join(parts)