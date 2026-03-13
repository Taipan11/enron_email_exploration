from __future__ import annotations

from enron.domain.email_payload import (
    MessageReferencePayload,
    MessageThreadInferencePayload,
)
from enron.services.normalization.email_normalization_service import (
    EmailNormalizationService,
)

class EmailThreadInferenceService:
    """
    Service métier chargé de déduire les informations de thread
    à partir de valeurs déjà extraites / normalisées.

    Ce service ne parse pas les headers lui-même.
    Il consomme seulement :
    - le sujet brut
    - le message_id
    - le in_reply_to
    - les références déjà normalisées
    - le body déjà nettoyé
    """

    def __init__(self, normalizer: EmailNormalizationService) -> None:
        self.normalizer = normalizer

    def build_thread_inference(
        self,
        *,
        subject_raw: str | None,
        message_id: str | None,
        in_reply_to: str | None,
        references: list[MessageReferencePayload],
        body_clean: str | None,
    ) -> MessageThreadInferencePayload:
        """
        Construit un MessageThreadInferencePayload simple.

        Règles :
        - is_response = vrai si in_reply_to, references ou sujet de réponse
        - is_forward = vrai si sujet de transfert
        - response_to_message_id :
            1. in_reply_to si présent
            2. sinon dernière référence
        - thread_root_message_id :
            1. première référence si présente
            2. sinon in_reply_to
            3. sinon message_id
        - references_depth = nombre de références valides
        - quoted_line_count = nombre de lignes citées dans le body
        """
        reference_ids = self._extract_reference_ids(references)

        is_reply_by_subject = self.normalizer.is_reply_subject(subject_raw)
        is_forward = self.normalizer.is_forward_subject(subject_raw)

        response_to_message_id = None
        response_to_message_id_source = None

        if in_reply_to:
            response_to_message_id = in_reply_to
            response_to_message_id_source = "in_reply_to"
        elif reference_ids:
            response_to_message_id = reference_ids[-1]
            response_to_message_id_source = "references"

        if reference_ids:
            thread_root_message_id = reference_ids[0]
        elif in_reply_to:
            thread_root_message_id = in_reply_to
        else:
            thread_root_message_id = message_id

        quoted_line_count = self.normalizer.count_quoted_lines(body_clean)

        is_response = response_to_message_id is not None
        looks_like_response = bool(
            is_response
            or is_reply_by_subject
            or self.normalizer.count_quoted_lines(body_clean) > 0
        )

        return MessageThreadInferencePayload(
            is_response=is_response,
            looks_like_response=looks_like_response,
            is_forward=is_forward,
            response_to_message_id=response_to_message_id,
            response_to_message_id_source=response_to_message_id_source,
            thread_root_message_id=thread_root_message_id,
            references_depth=len(reference_ids),
            quoted_line_count=quoted_line_count,
        )

    def _extract_reference_ids(
        self,
        references: list[MessageReferencePayload],
    ) -> list[str]:
        """
        Extrait les referenced_message_id non nuls.
        """
        result: list[str] = []

        for item in references:
            if item.referenced_message_id:
                result.append(item.referenced_message_id)

        return result