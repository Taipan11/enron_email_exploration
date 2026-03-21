from __future__ import annotations

from enron.domain.email_payload import MessagePayload
from enron.normalization.email_normalization_service import EmailNormalizationService
from enron.normalization.email_signature_service import EmailSignatureService
from enron.hashing.email_hashing_service import EmailHashingService
from enron.parser.email_header_parser import EmailHeaderParserService
from enron.parser.email_body_parser_service import EmailBodyParserService
from enron.parser.email_attachment_parser_service import EmailAttachmentParserService
from enron.inference.email_thread_inference_service import EmailThreadInferenceService


class EmailParserService:
    """
    Orchestrateur principal de parsing email.

    Il coordonne:
    - parsing des headers
    - parsing du body
    - parsing des attachments
    - inférence de thread
    - calcul des hash métier
    - assemblage du MessagePayload
    """

    def __init__(
        self,
        normalizer: EmailNormalizationService | None = None,
        header_parser: EmailHeaderParserService | None = None,
        body_parser: EmailBodyParserService | None = None,
        signature_service: EmailSignatureService | None = None,
        thread_inference_service: EmailThreadInferenceService | None = None,
        attachment_parser: EmailAttachmentParserService | None = None,
        hashing_service: EmailHashingService | None = None,
    ) -> None:
        self.normalizer = normalizer or EmailNormalizationService()
        self.signature_service = signature_service or EmailSignatureService(self.normalizer)

        self.header_parser = header_parser or EmailHeaderParserService(self.normalizer)
        self.body_parser = body_parser or EmailBodyParserService(
            self.normalizer,
            self.signature_service,
        )
        self.thread_inference_service = (
            thread_inference_service or EmailThreadInferenceService(self.normalizer)
        )
        self.attachment_parser = attachment_parser or EmailAttachmentParserService(
            self.normalizer
        )
        self.hashing_service = hashing_service or EmailHashingService(self.normalizer)

    def parse_email(self, raw_email: str | bytes) -> MessagePayload:
        try:
            raw_email_text = self._to_text(raw_email)
            raw_headers = self._extract_raw_headers(raw_email_text)

            parsed_headers = self.header_parser.parse(raw_headers)
            parsed_body = self.body_parser.parse(raw_email)
            attachments = self.attachment_parser.extract_attachments_from_raw_email(raw_email)

            sender_email = parsed_headers.sender.email if parsed_headers.sender else None
            
            thread_inference = self.thread_inference_service.infer(
                subject_raw=parsed_headers.subject_raw,
                message_id=parsed_headers.message_id,
                in_reply_to=parsed_headers.in_reply_to,
                references=parsed_headers.references,
                body_clean=parsed_body.body_clean,
            )

            content_hash = self.hashing_service.build_content_hash(
                sender_email=parsed_headers.sender.email if parsed_headers.sender else None,
                subject_normalized=parsed_headers.subject_normalized,
                body_clean=parsed_body.body_clean,
            )

            canonical_hash = self.hashing_service.build_canonical_hash(
                sender_email=sender_email,
                sent_at=parsed_headers.sent_at,
                subject_normalized=parsed_headers.subject_normalized,
                body_clean=parsed_body.body_clean,
                recipients=[
                    {
                        "recipient_type": recipient.recipient_type,
                        "email": (
                            recipient.email_address.email
                            if recipient.email_address else None
                        ),
                    }
                    for recipient in parsed_headers.recipients
                ],
                references=[
                    reference.referenced_message_id
                    for reference in parsed_headers.references
                ],
                attachments=[
                    {
                        "filename": attachment.filename,
                        "mime_type": attachment.mime_type,
                        "size_bytes": attachment.size_bytes,
                        "sha256": attachment.sha256,
                    }
                    for attachment in attachments
                ],
            )

            payload_fields = parsed_headers.to_message_payload_fields()

            payload_fields.update(
                {
                    "body_clean": parsed_body.body_clean,
                    "body_html_clean": parsed_body.body_html_clean,
                    "signature": parsed_body.signature,
                    "has_attachments": bool(attachments),
                    "attachment_count": len(attachments),
                    "quoted_line_count": thread_inference.quoted_line_count,
                    "attachments": attachments,
                    "is_response": thread_inference.is_response,
                    "is_forward": thread_inference.is_forward,
                    "response_to_message_id": thread_inference.response_to_message_id,
                    "response_to_message_id_source": thread_inference.response_to_message_id_source,
                    "thread_root_message_id": thread_inference.thread_root_message_id,
                    "references_depth": thread_inference.references_depth,
                    "content_hash": content_hash,
                    "canonical_hash": canonical_hash,
                }
            )

            return MessagePayload(
                parse_ok=True,
                parse_error=None,
                **payload_fields,
            )

        except Exception as exc:
            return MessagePayload(
                parse_ok=False,
                parse_error=f"{type(exc).__name__}: {exc}",
            )

    def _extract_raw_headers(self, raw_email: str) -> str:
        for separator in ("\r\n\r\n", "\n\n", "\r\r"):
            if separator in raw_email:
                return raw_email.split(separator, 1)[0]
        return raw_email

    def _to_text(self, raw_email: str | bytes) -> str:
        if isinstance(raw_email, bytes):
            return raw_email.decode("utf-8", errors="replace")
        return raw_email