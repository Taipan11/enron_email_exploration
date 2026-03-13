from enron.domain.email_payload import (
    EmailAddressPayload,
    MessagePayload,
)
from enron.services.normalization.email_normalization_service import EmailNormalizationService
from enron.services.parser.email_header_parser import EmailHeaderParserService
from enron.services.normalization.email_signature_service import EmailSignatureService
from enron.services.inference.email_thread_inference_service import EmailThreadInferenceService
from enron.services.parser.email_attachment_parser_service import EmailAttachmentParserService


class EmailParserService:
    """
    Service d'assemblage du message logique.
    """

    def __init__(
        self,
        normalizer: EmailNormalizationService | None = None,
        header_parser: EmailHeaderParserService | None = None,
        signature_service: EmailSignatureService | None = None,
        thread_inference_service: EmailThreadInferenceService | None = None,
        attachment_parser: EmailAttachmentParserService | None = None
    ) -> None:
        self.normalizer = normalizer or EmailNormalizationService()
        self.header_parser = header_parser or EmailHeaderParserService(self.normalizer)
        self.signature_service = signature_service or EmailSignatureService(self.normalizer)
        self.thread_inference_service = (
            thread_inference_service or EmailThreadInferenceService(self.normalizer)
        )
        self.attachment_parser = attachment_parser or EmailAttachmentParserService()

    def parse_email(
        self,
        raw_email: str,
        *,
        mime_type: str | None = None,
        is_html_body: bool = False,
    ) -> MessagePayload:
        try:
            raw_headers, raw_body = self._split_headers_and_body(raw_email)

            parsed_headers = self.header_parser.parse_header_block(raw_headers)

            if is_html_body:
                normalized_body = self.normalizer.normalize_html_body(raw_body)
            else:
                normalized_body = self.normalizer.clean_body_text(raw_body)

            body_without_signature, signature = self.signature_service.split_signature(
                normalized_body
            )

            recipients = self.header_parser.extract_recipients(parsed_headers)
            references = self.header_parser.extract_references(parsed_headers)
            
            attachments = self.attachment_parser.extract_attachments_from_raw_email(raw_email)
            if not attachments:
                attachments = self.attachment_parser.extract_attachments(parsed_headers)


            subject_raw = parsed_headers.get_first("Subject")
            in_reply_to = self.header_parser.extract_in_reply_to(parsed_headers)
            message_id = self.header_parser.extract_message_id(parsed_headers)

            sender_email = self.header_parser.extract_sender_email(parsed_headers)
            sender = EmailAddressPayload(email=sender_email)

            thread_inference = self.thread_inference_service.build_thread_inference(
                subject_raw=subject_raw,
                message_id=message_id,
                in_reply_to=in_reply_to,
                references=references,
                body_clean=body_without_signature,
            )

            return MessagePayload(
                parse_ok=True,
                parse_error=None,
                message_id=message_id,
                in_reply_to=in_reply_to,
                sender=sender,
                sent_at=self.header_parser.extract_sent_at(parsed_headers),
                subject_normalized=self.header_parser.extract_subject_normalized(parsed_headers),
                body_clean=body_without_signature,
                signature=signature,
                mime_type=mime_type,
                content_type_header=self.header_parser.extract_content_type_header(parsed_headers),
                has_attachments=bool(attachments),
                attachment_count=len(attachments),
                is_response=thread_inference.is_response,
                is_forward=thread_inference.is_forward,
                response_to_message_id=thread_inference.response_to_message_id,
                response_to_message_id_source=thread_inference.response_to_message_id_source,
                thread_root_message_id=thread_inference.thread_root_message_id,
                references_depth=thread_inference.references_depth,
                quoted_line_count=thread_inference.quoted_line_count,
                recipients=recipients,
                references=references,
                attachments=attachments,
            )

        except Exception as exc:
            return MessagePayload(
                parse_ok=False,
                parse_error=f"{type(exc).__name__}: {exc}",
            )

    def _split_headers_and_body(self, raw_email: str) -> tuple[str, str]:
        for separator in ("\r\n\r\n", "\n\n", "\r\r"):
            if separator in raw_email:
                return raw_email.split(separator, 1)

        return raw_email, ""