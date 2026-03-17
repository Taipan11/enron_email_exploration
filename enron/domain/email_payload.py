from dataclasses import dataclass, field
from datetime import datetime

@dataclass(slots=True)
class EmailAddressPayload:
    """
    Représente une adresse email normalisée, proche du modèle EmailAddress.
    """
    email: str | None = None
    display_name: str | None = None

    local_part: str | None = field(init=False, default=None)
    domain: str | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        if not self.email:
            return

        email_normalized = self.email.strip().lower()
        self.email = email_normalized

        if "@" not in email_normalized:
            return

        local_part, domain = email_normalized.rsplit("@", 1)
        self.local_part = local_part or None
        self.domain = domain or None




@dataclass(slots=True)
class MailboxPayload:
    """
    Représente une mailbox logique, proche du modèle Mailbox.
    """
    mailbox_key: str | None = None
    source_root_path: str | None = None

    def __post_init__(self) -> None:
        if self.mailbox_key:
            self.mailbox_key = self.mailbox_key.strip() or None

        if self.source_root_path:
            self.source_root_path = self.source_root_path.strip() or None



@dataclass(slots=True)
class FolderPayload:
    mailbox_key: str
    folder_path: str | None = None
    folder_name: str | None = None
    folder_type: str | None = None
    folder_topic: str | None = None


@dataclass(slots=True)
class MessageRecipientPayload:
    """
    Représente un destinataire d'un message, proche du modèle MessageRecipient.
    """
    recipient_type: str
    email_address: EmailAddressPayload = field(default_factory=EmailAddressPayload)
    display_name: str | None = None

    def __post_init__(self) -> None:
        if self.recipient_type:
            self.recipient_type = self.recipient_type.strip().lower()

        if self.display_name:
            self.display_name = self.display_name.strip() or None


@dataclass(slots=True)
class MessageReferencePayload:
    """
    Représente une référence individuelle extraite du header References,
    proche du modèle MessageReference.
    """
    referenced_message_id: str | None = None

    def __post_init__(self) -> None:
        if self.referenced_message_id:
            self.referenced_message_id = self.referenced_message_id.strip() or None

@dataclass(slots=True)
class MessageThreadInferencePayload:
    is_response: bool = False
    looks_like_response: bool = False
    is_forward: bool = False

    response_to_message_id: str | None = None
    response_to_message_id_source: str | None = None  # "in_reply_to", "references", "quoted_header", "subject_time_author", ...

    thread_root_message_id: str | None = None

    references_depth: int = 0
    quoted_line_count: int = 0

    confidence: float = 0.0
    normalized_subject: str | None = None
    decision_notes: list[str] | None = None

@dataclass(slots=True)
class AttachmentPayload:
    """
    Représente une pièce jointe extraite d'un message, proche du modèle Attachment.
    """
    filename: str | None = None
    mime_type: str | None = None
    content_id: str | None = None
    size_bytes: int | None = None
    sha256: str | None = None
    storage_path: str | None = None

    def __post_init__(self) -> None:
        if self.filename:
            self.filename = self.filename.strip() or None

        if self.mime_type:
            self.mime_type = self.mime_type.strip().lower() or None

        if self.content_id:
            self.content_id = self.content_id.strip() or None

        if self.sha256:
            self.sha256 = self.sha256.strip().lower() or None

        if self.storage_path:
            self.storage_path = self.storage_path.strip() or None

        if self.size_bytes is not None and self.size_bytes < 0:
            self.size_bytes = None


@dataclass(slots=True)
class MessagePayload:
    """
    Représente le message logique, proche du modèle Message.
    """
    parse_ok: bool

    parse_error: str | None = None

    message_id: str | None = None
    in_reply_to: str | None = None

    sender: EmailAddressPayload = field(default_factory=EmailAddressPayload)
    sent_at: datetime | None = None

    subject_normalized: str | None = None
    body_clean: str | None = None
    signature: str | None = None

    mime_type: str | None = None
    content_type_header: str | None = None

    has_attachments: bool = False
    attachment_count: int = 0

    is_response: bool = False
    is_forward: bool = False
    response_to_message_id: str | None = None
    response_to_message_id_source: str | None = None
    thread_root_message_id: str | None = None
    references_depth: int = 0
    quoted_line_count: int = 0

    recipients: list[MessageRecipientPayload] = field(default_factory=list)
    references: list[MessageReferencePayload] = field(default_factory=list)
    attachments: list[AttachmentPayload] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.parse_error:
            self.parse_error = self.parse_error.strip() or None

        if self.message_id:
            self.message_id = self.message_id.strip() or None

        if self.in_reply_to:
            self.in_reply_to = self.in_reply_to.strip() or None

        if self.subject_normalized:
            self.subject_normalized = self.subject_normalized.strip() or None

        if self.body_clean:
            self.body_clean = self.body_clean.strip() or None

        if self.signature:
            self.signature = self.signature.strip() or None

        if self.mime_type:
            self.mime_type = self.mime_type.strip().lower() or None

        if self.content_type_header:
            self.content_type_header = self.content_type_header.strip() or None

        if self.response_to_message_id:
            self.response_to_message_id = self.response_to_message_id.strip() or None

        if self.response_to_message_id_source:
            self.response_to_message_id_source = (
                self.response_to_message_id_source.strip().lower() or None
            )

        if self.thread_root_message_id:
            self.thread_root_message_id = self.thread_root_message_id.strip() or None

        if self.attachment_count < 0:
            self.attachment_count = 0

        if self.references_depth < 0:
            self.references_depth = 0

        if self.quoted_line_count < 0:
            self.quoted_line_count = 0


@dataclass(slots=True)
class MessageOccurrencePayload:
    """
    Représente la présence physique d'un message dans une mailbox/folder/source_file,
    proche du modèle MessageOccurrence.
    """
    source_file: str | None = None
    folder: FolderPayload = field(default_factory=FolderPayload)


@dataclass(slots=True)
class ParsedEmailPayload:
    """
    Payload racine renvoyé par le parser.

    Il sépare clairement :
    - le message logique
    - son occurrence physique dans le maildir
    """
    message: MessagePayload
    occurrence: MessageOccurrencePayload


@dataclass(slots=True)
class ValidationResult:
    """
    Résultat de validation d'un ParsedEmailPayload.
    """
    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ParsedHeaderMetadata:
    x_from: str | None = None
    x_to: str | None = None
    x_cc: str | None = None
    x_bcc: str | None = None
    x_folder: str | None = None
    x_origin: str | None = None
    x_filename: str | None = None
    mime_version: str | None = None
    content_transfer_encoding: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "x_from",
            "x_to",
            "x_cc",
            "x_bcc",
            "x_folder",
            "x_origin",
            "x_filename",
            "mime_version",
            "content_transfer_encoding",
        ):
            value = getattr(self, field_name)
            if isinstance(value, str):
                setattr(self, field_name, value.strip() or None)


@dataclass(slots=True)
class ParsedMessageHeaders:
    sender: EmailAddressPayload = field(default_factory=EmailAddressPayload)
    reply_to: EmailAddressPayload = field(default_factory=EmailAddressPayload)
    sent_at: datetime | None = None

    message_id: str | None = None
    in_reply_to: str | None = None

    subject_raw: str | None = None
    subject_normalized: str | None = None

    mime_type: str | None = None
    content_type_header: str | None = None

    recipients: list[MessageRecipientPayload] = field(default_factory=list)
    references: list[MessageReferencePayload] = field(default_factory=list)

    is_response: bool = False
    looks_like_response: bool = False
    is_forward: bool = False

    response_to_message_id: str | None = None
    response_to_message_id_source: str | None = None
    thread_root_message_id: str | None = None
    references_depth: int = 0

    metadata: ParsedHeaderMetadata = field(default_factory=ParsedHeaderMetadata)

    def __post_init__(self) -> None:
        for field_name in (
            "message_id",
            "in_reply_to",
            "subject_raw",
            "subject_normalized",
            "mime_type",
            "content_type_header",
            "response_to_message_id",
            "response_to_message_id_source",
            "thread_root_message_id",
        ):
            value = getattr(self, field_name)
            if isinstance(value, str):
                cleaned = value.strip()
                if field_name == "mime_type":
                    cleaned = cleaned.lower()
                setattr(self, field_name, cleaned or None)

        if self.references_depth < 0:
            self.references_depth = 0

    def to_message_payload_fields(self) -> dict:
        """
        Retourne uniquement les champs directement mappables vers MessagePayload.
        Utile pour construire MessagePayload plus proprement.
        """
        return {
            "message_id": self.message_id,
            "in_reply_to": self.in_reply_to,
            "sender": self.sender,
            "sent_at": self.sent_at,
            "subject_normalized": self.subject_normalized,
            "mime_type": self.mime_type,
            "content_type_header": self.content_type_header,
            "is_response": self.is_response,
            "is_forward": self.is_forward,
            "response_to_message_id": self.response_to_message_id,
            "response_to_message_id_source": self.response_to_message_id_source,
            "thread_root_message_id": self.thread_root_message_id,
            "references_depth": self.references_depth,
            "recipients": self.recipients,
            "references": self.references,
        }
    

@dataclass(slots=True)
class ParsedMessageBody:
    body_raw: str | None = None
    body_clean: str | None = None
    body_html_clean: str | None = None
    signature: str | None = None

    has_html_body: bool = False
    has_plain_text_body: bool = False

    quoted_text: str | None = None
    quoted_line_count: int = 0

    keywords: list[str] = field(default_factory=list)
    snippet: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "body_raw",
            "body_clean",
            "body_html_clean",
            "signature",
            "quoted_text",
            "snippet",
        ):
            value = getattr(self, field_name)
            if isinstance(value, str):
                setattr(self, field_name, value.strip() or None)

        if self.quoted_line_count < 0:
            self.quoted_line_count = 0