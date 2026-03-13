from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field


@dataclass(slots=True)
class CollaboratorPayload:
    """
    Représente un collaborateur, proche du modèle Collaborator.
    """
    employee_key: str | None = None
    display_name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    position_title: str | None = None
    is_enron_employee: bool = True
    notes: str | None = None

    def __post_init__(self) -> None:
        if self.employee_key:
            self.employee_key = self.employee_key.strip() or None

        if self.display_name:
            self.display_name = self.display_name.strip() or None

        if self.first_name:
            self.first_name = self.first_name.strip() or None

        if self.last_name:
            self.last_name = self.last_name.strip() or None

        if self.position_title:
            self.position_title = self.position_title.strip() or None

        if self.notes:
            self.notes = self.notes.strip() or None


@dataclass(slots=True)
class MailboxCollaboratorAggregate:
    mailbox_key: str
    message_count: int = 0
    folder_paths: set[str] = field(default_factory=set)

    xfilename_counter: Counter = field(default_factory=Counter)
    from_email_counter: Counter = field(default_factory=Counter)
    from_name_counter: Counter = field(default_factory=Counter)
    xfrom_counter: Counter = field(default_factory=Counter)
    title_counter: Counter = field(default_factory=Counter)

    all_email_counter: Counter = field(default_factory=Counter)
    email_name_counter: dict[str, Counter] = field(default_factory=dict)

    def get_email_name_counter(self, email: str) -> Counter:
        if email not in self.email_name_counter:
            self.email_name_counter[email] = Counter()
        return self.email_name_counter[email]
    


@dataclass(slots=True)
class CollaboratorInferenceResult:
    mailbox_key: str
    inferred_display_name: str | None
    inferred_first_name: str | None
    inferred_last_name: str | None
    inferred_primary_email: str | None
    inferred_position_title: str | None
    inferred_identity_type: str | None
    is_corporate_mailbox_candidate: bool
    message_count: int
    folder_count: int
    all_emails: list[str]
    all_display_names: list[str]
    top_from_email_count: int
    top_name_count: int
    owner_confidence_score: float
    owner_confidence_label: str
    dominant_sender_email: str | None
    dominant_sender_name: str | None
    dominant_sender_identity_type: str | None
    owner_vs_sender_mismatch: bool