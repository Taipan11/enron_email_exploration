from __future__ import annotations

from collections import defaultdict
import os
import re
from dataclasses import dataclass
from pathlib import Path
from enron.domain.email_payload import FolderPayload

@dataclass(slots=True)
class FolderRecord:
    file_path: str
    mailbox_owner: str | None
    mailbox_key: str | None
    folder_name: str | None
    folder_type: str | None
    folder_topic: str | None
    folder_path: str | None
    folder_depth: int
    file_name: str
    path_depth: int


class EmailFolderNormalizationService:
    """
    Service dédié à l'uniformisation des folders email.

    Règle métier :
    - si un dossier correspond à une catégorie connue, on regroupe
    - sinon on conserve une forme normalisée du nom tel quel
    - on n'utilise pas de valeurs génériques comme 'other' ou 'unknown'
    """

    DEFAULT_FOLDER_TYPE_PATTERNS = [
        (re.compile(r"^inbox$", re.I), "inbox"),
        (re.compile(r"^sent$", re.I), "sent"),
        (re.compile(r"^sent items?$", re.I), "sent"),
        (re.compile(r"^sent mail$", re.I), "sent"),
        (re.compile(r"^outbox$", re.I), "outbox"),
        (re.compile(r"^deleted items?$", re.I), "deleted"),
        (re.compile(r"^notes inbox$", re.I), "notes"),
        (re.compile(r"^discussion threads$", re.I), "discussion"),
        (re.compile(r"^all documents$", re.I), "archive"),
        (re.compile(r"^calendar$", re.I), "calendar"),
        (re.compile(r"^contacts$", re.I), "contacts"),
        (re.compile(r"^tasks?$", re.I), "tasks"),
        (re.compile(r"^to do$", re.I), "tasks"),
    ]

    DEFAULT_FOLDER_TOPIC_PATTERNS = [
        (re.compile(r"\bit\b", re.I), "it"),
        (re.compile(r"credit|loan|financ", re.I), "finance"),
        (re.compile(r"bank|west bank", re.I), "bank"),
        (re.compile(r"contract|contracts|lt contracts", re.I), "contracts"),
        (re.compile(r"ferc|regulat", re.I), "regulation"),
        (re.compile(r"transmission|power|energy|gas|electric", re.I), "energy"),
        (re.compile(r"legal|law|litigation", re.I), "legal"),
        (re.compile(r"task|to do|todo", re.I), "task_management"),
        (re.compile(r"calendar|meeting|schedule", re.I), "scheduling"),
        (re.compile(r"contact|address book", re.I), "contacts"),
        (re.compile(r"personal|family|private", re.I), "personal"),
        (re.compile(r"project|deal", re.I), "project"),
    ]

    SYSTEM_FOLDER_TYPES = {
        "inbox",
        "sent",
        "deleted",
        "notes",
        "discussion",
        "archive",
        "calendar",
        "contacts",
        "tasks",
        "outbox",
    }

    _MULTISPACE_RE = re.compile(r"\s+")
    _SEPARATOR_RE = re.compile(r"[/\\]+")
    _MAILBOX_CLEAN_RE = re.compile(r"[^a-z0-9._-]+", re.I)

    def __init__(
        self,
        folder_type_patterns: list[tuple[re.Pattern[str], str]] | None = None,
        folder_topic_patterns: list[tuple[re.Pattern[str], str]] | None = None,
    ) -> None:
        self.folder_type_patterns = folder_type_patterns or self.DEFAULT_FOLDER_TYPE_PATTERNS
        self.folder_topic_patterns = folder_topic_patterns or self.DEFAULT_FOLDER_TOPIC_PATTERNS

    def normalize_text(self, value: str | None) -> str | None:
        if value is None:
            return None

        value = value.strip()
        if not value:
            return None

        value = self._MULTISPACE_RE.sub(" ", value)
        return value or None

    def normalize_mailbox_owner(self, mailbox_owner: str | None) -> str | None:
        """
        Normalisation légère du propriétaire de mailbox.
        On ne force pas une liste fermée.
        On garde la valeur si elle n'est pas reconnue.
        """
        mailbox_owner = self.normalize_text(mailbox_owner)
        if not mailbox_owner:
            return None

        return mailbox_owner

    def normalize_mailbox_key(self, mailbox_owner: str | None) -> str | None:
        """
        Forme stable de la mailbox pour indexation / matching.
        On conserve l'identité métier sans remplacer par 'unknown'.
        """
        mailbox_owner = self.normalize_mailbox_owner(mailbox_owner)
        if not mailbox_owner:
            return None

        mailbox_key = mailbox_owner.lower()
        mailbox_key = self._MAILBOX_CLEAN_RE.sub("_", mailbox_key)
        mailbox_key = re.sub(r"_+", "_", mailbox_key).strip("_")
        return mailbox_key or None

    def normalize_folder_name(self, folder_name: str | None) -> str | None:
        """
        Normalisation légère du nom de dossier.
        """
        folder_name = self.normalize_text(folder_name)
        if not folder_name:
            return None

        return folder_name

    def normalize_folder_key(self, folder_name: str | None) -> str | None:
        """
        Forme stable pour matcher les patterns :
        - trim
        - lower
        - underscores / espaces multiples -> espace simple
        """
        folder_name = self.normalize_folder_name(folder_name)
        if not folder_name:
            return None

        folder_name = folder_name.replace("_", " ")
        folder_name = self._MULTISPACE_RE.sub(" ", folder_name).strip()
        return folder_name.lower() or None

    def normalize_folder_type(self, folder_name: str | None) -> str | None:
        """
        Retourne un type de dossier uniforme si connu.
        Sinon retourne le nom du dossier normalisé tel quel.
        """
        cleaned = self.normalize_folder_key(folder_name)
        if not cleaned:
            return None

        for pattern, label in self.folder_type_patterns:
            if pattern.match(cleaned):
                return label

        return cleaned

    def normalize_folder_topic(
        self,
        folder_name: str | None,
        folder_type: str | None = None,
    ) -> str | None:
        """
        Retourne un topic uniforme si identifiable.
        Sinon retourne None.
        """
        if not folder_name:
            return None

        if folder_type in self.SYSTEM_FOLDER_TYPES:
            return None

        cleaned = self.normalize_folder_key(folder_name)
        if not cleaned:
            return None

        for pattern, label in self.folder_topic_patterns:
            if pattern.search(cleaned):
                return label

        return None

    def extract_mailbox_owner(self, relative_parts: tuple[str, ...]) -> str | None:
        if len(relative_parts) >= 1:
            return self.normalize_mailbox_owner(relative_parts[0])
        return None

    def extract_folder_name(self, relative_parts: tuple[str, ...]) -> str | None:
        if len(relative_parts) >= 2:
            return self.normalize_folder_name(relative_parts[1])
        return None

    def extract_folder_parts(self, relative_parts: tuple[str, ...]) -> list[str]:
        """
        Retourne les segments du chemin correspondant au dossier,
        sans le mailbox owner et sans le nom de fichier final.
        """
        if len(relative_parts) >= 3:
            return [self.normalize_folder_name(part) or part for part in relative_parts[1:-1]]
        if len(relative_parts) >= 2:
            normalized = self.normalize_folder_name(relative_parts[1])
            return [normalized] if normalized else [relative_parts[1]]
        return []

    def normalize_folder_path(self, folder_parts: list[str]) -> str | None:
        normalized_parts = [part for part in folder_parts if part]
        if not normalized_parts:
            return None
        return "/".join(normalized_parts)

    def get_folder_name_from_path(self, file_path: Path, root: Path) -> str | None:
        record = self.build_record(file_path=file_path, root=root)
        return record.folder_name
    
    def get_folder_name_from_path(self, file_path: Path, root: Path) -> str | None:
        record = self.build_record(file_path=file_path, root=root)
        return record.folder_name
    
    def is_email_file(self, file_path: Path) -> bool:
        return file_path.is_file() and not file_path.name.startswith(".")
    
    def iter_email_files(self, root: Path):
        for dirpath, _, filenames in os.walk(root):
            for filename in filenames:
                file_path = Path(dirpath) / filename
                if self.is_email_file(file_path):
                    yield file_path

    def iter_email_files_by_mailbox(self, root: Path) -> dict[str, list[Path]]:
        files_by_mailbox: dict[str, list[Path]] = defaultdict(list)

        for file_path in self.iter_email_files(root):
            try:
                rel_parts = file_path.relative_to(root).parts
            except ValueError:
                continue

            mailbox_owner = self.extract_mailbox_owner(rel_parts)
            if not mailbox_owner:
                continue

            files_by_mailbox[mailbox_owner].append(file_path)

        return dict(files_by_mailbox)
                  
    def build_record(self, file_path: Path, root: Path) -> FolderRecord:
        """
        Construit un FolderRecord à partir d'un chemin de fichier.
        """
        rel_parts = file_path.relative_to(root).parts

        mailbox_owner = self.extract_mailbox_owner(rel_parts)
        mailbox_key = self.normalize_mailbox_key(mailbox_owner)

        folder_name = self.extract_folder_name(rel_parts)
        folder_type = self.normalize_folder_type(folder_name)
        folder_topic = self.normalize_folder_topic(folder_name, folder_type)

        folder_parts = self.extract_folder_parts(rel_parts)
        folder_path = self.normalize_folder_path(folder_parts)

        folder_depth = len(folder_parts)
        path_depth = len(rel_parts)

        return FolderRecord(
            file_path=str(file_path),
            mailbox_owner=mailbox_owner,
            mailbox_key=mailbox_key,
            folder_name=folder_name,
            folder_type=folder_type,
            folder_topic=folder_topic,
            folder_path=folder_path,
            folder_depth=folder_depth,
            file_name=file_path.name,
            path_depth=path_depth,
        )
    
    def build_folder_payload(self, file_path: Path, root: Path) -> FolderPayload:
        record = self.build_record(file_path=file_path, root=root)
        return FolderPayload(
            mailbox_key=record.mailbox_key,
            folder_path=record.folder_path,
            folder_name=record.folder_name,
            folder_type=record.folder_type,
            folder_topic=record.folder_topic,
        )