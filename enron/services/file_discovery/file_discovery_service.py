from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class DiscoveredEmailFile:
    """
    Représente un fichier email découvert sur disque.
    """

    source_file: str
    relative_path: str
    folder_name: str
    mailbox_key: str

class FileDiscoveryService:
    """
    Service chargé de découvrir les fichiers email à importer
    depuis une racine de type maildir Enron.

    Hypothèse simple :
    - chaque fichier est un email brut
    - folder_name = dernier dossier parent
    - mailbox_key = premier segment du chemin relatif sous la racine
    """

    def list_email_files(
        self,
        *,
        root: Path,
        max_mailboxes: int | None = None,
        max_files_per_mailbox: int | None = None,
    ) -> list[DiscoveredEmailFile]:
        """
        Retourne les fichiers emails trouvés sous `root`.
        """
        if not root.exists():
            raise FileNotFoundError(f"Le dossier racine n'existe pas : {root}")

        if not root.is_dir():
            raise NotADirectoryError(f"Le chemin n'est pas un dossier : {root}")

        mailbox_buckets: dict[str, list[DiscoveredEmailFile]] = {}

        for path in root.rglob("*"):
            if not path.is_file():
                continue

            discovered = self._build_discovered_email_file(root=root, path=path)
            if discovered is None:
                continue

            mailbox_buckets.setdefault(discovered.mailbox_key, []).append(discovered)

        mailbox_keys = sorted(mailbox_buckets.keys())
        if max_mailboxes is not None:
            mailbox_keys = mailbox_keys[:max_mailboxes]

        results: list[DiscoveredEmailFile] = []

        for mailbox_key in mailbox_keys:
            files = sorted(
                mailbox_buckets[mailbox_key],
                key=lambda item: item.relative_path,
            )

            if max_files_per_mailbox is not None:
                files = files[:max_files_per_mailbox]

            results.extend(files)

        return results

    def _build_discovered_email_file(
        self,
        *,
        root: Path,
        path: Path,
    ) -> DiscoveredEmailFile | None:
        """
        Construit un objet DiscoveredEmailFile à partir d'un path.
        """
        relative_path = path.relative_to(root)

        # Ignore quelques fichiers parasites éventuels
        if path.name.startswith("."):
            return None

        relative_parts = relative_path.parts
        if not relative_parts:
            return None

        folder_name = path.parent.name
        mailbox_key = relative_parts[0]

        return DiscoveredEmailFile(
            source_file=str(path),
            relative_path=str(relative_path),
            folder_name=folder_name,
            mailbox_key=mailbox_key,
        )