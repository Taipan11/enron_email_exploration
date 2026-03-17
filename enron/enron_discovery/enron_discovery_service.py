from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class DiscoveredMailbox:
    mailbox_key: str
    mailbox_path: Path


@dataclass(slots=True)
class DiscoveredEmailFile:
    source_file: str
    source_path: Path
    relative_path: str
    mailbox_key: str
    folder_name: str
    mailbox_path: Path


class EnronDiscoveryService:
    """
    Service chargé de découvrir les mailboxes et les fichiers email
    dans une racine de type maildir Enron.

    Hypothèse simple :
    - chaque premier dossier sous root correspond à une mailbox
    - chaque fichier sous une mailbox est un email brut
    """

    def discover_mailboxes(
        self,
        *,
        root: Path,
        max_mailboxes: int | None = None,
    ) -> list[DiscoveredMailbox]:
        self._validate_root(root)

        mailboxes: list[DiscoveredMailbox] = []

        for path in sorted(root.iterdir(), key=lambda p: p.name):
            if not path.is_dir():
                continue

            if path.name.startswith("."):
                continue

            mailboxes.append(
                DiscoveredMailbox(
                    mailbox_key=path.name,
                    mailbox_path=path,
                )
            )

        if max_mailboxes is not None:
            mailboxes = mailboxes[:max_mailboxes]

        return mailboxes

    def discover_email_files(
        self,
        *,
        mailbox_root: Path,
        max_files: int | None = None,
    ) -> list[DiscoveredEmailFile]:
        if not mailbox_root.exists():
            raise FileNotFoundError(f"Le dossier mailbox n'existe pas : {mailbox_root}")

        if not mailbox_root.is_dir():
            raise NotADirectoryError(f"Le chemin mailbox n'est pas un dossier : {mailbox_root}")

        mailbox_key = mailbox_root.name
        results: list[DiscoveredEmailFile] = []

        for path in sorted(mailbox_root.rglob("*"), key=lambda p: str(p)):
            if not path.is_file():
                continue

            if path.name.startswith("."):
                continue

            relative_path = path.relative_to(mailbox_root)

            results.append(
                DiscoveredEmailFile(
                    source_file=str(path),
                    source_path=path,
                    relative_path=relative_path.as_posix(),
                    mailbox_key=mailbox_key,
                    folder_name=path.parent.name,
                    mailbox_path=mailbox_root,
                )
            )

        if max_files is not None:
            results = results[:max_files]

        return results

    def list_email_files(
        self,
        *,
        root: Path,
        max_mailboxes: int | None = None,
        max_files_per_mailbox: int | None = None,
    ) -> list[DiscoveredEmailFile]:
        results: list[DiscoveredEmailFile] = []

        mailboxes = self.discover_mailboxes(
            root=root,
            max_mailboxes=max_mailboxes,
        )

        for mailbox in mailboxes:
            results.extend(
                self.discover_email_files(
                    mailbox_root=mailbox.mailbox_path,
                    max_files=max_files_per_mailbox,
                )
            )

        return results

    def _validate_root(self, root: Path) -> None:
        if not root.exists():
            raise FileNotFoundError(f"Le dossier racine n'existe pas : {root}")

        if not root.is_dir():
            raise NotADirectoryError(f"Le chemin n'est pas un dossier : {root}")