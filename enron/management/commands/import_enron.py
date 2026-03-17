from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path
from time import perf_counter

from django.core.management.base import BaseCommand, CommandError

from enron.create_database.enron_import_service import EnronImportService


class Command(BaseCommand):
    help = "Importe les emails Enron depuis un répertoire maildir."

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--root",
            type=str,
            required=True,
            help="Chemin racine du corpus Enron.",
        )
        parser.add_argument(
            "--max-mailboxes",
            type=int,
            default=None,
            help="Limite le nombre de mailboxes à traiter.",
        )
        parser.add_argument(
            "--max-files-per-mailbox",
            type=int,
            default=None,
            help="Limite le nombre de fichiers traités par mailbox.",
        )
        parser.add_argument(
            "--persist-invalid",
            action="store_true",
            help="Persiste aussi les payloads invalides.",
        )
        parser.add_argument(
            "--stop-on-error",
            action="store_true",
            help="Arrête immédiatement en cas d'erreur.",
        )
        parser.add_argument(
            "--debug-output-path",
            type=str,
            default=None,
            help="Chemin d'un fichier JSONL pour écrire les payloads de debug.",
        )

    def handle(self, *args, **options) -> None:
        root = self._resolve_root(options["root"])
        debug_path = self._resolve_debug_path(options.get("debug_output_path"))

        service = EnronImportService()

        self.stdout.write(self.style.NOTICE("Démarrage de l'import Enron..."))
        self.stdout.write(f"Racine : {root}")

        max_mailboxes = options.get("max_mailboxes")
        max_files_per_mailbox = options.get("max_files_per_mailbox")
        persist_invalid = options.get("persist_invalid", False)
        stop_on_error = options.get("stop_on_error", False)

        if max_mailboxes is not None:
            self.stdout.write(f"Max mailboxes : {max_mailboxes}")

        if max_files_per_mailbox is not None:
            self.stdout.write(f"Max fichiers par mailbox : {max_files_per_mailbox}")

        self.stdout.write(f"Persister les invalides : {'oui' if persist_invalid else 'non'}")
        self.stdout.write(f"Arrêt sur erreur : {'oui' if stop_on_error else 'non'}")

        if debug_path is not None:
            self.stdout.write(f"Fichier de debug : {debug_path}")

        started_at = perf_counter()

        try:
            report = service.run(
                root=root,
                max_mailboxes=max_mailboxes,
                max_files_per_mailbox=max_files_per_mailbox,
                stop_on_error=stop_on_error,
                persist_invalid=persist_invalid,
                debug_output_path=debug_path,
            )
        except Exception as exc:
            raise CommandError(f"Échec de l'import Enron : {type(exc).__name__}: {exc}") from exc

        elapsed = perf_counter() - started_at

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Import terminé."))
        self.stdout.write(f"Durée : {elapsed:.2f} s")
        self.stdout.write(f"Total découverts : {report.total_discovered}")
        self.stdout.write(f"Total traités : {report.total_processed}")
        self.stdout.write(f"Total persistés : {report.total_persisted}")
        self.stdout.write(f"Total créés : {report.total_created}")
        self.stdout.write(f"Total mis à jour : {report.total_updated}")
        self.stdout.write(f"Échecs validation : {report.total_failed_validation}")
        self.stdout.write(f"Erreurs de parsing : {report.total_parse_errors}")
        self.stdout.write(f"Warnings : {report.total_warnings}")

        self._print_messages(
            title="Erreurs de traitement",
            messages=report.processing_errors,
            style=self.style.WARNING,
        )
        self._print_messages(
            title="Erreurs de validation",
            messages=report.validation_errors,
            style=self.style.WARNING,
        )
        self._print_messages(
            title="Warnings de validation",
            messages=report.validation_warnings,
            style=self.style.WARNING,
        )

    def _resolve_root(self, raw_root: str) -> Path:
        root = Path(raw_root).expanduser().resolve()

        if not root.exists():
            raise CommandError(f"Le répertoire racine n'existe pas : {root}")

        if not root.is_dir():
            raise CommandError(f"Le chemin racine n'est pas un répertoire : {root}")

        return root

    def _resolve_debug_path(self, raw_debug_path: str | None) -> Path | None:
        if not raw_debug_path:
            return None

        debug_path = Path(raw_debug_path).expanduser().resolve()

        parent = debug_path.parent
        if not parent.exists():
            raise CommandError(
                f"Le dossier parent du fichier de debug n'existe pas : {parent}"
            )

        if not parent.is_dir():
            raise CommandError(
                f"Le parent du fichier de debug n'est pas un répertoire : {parent}"
            )

        return debug_path

    def _print_messages(
        self,
        *,
        title: str,
        messages: list[str],
        style,
        max_items: int = 20,
    ) -> None:
        if not messages:
            return

        self.stdout.write("")
        self.stdout.write(style(f"{title} :"))

        for message in messages[:max_items]:
            self.stdout.write(f"- {message}")

        remaining = len(messages) - max_items
        if remaining > 0:
            self.stdout.write(f"... {remaining} éléments supplémentaires")