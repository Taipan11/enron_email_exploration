from __future__ import annotations

from pathlib import Path
from time import perf_counter

from django.core.management.base import BaseCommand, CommandError

from enron.services.enron_import_service import EnronImportService


class Command(BaseCommand):
    help = "Importe les emails Enron depuis un répertoire maildir."

    def add_arguments(self, parser) -> None:
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
        root = Path(options["root"]).expanduser().resolve()
        if not root.exists():
            raise CommandError(f"Le répertoire racine n'existe pas : {root}")

        if not root.is_dir():
            raise CommandError(f"Le chemin racine n'est pas un répertoire : {root}")

        debug_output_path = options.get("debug_output_path")
        debug_path = (
            Path(debug_output_path).expanduser().resolve()
            if debug_output_path
            else None
        )

        service = EnronImportService()

        self.stdout.write(self.style.NOTICE("Démarrage de l'import Enron..."))
        self.stdout.write(f"Racine : {root}")
        if options.get("max_mailboxes") is not None:
            self.stdout.write(f"Max mailboxes : {options['max_mailboxes']}")
        if options.get("max_files_per_mailbox") is not None:
            self.stdout.write(
                f"Max fichiers par mailbox : {options['max_files_per_mailbox']}"
            )
        if debug_path is not None:
            self.stdout.write(f"Fichier de debug : {debug_path}")

        started_at = perf_counter()

        report = service.run(
            root=root,
            max_mailboxes=options.get("max_mailboxes"),
            max_files_per_mailbox=options.get("max_files_per_mailbox"),
            stop_on_error=options.get("stop_on_error", False),
            persist_invalid=options.get("persist_invalid", False),
            debug_output_path=debug_path,
        )

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

        if report.processing_errors:
            self.stdout.write("")
            self.stdout.write(self.style.WARNING("Erreurs de traitement :"))
            for message in report.processing_errors[:20]:
                self.stdout.write(f"- {message}")
            if len(report.processing_errors) > 20:
                self.stdout.write(
                    f"... {len(report.processing_errors) - 20} erreurs supplémentaires"
                )

        if report.validation_errors:
            self.stdout.write("")
            self.stdout.write(self.style.WARNING("Erreurs de validation :"))
            for message in report.validation_errors[:20]:
                self.stdout.write(f"- {message}")
            if len(report.validation_errors) > 20:
                self.stdout.write(
                    f"... {len(report.validation_errors) - 20} erreurs supplémentaires"
                )

        if report.validation_warnings:
            self.stdout.write("")
            self.stdout.write(self.style.WARNING("Warnings de validation :"))
            for message in report.validation_warnings[:20]:
                self.stdout.write(f"- {message}")
            if len(report.validation_warnings) > 20:
                self.stdout.write(
                    f"... {len(report.validation_warnings) - 20} warnings supplémentaires"
                )