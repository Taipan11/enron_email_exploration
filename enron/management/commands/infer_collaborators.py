from __future__ import annotations

from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from enron.inference.collaborator_inference_service import (
    CollaboratorInferenceService,
)
from enron.persistence.collaborator_persistence_service import (
    CollaboratorPersistenceService,
)
from enron.inference.mailbox_alias_inference_service import (
    MailboxAliasInferenceService,
)
from enron.persistence.alias_persistence_service import (
    CollaboratorAliasPersistenceService,
)


class Command(BaseCommand):
    help = (
        "Infère les collaborateurs probables à partir des fichiers email bruts, "
        "affiche un aperçu, peut exporter les rapports et persister en base."
    )

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--root",
            type=str,
            required=True,
            help="Chemin racine du dataset maildir.",
        )
        parser.add_argument(
            "--output-dir",
            type=str,
            default=None,
            help="Dossier de sortie pour sauvegarder les rapports CSV/JSON.",
        )
        parser.add_argument(
            "--max-mailboxes",
            type=int,
            default=None,
            help="Nombre maximum de mailboxes à explorer.",
        )
        parser.add_argument(
            "--max-files-per-mailbox",
            type=int,
            default=200,
            help="Nombre maximum de messages à lire par mailbox.",
        )
        parser.add_argument(
            "--preview",
            action="store_true",
            help="Affiche un aperçu sans écrire ni persister.",
        )
        parser.add_argument(
            "--persist",
            action="store_true",
            help="Persiste les collaborateurs inférés en base.",
        )
        parser.add_argument(
            "--limit-output",
            type=int,
            default=20,
            help="Nombre maximum de résultats affichés.",
        )

    def handle(self, *args, **options) -> None:
        root = Path(options["root"]).expanduser().resolve()
        output_dir_raw: str | None = options["output_dir"]
        output_dir = Path(output_dir_raw).expanduser().resolve() if output_dir_raw else None
        max_mailboxes: int | None = options["max_mailboxes"]
        max_messages_per_mailbox: int | None = options["max_files_per_mailbox"]
        preview: bool = options["preview"]
        persist: bool = options["persist"]
        limit_output: int = options["limit_output"]

        if not root.exists():
            raise CommandError(f"Le dossier root n'existe pas: {root}")

        if not root.is_dir():
            raise CommandError(f"Le chemin root n'est pas un dossier: {root}")

        if preview and persist:
            raise CommandError("Tu ne peux pas utiliser --preview et --persist en même temps.")

        inference_service = CollaboratorInferenceService()
        alias_service = MailboxAliasInferenceService()
        persistence_service = CollaboratorPersistenceService()
        alias_persistence_service = CollaboratorAliasPersistenceService()

        self.stdout.write("Inférence des collaborateurs en cours...")

        collaborators_df, summary, tables = inference_service.explore_collaborators(
            root=root,
            max_messages_per_mailbox=max_messages_per_mailbox,
            max_mailboxes=max_mailboxes,
        )

        results = inference_service.build_inference_results_from_dataframe(collaborators_df)

        self.stdout.write(self.style.SUCCESS(f"{len(results)} collaborateur(s) inféré(s)."))
        self.stdout.write("")

        for result in results[:limit_output]:
            self.stdout.write(
                f"- mailbox_key={result.mailbox_key} | "
                f"display_name={result.inferred_display_name!r} | "
                f"primary_email={result.inferred_primary_email!r} | "
                f"identity_type={result.inferred_identity_type!r} | "
                f"confidence={result.owner_confidence_score} ({result.owner_confidence_label}) | "
                f"messages={result.message_count} | "
                f"folders={result.folder_count}"
            )

        if preview:
            self.stdout.write(
                self.style.WARNING("Mode preview activé : aucune écriture.")
            )
            return

        if persist:
            self.stdout.write("Persistance des collaborateurs inférés...")
            collaborator_stats = persistence_service.save_many(results)

            self.stdout.write(self.style.SUCCESS("Persistance collaborateurs terminée."))
            self.stdout.write(f"processed={collaborator_stats['processed']}")
            self.stdout.write(f"collaborators_created={collaborator_stats['collaborators_created']}")
            self.stdout.write(f"emails_linked={collaborator_stats['emails_linked']}")
            self.stdout.write(f"mailboxes_updated={collaborator_stats['mailboxes_updated']}")
            self.stdout.write("")

            self.stdout.write("Recherche des alias email...")
            alias_df, alias_summary, alias_tables = alias_service.explore_mailbox_aliases(
                collaborators_df
            )

            self.stdout.write(
                self.style.SUCCESS(f"{len(alias_df)} alias candidat(s) détecté(s).")
            )

            self.stdout.write("Persistance des alias forts...")
            alias_stats = alias_persistence_service.persist_aliases_from_dataframe(
                alias_df,
                accepted_alias_labels={"strong_alias"},
            )

            self.stdout.write(self.style.SUCCESS("Persistance des alias terminée."))
            self.stdout.write(f"processed_rows={alias_stats['processed_rows']}")
            self.stdout.write(f"unique_candidate_emails={alias_stats['unique_candidate_emails']}")
            self.stdout.write(f"collaborators_found={alias_stats['collaborators_found']}")
            self.stdout.write(f"email_addresses_found={alias_stats['email_addresses_found']}")
            self.stdout.write(f"email_addresses_linked={alias_stats['email_addresses_linked']}")
            self.stdout.write(
                f"email_addresses_already_linked={alias_stats['email_addresses_already_linked']}"
            )