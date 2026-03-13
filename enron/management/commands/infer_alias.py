from __future__ import annotations

from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from enron.services.inference.collaborator_inference_service import (
    CollaboratorInferenceService,
)
from enron.services.inference.mailbox_alias_inference_service import (
    MailboxAliasInferenceService,
)
from enron.services.persistence.alias_persistence_service import (
    CollaboratorAliasPersistenceService,
)

class Command(BaseCommand):
    help = "Calcule et persiste les alias email des collaborateurs déjà présents en base."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--root", type=str, required=True)
        parser.add_argument("--max-mailboxes", type=int, default=None)
        parser.add_argument("--max-files-per-mailbox", type=int, default=200)
        parser.add_argument("--preview", action="store_true")
        parser.add_argument("--limit-output", type=int, default=20)

    def handle(self, *args, **options) -> None:
        root = Path(options["root"]).expanduser().resolve()
        max_mailboxes: int | None = options["max_mailboxes"]
        max_messages_per_mailbox: int | None = options["max_files_per_mailbox"]
        preview: bool = options["preview"]
        limit_output: int = options["limit_output"]

        if not root.exists():
            raise CommandError(f"Le dossier root n'existe pas: {root}")

        if not root.is_dir():
            raise CommandError(f"Le chemin root n'est pas un dossier: {root}")

        inference_service = CollaboratorInferenceService()
        alias_service = MailboxAliasInferenceService()
        alias_persistence_service = CollaboratorAliasPersistenceService()

        self.stdout.write("Reconstruction des collaborateurs inférés...")
        collaborators_df, _, _ = inference_service.explore_collaborators(
            root=root,
            max_messages_per_mailbox=max_messages_per_mailbox,
            max_mailboxes=max_mailboxes,
        )

        self.stdout.write("Inférence des alias email...")
        alias_df, alias_summary, _ = alias_service.explore_mailbox_aliases(collaborators_df)

        self.stdout.write(self.style.SUCCESS(f"{len(alias_df)} alias candidat(s) détecté(s)."))
        self.stdout.write("")

        preview_rows = alias_df.head(limit_output).to_dict(orient="records")
        for row in preview_rows:
            self.stdout.write(
                f"- mailbox_owner={row.get('mailbox_owner')!r} | "
                f"pivot_email={row.get('pivot_email')!r} | "
                f"candidate_email={row.get('candidate_email')!r} | "
                f"score={row.get('final_alias_score')} | "
                f"label={row.get('alias_label')!r} | "
                f"reasons={row.get('alias_reasons')!r}"
            )

        if len(alias_df) > limit_output:
            self.stdout.write(f"... {len(alias_df) - limit_output} résultat(s) supplémentaire(s)")

        self.stdout.write("")
        self.stdout.write("Résumé alias:")
        self.stdout.write(f"mailbox_count={alias_summary.get('mailbox_count', 0)}")
        self.stdout.write(f"candidate_row_count={alias_summary.get('candidate_row_count', 0)}")
        self.stdout.write(f"strong_alias_count={alias_summary.get('strong_alias_count', 0)}")
        self.stdout.write(f"possible_alias_count={alias_summary.get('possible_alias_count', 0)}")
        self.stdout.write(f"unlikely_alias_count={alias_summary.get('unlikely_alias_count', 0)}")
        self.stdout.write(f"not_alias_count={alias_summary.get('not_alias_count', 0)}")
        self.stdout.write("")

        if preview:
            self.stdout.write(self.style.WARNING("Mode preview activé : aucune persistance des alias."))
            return

        self.stdout.write("Persistance des alias forts...")
        stats = alias_persistence_service.persist_aliases_from_dataframe(
            alias_df,
            accepted_alias_labels={"strong_alias"},
        )

        self.stdout.write(self.style.SUCCESS("Persistance des alias terminée."))
        self.stdout.write(f"processed_rows={stats['processed_rows']}")
        self.stdout.write(f"unique_candidate_emails={stats['unique_candidate_emails']}")
        self.stdout.write(f"collaborators_found={stats['collaborators_found']}")
        self.stdout.write(f"email_addresses_found={stats['email_addresses_found']}")
        self.stdout.write(f"email_addresses_linked={stats['email_addresses_linked']}")
        self.stdout.write(f"email_addresses_already_linked={stats['email_addresses_already_linked']}")
        self.stdout.write(f"skipped_missing_mailbox_key={stats['skipped_missing_mailbox_key']}")
        self.stdout.write(f"skipped_missing_collaborator={stats['skipped_missing_collaborator']}")
        self.stdout.write(f"skipped_missing_email={stats['skipped_missing_email']}")
        self.stdout.write(f"skipped_unknown_email_address={stats['skipped_unknown_email_address']}")