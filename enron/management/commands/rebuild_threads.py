from django.core.management.base import BaseCommand

from enron.resolve.message_thread_resolver_service import (
    MessageThreadResolverService,
)


class Command(BaseCommand):
    help = "Reconstruit les threads de messages à partir de Message-ID, In-Reply-To et References."

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=1000,
            help="Taille des batchs pour le bulk_update.",
        )

    def handle(self, *args, **options):
        batch_size = options["batch_size"]

        service = MessageThreadResolverService()
        stats = service.rebuild_all(batch_size=batch_size)

        self.stdout.write(self.style.SUCCESS("Reconstruction des threads terminée."))
        self.stdout.write(f"Total messages              : {stats.total_messages}")
        self.stdout.write(f"Messages mis à jour         : {stats.updated_messages}")
        self.stdout.write(f"Résolus par References      : {stats.resolved_by_references}")
        self.stdout.write(f"Résolus par In-Reply-To     : {stats.resolved_by_in_reply_to}")
        self.stdout.write(f"Auto-racinés                : {stats.self_rooted_messages}")
        self.stdout.write(f"Non résolus                 : {stats.unresolved_messages}")