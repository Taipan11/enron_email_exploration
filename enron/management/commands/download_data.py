from __future__ import annotations

from time import perf_counter

from django.core.management.base import BaseCommand

from enron.download_enron.dowload_enron import download_enron


class Command(BaseCommand):
    help = "Télécharge les données Enron si elles n'existent pas déjà dans le projet."

    def handle(self, *args, **options) -> None:
        start = perf_counter()

        output_file = download_enron()

        elapsed = perf_counter() - start
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(f"Téléchargement terminé : {output_file}"))
        self.stdout.write(self.style.SUCCESS(f"Temps écoulé : {elapsed:.2f}s"))