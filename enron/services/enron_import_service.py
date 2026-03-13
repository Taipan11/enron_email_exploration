from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
import json

from enron.domain.email_payload import (
    FolderPayload,
    MessageOccurrencePayload,
    ParsedEmailPayload,
)
from enron.services.file_discovery.file_discovery_service import FileDiscoveryService
from enron.services.normalization.email_folder_normalization_service import (
    EmailFolderNormalizationService,
)
from enron.services.parser.email_parser_service import EmailParserService
from enron.services.validation.email_validation_service import EmailValidationService
from enron.services.persistence.email_persistence_service import EmailPersistenceService


@dataclass(slots=True)
class ImportExecutionReport:
    total_discovered: int = 0
    total_processed: int = 0
    total_persisted: int = 0
    total_created: int = 0
    total_updated: int = 0
    total_failed_validation: int = 0
    total_parse_errors: int = 0
    total_warnings: int = 0

    validation_errors: list[str] = field(default_factory=list)
    validation_warnings: list[str] = field(default_factory=list)
    processing_errors: list[str] = field(default_factory=list)

    def add_processing_error(self, source_file: str, message: str) -> None:
        self.processing_errors.append(f"[{source_file}] {message}")

    def add_validation_errors(self, source_file: str, messages: list[str]) -> None:
        self.validation_errors.extend(
            f"[{source_file}] {message}" for message in messages
        )

    def add_validation_warnings(self, source_file: str, messages: list[str]) -> None:
        self.validation_warnings.extend(
            f"[{source_file}] {message}" for message in messages
        )
        self.total_warnings += len(messages)


class EnronImportService:
    """
    Orchestrateur principal du pipeline d'import Enron.
    """

    def __init__(
        self,
        file_discovery_service: FileDiscoveryService | None = None,
        email_parser_service: EmailParserService | None = None,
        email_validation_service: EmailValidationService | None = None,
        email_persistence_service: EmailPersistenceService | None = None,
        email_folder_normalization_service: EmailFolderNormalizationService | None = None,
    ) -> None:
        self.file_discovery_service = file_discovery_service or FileDiscoveryService()
        self.email_parser_service = email_parser_service or EmailParserService()
        self.email_validation_service = email_validation_service or EmailValidationService()
        self.email_persistence_service = email_persistence_service or EmailPersistenceService()
        self.email_folder_normalization_service = (
            email_folder_normalization_service or EmailFolderNormalizationService()
        )

    def run(
        self,
        *,
        root: Path,
        max_mailboxes: int | None = None,
        max_files_per_mailbox: int | None = None,
        stop_on_error: bool = False,
        persist_invalid: bool = False,
        debug_output_path: Path | None = None,
    ) -> ImportExecutionReport:
        report = ImportExecutionReport()

        discovered_files = self.file_discovery_service.list_email_files(
            root=root,
            max_mailboxes=max_mailboxes,
            max_files_per_mailbox=max_files_per_mailbox,
        )
        report.total_discovered = len(discovered_files)

        for discovered_file in discovered_files:
            report.total_processed += 1

            try:
                folder_record = self.email_folder_normalization_service.build_record(
                    file_path=Path(discovered_file.source_file),
                    root=root,
                )

                raw_email = self._read_email_file(discovered_file.source_file)

                message_payload = self.email_parser_service.parse_email(
                    raw_email=raw_email,
                )

                if not message_payload.parse_ok:
                    report.total_parse_errors += 1

                occurrence_payload = MessageOccurrencePayload(
                    source_file=discovered_file.source_file,
                    folder=FolderPayload(
                        mailbox_key=folder_record.mailbox_key,
                        folder_path=folder_record.folder_path,
                        folder_name=folder_record.folder_name,
                        folder_type=folder_record.folder_type,
                        folder_topic=folder_record.folder_topic,
                    ),
                )

                parsed_email = ParsedEmailPayload(
                    message=message_payload,
                    occurrence=occurrence_payload,
                )

                validation_result = self.email_validation_service.validate(parsed_email)

                if debug_output_path is not None:
                    self._append_debug_output(
                        output_path=debug_output_path,
                        payload=parsed_email,
                        source_file=discovered_file.source_file,
                        validation_result=validation_result,
                    )

                if validation_result and validation_result.warnings:
                    report.add_validation_warnings(
                        discovered_file.source_file,
                        validation_result.warnings,
                    )

                if validation_result and not validation_result.is_valid:
                    report.total_failed_validation += 1
                    report.add_validation_errors(
                        discovered_file.source_file,
                        validation_result.errors,
                    )

                    if not persist_invalid:
                        continue

                persistence_result = self.email_persistence_service.save(
                    parsed_email=parsed_email,
                    validation_result=validation_result,
                )
                report.total_persisted += 1
                report.total_created += persistence_result.created
                report.total_updated += persistence_result.updated

            except Exception as exc:
                report.add_processing_error(
                    discovered_file.source_file,
                    f"Erreur de traitement : {type(exc).__name__}: {exc}",
                )

                if stop_on_error:
                    raise

        return report

    def _read_email_file(self, source_file: str) -> str:
        return Path(source_file).read_text(encoding="utf-8", errors="replace")

    def _append_debug_output(
        self,
        *,
        output_path: Path,
        payload,
        source_file: str,
        validation_result=None,
    ) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        record = self._build_debug_record(
            payload=payload,
            source_file=source_file,
            validation_result=validation_result,
        )

        with output_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _build_debug_record(
        self,
        *,
        payload,
        source_file: str,
        validation_result=None,
    ) -> dict:
        payload_dict = asdict(payload)
        payload_dict = self._serialize_datetimes(payload_dict)

        return {
            "source_file": source_file,
            "payload": payload_dict,
            "validation": {
                "is_valid": validation_result.is_valid,
                "errors": validation_result.errors,
                "warnings": validation_result.warnings,
            }
            if validation_result is not None
            else None,
        }

    def _serialize_datetimes(self, value):
        if isinstance(value, datetime):
            return value.isoformat()

        if isinstance(value, dict):
            return {
                key: self._serialize_datetimes(sub_value)
                for key, sub_value in value.items()
            }

        if isinstance(value, list):
            return [self._serialize_datetimes(item) for item in value]

        return value