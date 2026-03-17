from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from enron.domain.email_payload import (
    FolderPayload,
    MessageOccurrencePayload,
    ParsedEmailPayload,
)
from enron.enron_discovery.enron_discovery_service import (
    DiscoveredEmailFile,
    DiscoveredMailbox,
    EnronDiscoveryService,
)
from enron.normalization.email_folder_normalization_service import (
    EmailFolderNormalizationService,
)

from enron.parser.email_parser_service import EmailParserService
from enron.persistence.email_persistence_service import (
    EmailPersistenceService,
)
from enron.validation.email_validation_service import (
    EmailValidationService,
)


@dataclass(slots=True)
class EnronImportReport:
    total_discovered: int = 0
    total_processed: int = 0
    total_persisted: int = 0
    total_created: int = 0
    total_updated: int = 0
    total_failed_validation: int = 0
    total_parse_errors: int = 0
    total_warnings: int = 0

    processing_errors: list[str] = field(default_factory=list)
    validation_errors: list[str] = field(default_factory=list)
    validation_warnings: list[str] = field(default_factory=list)


class EnronImportService:
    def __init__(
        self,
        parser_service: EmailParserService | None = None,
        validation_service: EmailValidationService | None = None,
        persistence_service: EmailPersistenceService | None = None,
        discovery_service: EnronDiscoveryService | None = None,
        folder_normalization_service: EmailFolderNormalizationService | None = None,
    ) -> None:
        self.parser_service = parser_service or EmailParserService()
        self.validation_service = validation_service or EmailValidationService()
        self.persistence_service = persistence_service or EmailPersistenceService()
        self.discovery_service = discovery_service or EnronDiscoveryService()
        self.folder_normalization_service = (
            folder_normalization_service or EmailFolderNormalizationService()
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
    ) -> EnronImportReport:
        report = EnronImportReport()

        debug_handle = None
        try:
            if debug_output_path is not None:
                debug_handle = debug_output_path.open("w", encoding="utf-8")

            mailboxes = self.discovery_service.discover_mailboxes(
                root=root,
                max_mailboxes=max_mailboxes,
            )

            for mailbox in mailboxes:
                email_files = self.discovery_service.discover_email_files(
                    mailbox_root=mailbox.mailbox_path,
                    max_files=max_files_per_mailbox,
                )

                report.total_discovered += len(email_files)

                for email_file in email_files:
                    try:
                        self._process_email_file(
                            email_file=email_file,
                            mailbox=mailbox,
                            report=report,
                            persist_invalid=persist_invalid,
                            debug_handle=debug_handle,
                        )
                    except Exception as exc:
                        report.processing_errors.append(
                            f"{email_file.source_file}: {type(exc).__name__}: {exc}"
                        )
                        if stop_on_error:
                            raise

        finally:
            if debug_handle is not None:
                debug_handle.close()

        return report

    def _process_email_file(
        self,
        *,
        email_file: DiscoveredEmailFile,
        mailbox: DiscoveredMailbox,
        report: EnronImportReport,
        persist_invalid: bool,
        debug_handle,
    ) -> None:
        report.total_processed += 1

        raw_email = self._read_email_file(email_file)

        message_payload = self.parser_service.parse_email(raw_email)

        if not message_payload.parse_ok:
            report.total_parse_errors += 1
            report.processing_errors.append(
                f"{email_file.source_file}: parse_error={message_payload.parse_error}"
            )

        occurrence_payload = self._build_occurrence_payload(
            email_file=email_file,
            mailbox=mailbox,
        )

        parsed_email = ParsedEmailPayload(
            message=message_payload,
            occurrence=occurrence_payload,
        )

        validation_result = self.validation_service.validate(parsed_email)

        if validation_result.warnings:
            report.total_warnings += len(validation_result.warnings)
            report.validation_warnings.extend(
                f"{email_file.source_file}: {warning}"
                for warning in validation_result.warnings
            )

        if not validation_result.is_valid:
            report.total_failed_validation += 1
            report.validation_errors.extend(
                f"{email_file.source_file}: {error}"
                for error in validation_result.errors
            )

            if not persist_invalid:
                self._write_debug_payload(
                    debug_handle=debug_handle,
                    email_file=email_file,
                    parsed_email=parsed_email,
                    validation_result=validation_result,
                )
                return

        persistence_result = self.persistence_service.save(
            parsed_email=parsed_email,
            validation_result=validation_result,
        )

        report.total_persisted += 1
        report.total_created += persistence_result.created
        report.total_updated += persistence_result.updated

        self._write_debug_payload(
            debug_handle=debug_handle,
            email_file=email_file,
            parsed_email=parsed_email,
            validation_result=validation_result,
            persistence_result=persistence_result,
        )

    def _build_occurrence_payload(
        self,
        *,
        email_file: DiscoveredEmailFile,
        mailbox: DiscoveredMailbox,
    ) -> MessageOccurrencePayload:
        relative_file_path = email_file.source_path.relative_to(mailbox.mailbox_path)
        relative_folder_path = relative_file_path.parent.as_posix()
        folder_name = relative_file_path.parent.name if relative_folder_path else None

        normalized_folder_type = self.folder_normalization_service.normalize_folder_type(folder_name)
        normalized_folder_topic = self.folder_normalization_service.normalize_folder_topic(
            folder_name,
            normalized_folder_type,
        )

        folder_payload = FolderPayload(
            mailbox_key=mailbox.mailbox_key,
            folder_path=relative_folder_path or None,
            folder_name=folder_name,
            folder_type=normalized_folder_type,
            folder_topic=normalized_folder_topic,
        )

        return MessageOccurrencePayload(
            source_file=email_file.source_file,
            folder=folder_payload,
        )

    def _read_email_file(self, email_file: DiscoveredEmailFile) -> bytes:
        return email_file.source_path.read_bytes()

    def _write_debug_payload(
        self,
        *,
        debug_handle,
        email_file: DiscoveredEmailFile,
        parsed_email: ParsedEmailPayload,
        validation_result,
        persistence_result=None,
    ) -> None:
        if debug_handle is None:
            return

        payload = {
            "source_file": email_file.source_file,
            "parsed_email": asdict(parsed_email),
            "validation_result": asdict(validation_result),
            "persistence_result": asdict(persistence_result)
            if persistence_result is not None
            else None,
        }

        debug_handle.write(json.dumps(payload, ensure_ascii=False) + "\n")