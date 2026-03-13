from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from enron.services.normalization.email_normalization_service import (
    EmailNormalizationService,
)
from enron.services.normalization.email_folder_normalization_service import (
    EmailFolderNormalizationService,
)
from enron.services.normalization.identity_normalization_service import (
    IdentityNormalizationService,
)
from enron.services.normalization.filesystem_exploration_service import (
    EmailFilesystemExplorationService,
)
from enron.domain.collaborator_payloads import CollaboratorInferenceResult

@dataclass(slots=True)
class CollaboratorRecord:
    mailbox_owner: str
    inferred_display_name: str | None
    inferred_first_name: str | None
    inferred_last_name: str | None
    inferred_primary_email: str | None
    inferred_position_title: str | None
    inferred_identity_type: str | None
    is_corporate_mailbox_candidate: bool
    message_count: int
    folder_count: int
    all_emails: str
    all_display_names: str
    all_title_candidates: str
    x_filename_examples: str
    top_from_email_count: int
    top_name_count: int
    top_xfrom_count: int
    owner_confidence_score: float
    owner_confidence_label: str
    dominant_sender_email: str | None
    dominant_sender_name: str | None
    dominant_sender_identity_type: str | None
    owner_vs_sender_mismatch: bool


class CollaboratorInferenceService:
    def __init__(
        self,
        email_normalization_service: EmailNormalizationService | None = None,
        folder_normalization_service: EmailFolderNormalizationService | None = None,
        identity_normalization_service: IdentityNormalizationService | None = None,
        filesystem_exploration_service: EmailFilesystemExplorationService | None = None,
    ) -> None:
        self.email_service = email_normalization_service or EmailNormalizationService()
        self.folder_service = folder_normalization_service or EmailFolderNormalizationService()
        self.identity_service = identity_normalization_service or IdentityNormalizationService()
        self.filesystem_service = (
            filesystem_exploration_service or EmailFilesystemExplorationService()
        )

    def build_collaborators_dataframe(
        self,
        root: Path,
        max_messages_per_mailbox: int | None = 200,
        max_mailboxes: int | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        total_messages = 0
        parsed_messages = 0
        failed_messages = 0

        mailbox_message_counts: Counter[str] = Counter()
        mailbox_folder_sets: dict[str, set[str]] = defaultdict(set)
        mailbox_xfilename_counter: dict[str, Counter[str]] = defaultdict(Counter)

        mailbox_from_email_counter: dict[str, Counter[str]] = defaultdict(Counter)
        mailbox_from_name_counter: dict[str, Counter[str]] = defaultdict(Counter)
        mailbox_xfrom_counter: dict[str, Counter[str]] = defaultdict(Counter)
        mailbox_title_counter: dict[str, Counter[str]] = defaultdict(Counter)

        mailbox_email_name_counter: dict[str, dict[str, Counter[str]]] = defaultdict(
            lambda: defaultdict(Counter)
        )
        mailbox_all_emails: dict[str, Counter[str]] = defaultdict(Counter)

        files_by_mailbox = self.folder_service.iter_email_files_by_mailbox(root)
        mailbox_owners = sorted(files_by_mailbox.keys())

        if max_mailboxes is not None:
            mailbox_owners = mailbox_owners[:max_mailboxes]

        for mailbox_owner in mailbox_owners:
            mailbox_files = sorted(files_by_mailbox[mailbox_owner])

            if max_messages_per_mailbox is not None:
                mailbox_files = mailbox_files[:max_messages_per_mailbox]

            for file_path in mailbox_files:
                total_messages += 1

                msg = self.filesystem_service.safe_parse_email(file_path)
                if msg is None:
                    failed_messages += 1
                    continue

                parsed_messages += 1
                mailbox_message_counts[mailbox_owner] += 1

                folder_name = self.folder_service.get_folder_name_from_path(file_path, root)
                if folder_name:
                    mailbox_folder_sets[mailbox_owner].add(folder_name)

                x_filename = self.email_service.normalize_text(msg.get("X-FileName"))
                if x_filename:
                    mailbox_xfilename_counter[mailbox_owner][x_filename] += 1

                from_pairs = self.email_service.parse_address_header(msg.get("From"))
                if from_pairs:
                    from_name, from_email = from_pairs[0]

                    if from_email:
                        mailbox_from_email_counter[mailbox_owner][from_email] += 1
                        mailbox_all_emails[mailbox_owner][from_email] += 1

                    if from_name:
                        mailbox_from_name_counter[mailbox_owner][from_name] += 1

                    if from_email and from_name:
                        mailbox_email_name_counter[mailbox_owner][from_email][from_name] += 1

                x_from_name = self.email_service.extract_xfrom_name(msg.get("X-From"))
                if x_from_name:
                    mailbox_xfrom_counter[mailbox_owner][x_from_name] += 1

                if from_pairs and x_from_name and from_pairs[0][1]:
                    mailbox_email_name_counter[mailbox_owner][from_pairs[0][1]][x_from_name] += 1

                titles = self.identity_service.extract_title_candidates(
                    self.email_service.normalize_text(msg.get("X-From")),
                    self.email_service.normalize_text(msg.get("From")),
                )
                for title in titles:
                    mailbox_title_counter[mailbox_owner][title] += 1

                for header in ("To", "Cc", "Bcc", "X-To", "X-cc", "X-bcc"):
                    for name, email in self.email_service.parse_address_header(msg.get(header)):
                        if email:
                            mailbox_all_emails[mailbox_owner][email] += 1
                        if email and name:
                            mailbox_email_name_counter[mailbox_owner][email][name] += 1

        collaborator_rows: list[dict[str, Any]] = []

        for mailbox_owner, message_count in mailbox_message_counts.items():
            row = self._build_collaborator_row(
                mailbox_owner=mailbox_owner,
                message_count=message_count,
                mailbox_folder_sets=mailbox_folder_sets,
                mailbox_xfilename_counter=mailbox_xfilename_counter,
                mailbox_from_email_counter=mailbox_from_email_counter,
                mailbox_from_name_counter=mailbox_from_name_counter,
                mailbox_xfrom_counter=mailbox_xfrom_counter,
                mailbox_title_counter=mailbox_title_counter,
                mailbox_email_name_counter=mailbox_email_name_counter,
                mailbox_all_emails=mailbox_all_emails,
            )
            collaborator_rows.append(row)

        collaborators_df = pd.DataFrame(collaborator_rows)

        if not collaborators_df.empty:
            collaborators_df = collaborators_df.sort_values(
                by=["is_corporate_mailbox_candidate", "owner_confidence_score", "message_count"],
                ascending=[True, False, False],
            ).reset_index(drop=True)

        meta = {
            "total_messages": total_messages,
            "parsed_messages": parsed_messages,
            "failed_messages": failed_messages,
            "collaborator_count": int(len(collaborators_df)),
            "high_confidence_count": int(
                (collaborators_df["owner_confidence_label"] == "high").sum()
            ) if not collaborators_df.empty else 0,
            "medium_confidence_count": int(
                (collaborators_df["owner_confidence_label"] == "medium").sum()
            ) if not collaborators_df.empty else 0,
            "low_confidence_count": int(
                (collaborators_df["owner_confidence_label"] == "low").sum()
            ) if not collaborators_df.empty else 0,
            "corporate_count": int(
                (collaborators_df["owner_confidence_label"] == "corporate").sum()
            ) if not collaborators_df.empty else 0,
            "system_count": int(
                (collaborators_df["owner_confidence_label"] == "system").sum()
            ) if not collaborators_df.empty else 0,
            "external_bulk_count": int(
                (collaborators_df["owner_confidence_label"] == "external_bulk").sum()
            ) if not collaborators_df.empty else 0,
        }

        return collaborators_df, meta

    def explore_collaborators(
        self,
        root: Path,
        max_messages_per_mailbox: int | None = 200,
        max_mailboxes: int | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
        df, meta = self.build_collaborators_dataframe(
            root=root,
            max_messages_per_mailbox=max_messages_per_mailbox,
            max_mailboxes=max_mailboxes,
        )
        summary = self.compute_collaborators_summary(df, meta)
        tables = self.compute_collaborator_tables(df)
        return df, summary, tables

    def run_and_save_collaborator_exploration(
        self,
        *,
        root: Path,
        output_dir: Path,
        max_messages_per_mailbox: int | None = 5000,
        max_mailboxes: int | None = None,
    ) -> dict[str, Any]:
        output_dir.mkdir(parents=True, exist_ok=True)

        df, summary, tables = self.explore_collaborators(
            root=root,
            max_messages_per_mailbox=max_messages_per_mailbox,
            max_mailboxes=max_mailboxes,
        )

        files = self.save_collaborator_reports(
            df=df,
            summary=summary,
            tables=tables,
            output_dir=output_dir,
        )

        manifest = {
            "status": "success",
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "source_root": str(root),
            "max_messages_per_mailbox": max_messages_per_mailbox,
            "max_mailboxes": max_mailboxes,
            "row_count": int(len(df)),
            "files": files,
        }

        manifest_path = output_dir / "collaborators_manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        return {
            **files,
            "manifest_json": str(manifest_path),
            "summary": summary,
            "tables": tables,
            "row_count": int(len(df)),
        }

    def save_collaborator_reports(
        self,
        df: pd.DataFrame,
        summary: dict[str, Any],
        tables: dict[str, Any],
        output_dir: Path,
    ) -> dict[str, str]:
        output_dir.mkdir(parents=True, exist_ok=True)

        csv_path = output_dir / "collaborators_profile.csv"
        json_summary_path = output_dir / "collaborators_summary.json"
        json_tables_path = output_dir / "collaborators_tables.json"

        df.to_csv(csv_path, index=False)

        json_summary_path.write_text(
            json.dumps(summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        json_tables_path.write_text(
            json.dumps(tables, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        return {
            "csv": str(csv_path),
            "summary_json": str(json_summary_path),
            "tables_json": str(json_tables_path),
        }

    def load_saved_collaborator_reports(self, output_dir: Path) -> dict[str, Any]:
        csv_path = output_dir / "collaborators_profile.csv"
        summary_path = output_dir / "collaborators_summary.json"
        tables_path = output_dir / "collaborators_tables.json"
        manifest_path = output_dir / "collaborators_manifest.json"

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV introuvable: {csv_path}")
        if not summary_path.exists():
            raise FileNotFoundError(f"Summary introuvable: {summary_path}")
        if not tables_path.exists():
            raise FileNotFoundError(f"Tables introuvable: {tables_path}")

        df = pd.read_csv(csv_path)
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        tables = json.loads(tables_path.read_text(encoding="utf-8"))
        manifest = (
            json.loads(manifest_path.read_text(encoding="utf-8"))
            if manifest_path.exists()
            else {}
        )

        return {
            "df": df,
            "summary": summary,
            "tables": tables,
            "manifest": manifest,
            "files": {
                "csv": str(csv_path),
                "summary_json": str(summary_path),
                "tables_json": str(tables_path),
                "manifest_json": str(manifest_path),
            },
        }

    def compute_collaborators_summary(
        self,
        df: pd.DataFrame,
        meta: dict[str, Any],
    ) -> dict[str, Any]:
        if df.empty:
            return {
                **meta,
                "row_count": 0,
                "unique_mailbox_owners": 0,
                "unique_primary_emails": 0,
                "unique_display_names": 0,
                "corporate_mailbox_candidates": 0,
                "owner_sender_mismatch_count": 0,
                "avg_owner_confidence_score": 0.0,
                "top_identity_types": {},
                "top_confidence_labels": {},
                "top_position_titles": {},
                "top_mailbox_owners": {},
                "top_primary_emails": {},
            }

        score_series = df["owner_confidence_score"].dropna()

        return {
            **meta,
            "row_count": int(len(df)),
            "unique_mailbox_owners": int(df["mailbox_owner"].dropna().nunique()),
            "unique_primary_emails": int(df["inferred_primary_email"].dropna().nunique()),
            "unique_display_names": int(df["inferred_display_name"].dropna().nunique()),
            "corporate_mailbox_candidates": int(df["is_corporate_mailbox_candidate"].sum()),
            "owner_sender_mismatch_count": int(
                df["owner_vs_sender_mismatch"].fillna(False).astype(bool).sum()
            ),
            "avg_owner_confidence_score": round(float(score_series.mean()), 2)
            if not score_series.empty
            else 0.0,
            "top_identity_types": (
                df["inferred_identity_type"]
                .fillna("<missing>")
                .value_counts()
                .head(20)
                .to_dict()
            ),
            "top_confidence_labels": (
                df["owner_confidence_label"]
                .fillna("<missing>")
                .value_counts()
                .head(20)
                .to_dict()
            ),
            "top_position_titles": (
                df["inferred_position_title"]
                .fillna("<missing>")
                .value_counts()
                .head(20)
                .to_dict()
            ),
            "top_mailbox_owners": (
                df["mailbox_owner"]
                .fillna("<missing>")
                .value_counts()
                .head(20)
                .to_dict()
            ),
            "top_primary_emails": (
                df["inferred_primary_email"]
                .fillna("<missing>")
                .value_counts()
                .head(20)
                .to_dict()
            ),
        }

    def compute_collaborator_tables(self, df: pd.DataFrame) -> dict[str, Any]:
        if df.empty:
            return {
                "collaborator_preview": [],
                "top_confident_collaborators": [],
                "corporate_candidates": [],
                "owner_sender_mismatches": [],
                "identity_confidence_pairs": [],
            }

        collaborator_preview = (
            df.head(200)
            .fillna("")
            .to_dict(orient="records")
        )

        top_confident_collaborators = (
            df[
                [
                    "mailbox_owner",
                    "inferred_display_name",
                    "inferred_first_name",
                    "inferred_last_name",
                    "inferred_primary_email",
                    "inferred_position_title",
                    "inferred_identity_type",
                    "owner_confidence_score",
                    "owner_confidence_label",
                    "message_count",
                    "folder_count",
                ]
            ]
            .sort_values(
                by=["owner_confidence_score", "message_count"],
                ascending=[False, False],
            )
            .head(200)
            .fillna("")
            .to_dict(orient="records")
        )

        corporate_candidates = (
            df[df["is_corporate_mailbox_candidate"] == True][
                [
                    "mailbox_owner",
                    "inferred_display_name",
                    "inferred_primary_email",
                    "inferred_identity_type",
                    "owner_confidence_score",
                    "owner_confidence_label",
                    "message_count",
                    "folder_count",
                ]
            ]
            .sort_values(
                by=["owner_confidence_score", "message_count"],
                ascending=[False, False],
            )
            .head(200)
            .fillna("")
            .to_dict(orient="records")
        )

        owner_sender_mismatches = (
            df[df["owner_vs_sender_mismatch"] == True][
                [
                    "mailbox_owner",
                    "inferred_display_name",
                    "inferred_primary_email",
                    "inferred_identity_type",
                    "dominant_sender_name",
                    "dominant_sender_email",
                    "dominant_sender_identity_type",
                    "owner_confidence_score",
                    "owner_confidence_label",
                    "message_count",
                ]
            ]
            .sort_values(
                by=["owner_confidence_score", "message_count"],
                ascending=[False, False],
            )
            .head(200)
            .fillna("")
            .to_dict(orient="records")
        )

        identity_confidence_pairs = (
            df[
                [
                    "inferred_identity_type",
                    "owner_confidence_label",
                ]
            ]
            .fillna("")
            .value_counts()
            .reset_index(name="count")
            .head(100)
            .to_dict(orient="records")
        )

        return {
            "collaborator_preview": collaborator_preview,
            "top_confident_collaborators": top_confident_collaborators,
            "corporate_candidates": corporate_candidates,
            "owner_sender_mismatches": owner_sender_mismatches,
            "identity_confidence_pairs": identity_confidence_pairs,
        }

    def _build_collaborator_row(
        self,
        *,
        mailbox_owner: str,
        message_count: int,
        mailbox_folder_sets: dict[str, set[str]],
        mailbox_xfilename_counter: dict[str, Counter[str]],
        mailbox_from_email_counter: dict[str, Counter[str]],
        mailbox_from_name_counter: dict[str, Counter[str]],
        mailbox_xfrom_counter: dict[str, Counter[str]],
        mailbox_title_counter: dict[str, Counter[str]],
        mailbox_email_name_counter: dict[str, dict[str, Counter[str]]],
        mailbox_all_emails: dict[str, Counter[str]],
    ) -> dict[str, Any]:
        from_email_counter = mailbox_from_email_counter[mailbox_owner]
        from_name_counter = mailbox_from_name_counter[mailbox_owner]
        xfrom_counter = mailbox_xfrom_counter[mailbox_owner]
        title_counter = mailbox_title_counter[mailbox_owner]
        all_email_counter = mailbox_all_emails[mailbox_owner]

        primary_candidates = set(from_email_counter.keys())
        secondary_candidates = set(all_email_counter.keys()) - primary_candidates
        ordered_candidates = list(primary_candidates) + list(secondary_candidates)

        best_email: str | None = None
        best_name: str | None = None
        best_identity_type: str | None = None
        best_score = float("-inf")
        best_rank: tuple[int, float] | None = None

        xfilename_values = [
            name for name, _ in mailbox_xfilename_counter[mailbox_owner].most_common(20)
        ]
        xfilename_tokens = self._extract_owner_hints_from_xfilename(xfilename_values)

        for email in ordered_candidates:
            observed_names = mailbox_email_name_counter[mailbox_owner].get(email, Counter())
            candidate_name = observed_names.most_common(1)[0][0] if observed_names else None

            if not candidate_name:
                candidate_name = self.email_service.derive_name_from_email(
                    email,
                    self.identity_service.GENERIC_LOCALPARTS,
                )

            score, identity_type = self._score_owner_candidate(
                candidate_email=email,
                candidate_name=candidate_name,
                mailbox_owner=mailbox_owner,
                from_email_counter=from_email_counter,
                from_name_counter=from_name_counter,
                xfrom_counter=xfrom_counter,
                xfilename_tokens=xfilename_tokens,
            )

            rank = (self.identity_service.identity_priority(identity_type), -score)

            if best_rank is None or rank < best_rank:
                best_rank = rank
                best_score = score
                best_email = email
                best_name = candidate_name
                best_identity_type = identity_type

        if not best_name:
            if xfrom_counter:
                best_name = xfrom_counter.most_common(1)[0][0]
            elif from_name_counter:
                best_name = from_name_counter.most_common(1)[0][0]
            elif best_email:
                best_name = self.email_service.derive_name_from_email(
                    best_email,
                    self.identity_service.GENERIC_LOCALPARTS,
                )
            else:
                best_name = mailbox_owner.replace("-", " ").replace("_", " ").title()

        if not best_identity_type:
            best_identity_type = self.identity_service.classify_identity(
                best_name,
                best_email,
                self.email_service,
            )

        inferred_first_name, inferred_last_name = self.identity_service.split_name_parts(
            best_name
        )
        inferred_title = title_counter.most_common(1)[0][0] if title_counter else None

        all_emails = sorted(all_email_counter.keys())
        all_display_names = sorted(set(from_name_counter.keys()) | set(xfrom_counter.keys()))
        all_titles = [title for title, _ in title_counter.most_common(20)]
        x_filename_examples = [
            name for name, _ in mailbox_xfilename_counter[mailbox_owner].most_common(10)
        ]

        final_score = best_score if best_score != float("-inf") else 0.0
        final_confidence = self._confidence_label(final_score, best_identity_type)

        dominant_sender_email = (
            from_email_counter.most_common(1)[0][0]
            if from_email_counter
            else None
        )

        dominant_sender_name: str | None = None
        if dominant_sender_email:
            observed_names = mailbox_email_name_counter[mailbox_owner].get(
                dominant_sender_email,
                Counter(),
            )
            dominant_sender_name = (
                observed_names.most_common(1)[0][0] if observed_names else None
            )

        dominant_sender_identity_type = self.identity_service.classify_identity(
            dominant_sender_name,
            dominant_sender_email,
            self.email_service,
        )

        owner_vs_sender_mismatch = (
            best_email != dominant_sender_email
            or best_identity_type != dominant_sender_identity_type
        )

        record = CollaboratorRecord(
            mailbox_owner=mailbox_owner,
            inferred_display_name=best_name,
            inferred_first_name=inferred_first_name,
            inferred_last_name=inferred_last_name,
            inferred_primary_email=best_email,
            inferred_position_title=inferred_title,
            inferred_identity_type=best_identity_type,
            is_corporate_mailbox_candidate=best_identity_type in {
                "corporate",
                "system",
                "external_bulk",
            },
            message_count=int(message_count),
            folder_count=len(mailbox_folder_sets[mailbox_owner]),
            all_emails=self.identity_service.join_pipe(all_emails),
            all_display_names=self.identity_service.join_pipe(all_display_names),
            all_title_candidates=self.identity_service.join_pipe(all_titles),
            x_filename_examples=self.identity_service.join_pipe(x_filename_examples),
            top_from_email_count=from_email_counter.get(best_email, 0) if best_email else 0,
            top_name_count=from_name_counter.get(best_name, 0) if best_name else 0,
            top_xfrom_count=xfrom_counter.get(best_name, 0) if best_name else 0,
            owner_confidence_score=round(final_score, 2),
            owner_confidence_label=final_confidence,
            dominant_sender_email=dominant_sender_email,
            dominant_sender_name=dominant_sender_name,
            dominant_sender_identity_type=dominant_sender_identity_type,
            owner_vs_sender_mismatch=owner_vs_sender_mismatch,
        )
        return asdict(record)

    def _score_owner_candidate(
        self,
        *,
        candidate_email: str | None,
        candidate_name: str | None,
        mailbox_owner: str,
        from_email_counter: Counter[str],
        from_name_counter: Counter[str],
        xfrom_counter: Counter[str],
        xfilename_tokens: set[str],
    ) -> tuple[float, str]:
        identity_type = self.identity_service.classify_identity(
            candidate_name,
            candidate_email,
            self.email_service,
        )
        score = 0.0

        from_hits = from_email_counter.get(candidate_email, 0) if candidate_email else 0
        name_hits = from_name_counter.get(candidate_name, 0) if candidate_name else 0
        xfrom_hits = xfrom_counter.get(candidate_name, 0) if candidate_name else 0

        score += math.log1p(from_hits) * 12.0
        score += math.log1p(name_hits) * 5.0
        score += math.log1p(xfrom_hits) * 8.0

        if self.email_service.is_internal_enron_email(candidate_email):
            score += 8.0

        score += self._compute_owner_anchor_bonus(
            mailbox_owner=mailbox_owner,
            xfilename_tokens=xfilename_tokens,
            candidate_name=candidate_name,
            candidate_email=candidate_email,
        )

        score += self._compute_mailbox_alignment_score(
            mailbox_owner=mailbox_owner,
            candidate_name=candidate_name,
            candidate_email=candidate_email,
        )

        score += self._compute_xfilename_alignment_score(
            xfilename_tokens=xfilename_tokens,
            candidate_name=candidate_name,
            candidate_email=candidate_email,
        )

        localpart = self.email_service.extract_email_localpart(candidate_email)
        if localpart and "." in localpart:
            score += 4.0

        if identity_type == "person":
            score += 20.0
        elif identity_type == "likely_person":
            score += 8.0
        elif identity_type == "unknown":
            score -= 2.0
        elif identity_type == "corporate":
            score -= 35.0
        elif identity_type == "system":
            score -= 45.0
        elif identity_type == "external_bulk":
            score -= 45.0

        return score, identity_type

    def _confidence_label(self, score: float, identity_type: str | None) -> str:
        if identity_type in {"corporate", "system", "external_bulk"}:
            return identity_type
        if score >= 40:
            return "high"
        if score >= 18:
            return "medium"
        return "low"

    def _extract_owner_hints_from_xfilename(self, values: list[str]) -> set[str]:
        tokens: set[str] = set()

        for value in values:
            value = self.email_service.normalize_text(value)
            if not value:
                continue

            cleaned = value.lower()
            cleaned = re.sub(r"\(non-privileged\)", " ", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\.pst$|\.nsf$", " ", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"[^a-z]+", " ", cleaned)

            for token in cleaned.split():
                if len(token) >= 2:
                    tokens.add(token)

        return tokens

    def _compute_mailbox_alignment_score(
        self,
        *,
        mailbox_owner: str,
        candidate_name: str | None,
        candidate_email: str | None,
    ) -> float:
        owner_tokens = self.identity_service.tokenize_alpha(mailbox_owner)
        name_tokens = self.identity_service.tokenize_alpha(candidate_name)
        email_tokens = self.identity_service.tokenize_alpha(
            self.email_service.extract_email_localpart(candidate_email)
        )

        score = 0.0

        if owner_tokens and owner_tokens.intersection(name_tokens):
            score += 20.0

        if owner_tokens and owner_tokens.intersection(email_tokens):
            score += 20.0

        return score

    def _compute_xfilename_alignment_score(
        self,
        *,
        xfilename_tokens: set[str],
        candidate_name: str | None,
        candidate_email: str | None,
    ) -> float:
        if not xfilename_tokens:
            return 0.0

        name_tokens = self.identity_service.tokenize_alpha(candidate_name)
        email_tokens = self.identity_service.tokenize_alpha(
            self.email_service.extract_email_localpart(candidate_email)
        )

        score = 0.0

        if xfilename_tokens.intersection(name_tokens):
            score += 18.0

        if xfilename_tokens.intersection(email_tokens):
            score += 18.0

        return score

    def _compute_owner_anchor_bonus(
        self,
        *,
        mailbox_owner: str,
        xfilename_tokens: set[str],
        candidate_name: str | None,
        candidate_email: str | None,
    ) -> float:
        owner_tokens = self.identity_service.tokenize_alpha(mailbox_owner)
        name_tokens = self.identity_service.tokenize_alpha(candidate_name)
        email_tokens = self.identity_service.tokenize_alpha(
            self.email_service.extract_email_localpart(candidate_email)
        )

        owner_match = bool(
            owner_tokens.intersection(name_tokens)
            or owner_tokens.intersection(email_tokens)
        )
        xfile_match = bool(
            xfilename_tokens.intersection(name_tokens)
            or xfilename_tokens.intersection(email_tokens)
        )

        if owner_match and xfile_match:
            return 30.0
        if owner_match or xfile_match:
            return 12.0
        return 0.0
    
    def infer_all(
        self,
        *,
        root: Path,
        max_messages_per_mailbox: int | None = 200,
        max_mailboxes: int | None = None,
    ) -> list[CollaboratorInferenceResult]:
        df, _, _ = self.explore_collaborators(
            root=root,
            max_messages_per_mailbox=max_messages_per_mailbox,
            max_mailboxes=max_mailboxes,
        )
        return self.build_inference_results_from_dataframe(df)
    
    def build_inference_results_from_dataframe(
        self,
        df,
    ) -> list[CollaboratorInferenceResult]:
        results: list[CollaboratorInferenceResult] = []

        for row in df.to_dict(orient="records"):
            mailbox_owner = row.get("mailbox_owner")
            mailbox_key = self.folder_service.normalize_mailbox_key(mailbox_owner)

            if not mailbox_key:
                continue

            results.append(
                CollaboratorInferenceResult(
                    mailbox_key=mailbox_key,
                    inferred_display_name=row.get("inferred_display_name"),
                    inferred_first_name=row.get("inferred_first_name"),
                    inferred_last_name=row.get("inferred_last_name"),
                    inferred_primary_email=row.get("inferred_primary_email"),
                    inferred_position_title=row.get("inferred_position_title"),
                    inferred_identity_type=row.get("inferred_identity_type"),
                    is_corporate_mailbox_candidate=bool(
                        row.get("is_corporate_mailbox_candidate")
                    ),
                    message_count=int(row.get("message_count") or 0),
                    folder_count=int(row.get("folder_count") or 0),
                    all_emails=self._split_pipe(row.get("all_emails")),
                    all_display_names=self._split_pipe(row.get("all_display_names")),
                    top_from_email_count=int(row.get("top_from_email_count") or 0),
                    top_name_count=int(row.get("top_name_count") or 0),
                    owner_confidence_score=float(row.get("owner_confidence_score") or 0.0),
                    owner_confidence_label=row.get("owner_confidence_label") or "low",
                    dominant_sender_email=row.get("dominant_sender_email"),
                    dominant_sender_name=row.get("dominant_sender_name"),
                    dominant_sender_identity_type=row.get("dominant_sender_identity_type"),
                    owner_vs_sender_mismatch=bool(row.get("owner_vs_sender_mismatch")),
                )
            )

        return results

    def _split_pipe(self, value: str | None) -> list[str]:
        if not value:
            return []
        return [item.strip() for item in str(value).split("|") if item.strip()]