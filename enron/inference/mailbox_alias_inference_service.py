from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from enron.normalization.email_normalization_service import (
    EmailNormalizationService,
)
from enron.normalization.email_folder_normalization_service import (
    EmailFolderNormalizationService,
)
from enron.normalization.identity_normalization_service import (
    IdentityNormalizationService,
)


@dataclass(slots=True)
class MailboxAliasCandidateRecord:
    mailbox_owner: str
    pivot_email: str | None
    pivot_name: str | None
    pivot_identity_type: str | None
    candidate_email: str
    candidate_name: str | None
    candidate_domain: str | None
    candidate_localpart: str | None
    candidate_identity_type: str | None
    is_same_as_pivot: bool
    is_internal_enron_email: bool
    is_external_email: bool
    localpart_similarity_score: float
    name_similarity_score: float
    mailbox_owner_alignment_score: float
    xfilename_alignment_score: float
    domain_alignment_score: float
    sender_alignment_score: float
    final_alias_score: float
    alias_label: str
    alias_reasons: str


class MailboxAliasInferenceService:
    def __init__(
        self,
        email_normalization_service: EmailNormalizationService | None = None,
        folder_normalization_service: EmailFolderNormalizationService | None = None,
        identity_normalization_service: IdentityNormalizationService | None = None,
    ) -> None:
        self.email_service = email_normalization_service or EmailNormalizationService()
        self.folder_service = folder_normalization_service or EmailFolderNormalizationService()
        self.identity_service = identity_normalization_service or IdentityNormalizationService()

    def build_mailbox_alias_candidates_dataframe(
        self,
        collaborators_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        if collaborators_df is None or collaborators_df.empty:
            empty_df = pd.DataFrame([])
            meta = {
                "mailbox_count": 0,
                "candidate_row_count": 0,
                "strong_alias_count": 0,
                "possible_alias_count": 0,
                "unlikely_alias_count": 0,
                "not_alias_count": 0,
            }
            return empty_df, meta

        rows: list[dict[str, Any]] = []

        for collaborator in collaborators_df.fillna("").to_dict(orient="records"):
            mailbox_owner = self.email_service.normalize_text(collaborator.get("mailbox_owner"))
            pivot_email = self._normalize_email_value(collaborator.get("inferred_primary_email"))
            pivot_name = self._normalize_name_value(collaborator.get("inferred_display_name"))
            pivot_identity_type = self.email_service.normalize_text(
                collaborator.get("inferred_identity_type")
            )
            dominant_sender_email = self._normalize_email_value(
                collaborator.get("dominant_sender_email")
            )

            all_emails = [
                self._normalize_email_value(email)
                for email in self._split_pipe(collaborator.get("all_emails"))
            ]
            all_emails = [email for email in all_emails if email]

            all_display_names = [
                self._normalize_name_value(name)
                for name in self._split_pipe(collaborator.get("all_display_names"))
            ]
            all_display_names = [name for name in all_display_names if name]

            xfilename_examples = self._split_pipe(collaborator.get("x_filename_examples"))
            xfilename_hints = self._extract_xfilename_hints(xfilename_examples)

            seen_candidates: set[str] = set()

            for candidate_email in all_emails:
                if not candidate_email or candidate_email in seen_candidates:
                    continue
                seen_candidates.add(candidate_email)

                candidate_localpart = self.email_service.extract_email_localpart(candidate_email)
                candidate_domain = self.email_service.extract_email_domain(candidate_email)

                candidate_name = self._normalize_name_value(
                    self.email_service.derive_name_from_email(
                        candidate_email,
                        self.identity_service.GENERIC_LOCALPARTS,
                    )
                )

                if not candidate_name and pivot_name:
                    candidate_name = pivot_name

                candidate_identity_type = self.identity_service.classify_identity(
                    candidate_name,
                    candidate_email,
                    self.email_service,
                )

                is_same_as_pivot = bool(pivot_email and candidate_email == pivot_email)
                is_internal_enron = self.email_service.is_internal_enron_email(candidate_email)
                is_external_email = bool(candidate_domain and not is_internal_enron)

                if is_same_as_pivot:
                    continue

                if self._is_generic_or_system_candidate(
                    candidate_email,
                    candidate_identity_type,
                ):
                    continue

                localpart_score = self._localpart_similarity(
                    self.email_service.extract_email_localpart(pivot_email),
                    candidate_localpart,
                )
                name_score = self._name_similarity(pivot_name, candidate_name)
                mailbox_owner_score = self._compute_mailbox_owner_alignment_score(
                    mailbox_owner=mailbox_owner,
                    candidate_name=candidate_name,
                    candidate_email=candidate_email,
                )
                xfilename_score = self._compute_xfilename_alignment_score(
                    xfilename_hints=xfilename_hints,
                    candidate_name=candidate_name,
                    candidate_email=candidate_email,
                )
                domain_score = self._compute_domain_alignment_score(
                    pivot_email=pivot_email,
                    candidate_email=candidate_email,
                )
                sender_score = self._compute_sender_alignment_score(
                    dominant_sender_email=dominant_sender_email,
                    candidate_email=candidate_email,
                )

                if (
                    localpart_score < 0.45
                    and name_score < 0.45
                    and mailbox_owner_score == 0.0
                    and xfilename_score == 0.0
                    and sender_score < 0.6
                ):
                    continue

                final_score = 0.0
                final_score += localpart_score * 35.0
                final_score += name_score * 25.0
                final_score += mailbox_owner_score * 15.0
                final_score += xfilename_score * 15.0
                final_score += domain_score * 5.0
                final_score += sender_score * 5.0

                if is_external_email and localpart_score >= 0.75:
                    final_score += 5.0

                if candidate_identity_type == "person":
                    final_score += 5.0
                elif candidate_identity_type == "likely_person":
                    final_score += 2.0
                elif candidate_identity_type == "unknown":
                    final_score -= 3.0
                elif candidate_identity_type in {"corporate", "system", "external_bulk"}:
                    final_score -= 40.0

                final_score = round(max(0.0, min(final_score, 100.0)), 2)

                alias_label = self._alias_label_from_score(
                    score=final_score,
                    candidate_identity_type=candidate_identity_type,
                )

                alias_reasons = self._build_alias_reasons(
                    localpart_score=localpart_score,
                    name_score=name_score,
                    mailbox_owner_score=mailbox_owner_score,
                    xfilename_score=xfilename_score,
                    domain_score=domain_score,
                    sender_score=sender_score,
                    candidate_identity_type=candidate_identity_type,
                )

                rows.append(
                    asdict(
                        MailboxAliasCandidateRecord(
                            mailbox_owner=mailbox_owner or "",
                            pivot_email=pivot_email,
                            pivot_name=pivot_name,
                            pivot_identity_type=pivot_identity_type,
                            candidate_email=candidate_email,
                            candidate_name=candidate_name,
                            candidate_domain=candidate_domain,
                            candidate_localpart=candidate_localpart,
                            candidate_identity_type=candidate_identity_type,
                            is_same_as_pivot=is_same_as_pivot,
                            is_internal_enron_email=bool(is_internal_enron),
                            is_external_email=is_external_email,
                            localpart_similarity_score=round(localpart_score, 4),
                            name_similarity_score=round(name_score, 4),
                            mailbox_owner_alignment_score=round(mailbox_owner_score, 4),
                            xfilename_alignment_score=round(xfilename_score, 4),
                            domain_alignment_score=round(domain_score, 4),
                            sender_alignment_score=round(sender_score, 4),
                            final_alias_score=final_score,
                            alias_label=alias_label,
                            alias_reasons=alias_reasons,
                        )
                    )
                )

        alias_df = pd.DataFrame(rows)

        if not alias_df.empty:
            alias_df = alias_df.sort_values(
                by=["final_alias_score", "mailbox_owner", "candidate_email"],
                ascending=[False, True, True],
            ).reset_index(drop=True)

        meta = {
            "mailbox_count": int(collaborators_df["mailbox_owner"].nunique())
            if not collaborators_df.empty
            else 0,
            "candidate_row_count": int(len(alias_df)),
            "strong_alias_count": int((alias_df["alias_label"] == "strong_alias").sum())
            if not alias_df.empty
            else 0,
            "possible_alias_count": int((alias_df["alias_label"] == "possible_alias").sum())
            if not alias_df.empty
            else 0,
            "unlikely_alias_count": int((alias_df["alias_label"] == "unlikely_alias").sum())
            if not alias_df.empty
            else 0,
            "not_alias_count": int((alias_df["alias_label"] == "not_alias").sum())
            if not alias_df.empty
            else 0,
        }

        return alias_df, meta

    def explore_mailbox_aliases(
        self,
        collaborators_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
        alias_df, meta = self.build_mailbox_alias_candidates_dataframe(collaborators_df)
        summary = self.compute_mailbox_alias_summary(alias_df, meta)
        tables = self.compute_mailbox_alias_tables(alias_df)
        return alias_df, summary, tables

    def save_mailbox_alias_reports(
        self,
        alias_df: pd.DataFrame,
        summary: dict[str, Any],
        tables: dict[str, Any],
        output_dir: Path,
    ) -> dict[str, str]:
        output_dir.mkdir(parents=True, exist_ok=True)

        csv_path = output_dir / "mailbox_alias_candidates.csv"
        summary_json_path = output_dir / "mailbox_alias_summary.json"
        tables_json_path = output_dir / "mailbox_alias_tables.json"

        alias_df.to_csv(csv_path, index=False)

        summary_json_path.write_text(
            json.dumps(summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        tables_json_path.write_text(
            json.dumps(tables, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        return {
            "csv": str(csv_path),
            "summary_json": str(summary_json_path),
            "tables_json": str(tables_json_path),
        }

    def run_and_save_mailbox_alias_exploration(
        self,
        *,
        collaborators_df: pd.DataFrame,
        output_dir: Path,
        source_csv_path: str | None = None,
    ) -> dict[str, Any]:
        output_dir.mkdir(parents=True, exist_ok=True)

        alias_df, summary, tables = self.explore_mailbox_aliases(collaborators_df)
        files = self.save_mailbox_alias_reports(
            alias_df=alias_df,
            summary=summary,
            tables=tables,
            output_dir=output_dir,
        )

        manifest = {
            "status": "success",
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "source_csv_path": source_csv_path,
            "input_row_count": int(len(collaborators_df)),
            "row_count": int(len(alias_df)),
            "files": files,
        }

        manifest_path = output_dir / "mailbox_alias_manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        return {
            **files,
            "manifest_json": str(manifest_path),
            "summary": summary,
            "tables": tables,
            "row_count": int(len(alias_df)),
        }

    def compute_mailbox_alias_summary(
        self,
        alias_df: pd.DataFrame,
        meta: dict[str, Any],
    ) -> dict[str, Any]:
        if alias_df.empty:
            return {
                **meta,
                "row_count": 0,
                "unique_pivot_emails": 0,
                "unique_candidate_emails": 0,
                "internal_candidate_count": 0,
                "external_candidate_count": 0,
                "avg_alias_score": 0.0,
                "top_alias_labels": {},
                "top_candidate_domains": {},
                "top_mailboxes_with_aliases": {},
                "top_pivot_emails": {},
            }

        score_series = alias_df["final_alias_score"].dropna()

        return {
            **meta,
            "row_count": int(len(alias_df)),
            "unique_pivot_emails": int(alias_df["pivot_email"].dropna().nunique()),
            "unique_candidate_emails": int(alias_df["candidate_email"].dropna().nunique()),
            "internal_candidate_count": int(
                alias_df["is_internal_enron_email"].fillna(False).astype(bool).sum()
            ),
            "external_candidate_count": int(
                alias_df["is_external_email"].fillna(False).astype(bool).sum()
            ),
            "avg_alias_score": round(float(score_series.mean()), 2) if not score_series.empty else 0.0,
            "top_alias_labels": (
                alias_df["alias_label"].fillna("<missing>").value_counts().head(20).to_dict()
            ),
            "top_candidate_domains": (
                alias_df["candidate_domain"].fillna("<missing>").value_counts().head(20).to_dict()
            ),
            "top_mailboxes_with_aliases": (
                alias_df["mailbox_owner"].fillna("<missing>").value_counts().head(20).to_dict()
            ),
            "top_pivot_emails": (
                alias_df["pivot_email"].fillna("<missing>").value_counts().head(20).to_dict()
            ),
        }

    def compute_mailbox_alias_tables(self, alias_df: pd.DataFrame) -> dict[str, Any]:
        if alias_df.empty:
            return {
                "alias_candidate_preview": [],
                "top_strong_aliases": [],
                "top_possible_aliases": [],
                "mailbox_alias_counts": [],
                "pivot_candidate_pairs": [],
            }

        preview_columns = [
            "mailbox_owner",
            "pivot_email",
            "pivot_name",
            "candidate_email",
            "candidate_name",
            "candidate_domain",
            "candidate_identity_type",
            "localpart_similarity_score",
            "name_similarity_score",
            "mailbox_owner_alignment_score",
            "xfilename_alignment_score",
            "sender_alignment_score",
            "final_alias_score",
            "alias_label",
            "alias_reasons",
        ]

        alias_candidate_preview = (
            alias_df[preview_columns].head(200).fillna("").to_dict(orient="records")
        )

        top_strong_aliases = (
            alias_df[alias_df["alias_label"] == "strong_alias"][preview_columns]
            .sort_values(by=["final_alias_score", "mailbox_owner"], ascending=[False, True])
            .head(200)
            .fillna("")
            .to_dict(orient="records")
        )

        top_possible_aliases = (
            alias_df[alias_df["alias_label"] == "possible_alias"][preview_columns]
            .sort_values(by=["final_alias_score", "mailbox_owner"], ascending=[False, True])
            .head(200)
            .fillna("")
            .to_dict(orient="records")
        )

        mailbox_alias_counts = (
            alias_df.groupby(["mailbox_owner", "alias_label"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(by=["count", "mailbox_owner"], ascending=[False, True])
            .head(200)
            .fillna("")
            .to_dict(orient="records")
        )

        pivot_candidate_pairs = (
            alias_df[["pivot_email", "candidate_email", "alias_label"]]
            .fillna("")
            .value_counts()
            .reset_index(name="count")
            .sort_values(by=["count", "pivot_email"], ascending=[False, True])
            .head(200)
            .to_dict(orient="records")
        )

        return {
            "alias_candidate_preview": alias_candidate_preview,
            "top_strong_aliases": top_strong_aliases,
            "top_possible_aliases": top_possible_aliases,
            "mailbox_alias_counts": mailbox_alias_counts,
            "pivot_candidate_pairs": pivot_candidate_pairs,
        }

    def _split_pipe(self, value: str | None) -> list[str]:
        value = self.email_service.normalize_text(value)
        if not value:
            return []
        return [part.strip() for part in value.split("|") if part and part.strip()]

    def _normalize_email_value(self, value: str | None) -> str | None:
        value = self.email_service.normalize_text(value)
        if not value:
            return None
        value = value.strip().lower().strip("'\"")
        return value or None

    def _normalize_name_value(self, value: str | None) -> str | None:
        value = self.email_service.normalize_text(value)
        if not value:
            return None

        value = value.strip().strip("'\"")

        if "," in value:
            left, right = [p.strip() for p in value.split(",", 1)]
            if left and right:
                value = f"{right} {left}"

        tokens = [t for t in re.split(r"\s+", value) if t]
        tokens = [t for t in tokens if len(t) > 1]
        if not tokens:
            return None

        return " ".join(token.capitalize() for token in tokens)

    def _normalize_localpart_for_alias(self, localpart: str | None) -> str | None:
        if not localpart:
            return None
        localpart = localpart.lower()
        localpart = re.sub(r"[^a-z0-9]", "", localpart)
        return localpart or None

    def _extract_name_tokens(self, name: str | None) -> set[str]:
        return self.identity_service.tokenize_alpha(self._normalize_name_value(name))

    def _extract_xfilename_hints(self, values: list[str]) -> set[str]:
        hints: set[str] = set()

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
                    hints.add(token)

        return hints

    def _jaccard_score(self, set_a: set[str], set_b: set[str]) -> float:
        if not set_a or not set_b:
            return 0.0
        inter = len(set_a.intersection(set_b))
        union = len(set_a.union(set_b))
        return inter / union if union else 0.0

    def _localpart_similarity(self, localpart_a: str | None, localpart_b: str | None) -> float:
        norm_a = self._normalize_localpart_for_alias(localpart_a)
        norm_b = self._normalize_localpart_for_alias(localpart_b)

        if not norm_a or not norm_b:
            return 0.0
        if norm_a == norm_b:
            return 1.0

        score = self._jaccard_score(set(norm_a), set(norm_b))

        if norm_a in norm_b or norm_b in norm_a:
            score = max(score, 0.85)

        return round(score, 4)

    def _name_similarity(self, name_a: str | None, name_b: str | None) -> float:
        tokens_a = self._extract_name_tokens(name_a)
        tokens_b = self._extract_name_tokens(name_b)

        if not tokens_a or not tokens_b:
            return 0.0

        return round(self._jaccard_score(tokens_a, tokens_b), 4)

    def _compute_mailbox_owner_alignment_score(
        self,
        *,
        mailbox_owner: str | None,
        candidate_name: str | None,
        candidate_email: str | None,
    ) -> float:
        owner_tokens = self.identity_service.tokenize_alpha(mailbox_owner)
        name_tokens = self._extract_name_tokens(candidate_name)
        email_tokens = self.identity_service.tokenize_alpha(
            self.email_service.extract_email_localpart(candidate_email)
        )

        score = 0.0
        if owner_tokens and owner_tokens.intersection(name_tokens):
            score += 0.5
        if owner_tokens and owner_tokens.intersection(email_tokens):
            score += 0.5

        return round(min(score, 1.0), 4)

    def _compute_xfilename_alignment_score(
        self,
        *,
        xfilename_hints: set[str],
        candidate_name: str | None,
        candidate_email: str | None,
    ) -> float:
        if not xfilename_hints:
            return 0.0

        name_tokens = self._extract_name_tokens(candidate_name)
        email_tokens = self.identity_service.tokenize_alpha(
            self.email_service.extract_email_localpart(candidate_email)
        )

        score = 0.0
        if xfilename_hints.intersection(name_tokens):
            score += 0.5
        if xfilename_hints.intersection(email_tokens):
            score += 0.5

        return round(min(score, 1.0), 4)

    def _compute_domain_alignment_score(
        self,
        *,
        pivot_email: str | None,
        candidate_email: str | None,
    ) -> float:
        pivot_domain = self.email_service.extract_email_domain(pivot_email)
        candidate_domain = self.email_service.extract_email_domain(candidate_email)

        if not pivot_domain or not candidate_domain:
            return 0.0
        if pivot_domain == candidate_domain:
            return 1.0

        if pivot_domain.endswith("enron.com") and candidate_domain.endswith("enron.com"):
            return 0.8

        if pivot_domain.split(".")[-2:] == candidate_domain.split(".")[-2:]:
            return 0.4

        return 0.0

    def _compute_sender_alignment_score(
        self,
        *,
        dominant_sender_email: str | None,
        candidate_email: str | None,
    ) -> float:
        dominant_sender_email = self._normalize_email_value(dominant_sender_email)
        candidate_email = self._normalize_email_value(candidate_email)

        if not dominant_sender_email or not candidate_email:
            return 0.0
        if dominant_sender_email == candidate_email:
            return 1.0

        return self._localpart_similarity(
            self.email_service.extract_email_localpart(dominant_sender_email),
            self.email_service.extract_email_localpart(candidate_email),
        )

    def _is_generic_or_system_candidate(
        self,
        email: str | None,
        candidate_identity_type: str | None,
    ) -> bool:
        if not email:
            return True

        localpart = (self.email_service.extract_email_localpart(email) or "").lower()
        blocked_localparts = {
            "admin",
            "administrator",
            "support",
            "help",
            "info",
            "mail",
            "announce",
            "announcements",
            "team",
            "all",
            "office",
            "chairman",
            "outlook",
            "perfmgmt",
            "postmaster",
            "noreply",
            "no-reply",
            "webmaster",
            "customerservice",
            "newsletter",
        }

        if candidate_identity_type in {"corporate", "system", "external_bulk"}:
            return True

        if localpart in blocked_localparts:
            return True

        if localpart.startswith("all.") or localpart.startswith("dl-") or localpart.startswith("dl_"):
            return True

        return False

    def _build_alias_reasons(
        self,
        *,
        localpart_score: float,
        name_score: float,
        mailbox_owner_score: float,
        xfilename_score: float,
        domain_score: float,
        sender_score: float,
        candidate_identity_type: str | None,
    ) -> str:
        reasons: list[str] = []

        if localpart_score >= 0.95:
            reasons.append("localpart quasi identique")
        elif localpart_score >= 0.75:
            reasons.append("localpart proche")

        if name_score >= 0.95:
            reasons.append("nom quasi identique")
        elif name_score >= 0.75:
            reasons.append("nom proche")

        if mailbox_owner_score >= 1.0:
            reasons.append("fort alignement mailbox_owner")
        elif mailbox_owner_score > 0:
            reasons.append("alignement mailbox_owner")

        if xfilename_score >= 1.0:
            reasons.append("fort alignement X-FileName")
        elif xfilename_score > 0:
            reasons.append("alignement X-FileName")

        if domain_score >= 1.0:
            reasons.append("même domaine")
        elif domain_score >= 0.8:
            reasons.append("domaine Enron compatible")

        if sender_score >= 1.0:
            reasons.append("même sender dominant")
        elif sender_score >= 0.75:
            reasons.append("sender dominant proche")

        if candidate_identity_type == "person":
            reasons.append("profil personne")
        elif candidate_identity_type == "likely_person":
            reasons.append("profil probablement personne")
        elif candidate_identity_type in {"corporate", "system", "external_bulk"}:
            reasons.append("profil non personnel")

        return self.identity_service.join_pipe(reasons)

    def _alias_label_from_score(self, score: float, candidate_identity_type: str | None) -> str:
        if candidate_identity_type in {"corporate", "system", "external_bulk"}:
            if score >= 55:
                return "possible_alias"
            return "not_alias"

        if score >= 70:
            return "strong_alias"
        if score >= 45:
            return "possible_alias"
        if score >= 25:
            return "unlikely_alias"
        return "not_alias"