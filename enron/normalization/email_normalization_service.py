from __future__ import annotations

import re
from datetime import datetime
from email.utils import getaddresses, parsedate_to_datetime


class EmailNormalizationService:
    """
    Service utilitaire de normalisation des données email.

    Ce service ne dépend pas du parsing complet d'un fichier.
    Il fournit des fonctions réutilisables pour :
    - les headers
    - les adresses
    - les Message-ID
    - les sujets
    - les dates
    - le body
    """

    _MULTISPACE_RE = re.compile(r"\s+")
    _MESSAGE_ID_RE = re.compile(r"<[^<>]+>")
    _SUBJECT_PREFIX_RE = re.compile(
        r"^((re|fw|fwd)\s*:\s*)+",
        flags=re.IGNORECASE,
    )

    # Email "tolérant" pour datasets sales type Enron.
    # Accepte notamment:
    # - 1.10969419.-2@multexinvestornetwork.com
    # - 12152@enron.com
    # - 00.walt@enron.com
    _EMAIL_CANDIDATE_RE = re.compile(
        r"(?i)\b[a-z0-9][a-z0-9._%+\-']*@(?:[a-z0-9-]+\.)+[a-z]{2,}\b"
    )

    _SURROUNDING_JUNK_RE = re.compile(r"^[\s<>\(\)\[\]\{\},;:]+|[\s<>\(\)\[\]\{\},;:]+$")
    _LEADING_MAILTO_RE = re.compile(r"(?i)^mailto:")
    _QUOTES_AROUND_RE = re.compile(r"""^(["']+)|(["']+)$""")
    _ANGLE_ADDR_RE = re.compile(r"<\s*([^<>@\s]+@[^<>@\s]+)\s*>")
    _AT_LEAST_ONE_AT_RE = re.compile(r"@")

    _HTML_SCRIPT_STYLE_RE = re.compile(r"(?is)<(script|style)\b[^>]*>.*?</\1>")
    _HTML_BR_RE = re.compile(r"(?i)<br\s*/?>")
    _HTML_P_RE = re.compile(r"(?i)</p\s*>")
    _HTML_TAG_RE = re.compile(r"(?s)<[^>]+>")
    
    _QUOTED_LINE_RE = re.compile(r"^\s*>+")
    _ON_WROTE_RE = re.compile(r"(?im)^\s*on .+ wrote:\s*$")
    _ORIGINAL_MESSAGE_RE = re.compile(r"(?im)^\s*-{2,}\s*original message\s*-{2,}\s*$")
    _FORWARDED_MESSAGE_RE = re.compile(
        r"(?im)^\s*(?:begin forwarded message|[- ]*forwarded by|[- ]*forwarded message)\b"
    )
    _QUOTED_HEADER_BLOCK_RE = re.compile(
        r"(?im)^\s*(from|sent|to|cc|bcc|subject|date)\s*:\s+.+$"
    )
    
    def normalize_text(self, value: str | None) -> str | None:
        """
        Nettoie un texte simple :
        - trim
        - collapse des espaces
        - None si vide
        """
        if value is None:
            return None

        value = value.strip()
        if not value:
            return None

        value = self._MULTISPACE_RE.sub(" ", value)
        return value or None

    def _strip_surrounding_email_junk(self, value: str) -> str:
        value = value.strip()
        value = self._LEADING_MAILTO_RE.sub("", value).strip()
        value = self._SURROUNDING_JUNK_RE.sub("", value).strip()

        # enlève les quotes parasites en bordure seulement
        # ex:
        #   "'12152'@enron.com" -> 12152'@enron.com puis plus bas -> 12152@enron.com
        #   "'00.walt@enron.com" -> 00.walt@enron.com
        value = self._QUOTES_AROUND_RE.sub("", value).strip()

        # si toute la valeur est entre <...>
        if value.startswith("<") and value.endswith(">"):
            value = value[1:-1].strip()

        return value

    def _salvage_email_candidate(self, value: str) -> str | None:
        """
        Essaie de récupérer une adresse plausible depuis une chaîne bruitée.
        """
        if not value:
            return None

        value = self._strip_surrounding_email_junk(value)

        if not value:
            return None

        # Cas "<foo@bar.com>"
        angle_match = self._ANGLE_ADDR_RE.search(value)
        if angle_match:
            value = angle_match.group(1).strip()

        # Si plusieurs @, on laisse tomber: trop ambigu
        if len(self._AT_LEAST_ONE_AT_RE.findall(value)) != 1:
            # fallback: cherche un email plausible quelque part dans la chaîne
            match = self._EMAIL_CANDIDATE_RE.search(value)
            return match.group(0).lower() if match else None

        # Nettoyage local-part / domaine
        local, domain = value.split("@", 1)
        local = local.strip()
        domain = domain.strip().lower().strip(".")
        domain = re.sub(r"\.+", ".", domain)

        # apostrophes parasites au bord du local-part
        local = local.strip(" '\"")
        domain = domain.strip(" '\"")

        # cas "'12152'@enron.com" -> "12152@enron.com"
        local = local.replace("'", "")

        # collapse ponctuation excessive dans le local-part
        local = re.sub(r"\.{2,}", ".", local)
        local = re.sub(r"-{2,}", "-", local)

        # retire ponctuation parasite en bordure
        local = local.strip(" .-_")
        domain = domain.strip(" .-_")

        if not local or not domain:
            return None

        candidate = f"{local}@{domain}"

        # Validation légère, volontairement tolérante
        if not re.fullmatch(r"(?i)[a-z0-9][a-z0-9._%+\-]*@(?:[a-z0-9-]+\.)+[a-z]{2,}", candidate):
            # dernier fallback: extraire un email plausible de la candidate
            match = self._EMAIL_CANDIDATE_RE.search(candidate)
            return match.group(0).lower() if match else None

        return candidate.lower()

    def normalize_email_address(self, value: str | None) -> str | None:
        """
        Normalise une adresse email de manière robuste pour datasets réels :
        - trim
        - lower
        - suppression / réparation légère des chevrons, quotes, mailto:
        - récupération tolérante d'adresses bruitées
        - None si vide ou irrécupérable
        """
        if value is None:
            return None

        value = value.strip()
        if not value:
            return None

        return self._salvage_email_candidate(value)

    def extract_email_local_part(self, email: str | None) -> str | None:
        """
        Retourne la partie locale d'une adresse email.
        """
        email = self.normalize_email_address(email)
        if not email or "@" not in email:
            return None
        return email.split("@", 1)[0] or None

    def extract_email_domain(self, email: str | None) -> str | None:
        """
        Retourne le domaine d'une adresse email.
        """
        email = self.normalize_email_address(email)
        if not email or "@" not in email:
            return None
        return email.split("@", 1)[1] or None

    def is_internal_enron_email(self, email: str | None) -> bool | None:
        """
        Indique si l'adresse appartient au domaine enron.com.
        """
        domain = self.extract_email_domain(email)
        if domain is None:
            return None
        return domain == "enron.com"

    def _extract_fallback_emails_from_header(self, header_value: str) -> list[str]:
        """
        Extraction tolérante d'emails depuis un header mal formé.
        """
        candidates: list[str] = []

        # 1) emails déjà bien visibles dans le texte
        for raw in self._EMAIL_CANDIDATE_RE.findall(header_value):
            normalized = self.normalize_email_address(raw)
            if normalized:
                candidates.append(normalized)

        # 2) tokens grossiers séparés par virgule / point-virgule
        rough_tokens = re.split(r"[;,]", header_value)
        for token in rough_tokens:
            normalized = self.normalize_email_address(token)
            if normalized:
                candidates.append(normalized)

        # déduplication en préservant l'ordre
        seen: set[str] = set()
        result: list[str] = []
        for item in candidates:
            if item not in seen:
                seen.add(item)
                result.append(item)

        return result

    def parse_address_header(self, header_value: str | None) -> list[tuple[str | None, str | None]]:
        """
        Parse un header d'adresses (From, To, Cc, Bcc, Reply-To).

        Retour :
            [(display_name, email_normalized), ...]
        """
        if not header_value:
            return []

        pairs: list[tuple[str | None, str | None]] = []

        # Premier passage standard
        parsed = getaddresses([header_value])
        for display_name, email in parsed:
            normalized_name = self.normalize_text(display_name)
            normalized_email = self.normalize_email_address(email)

            # parfois getaddresses met toute la chaîne dans display_name et rien dans email
            if normalized_email is None and normalized_name:
                salvaged = self.normalize_email_address(normalized_name)
                if salvaged:
                    normalized_email = salvaged
                    normalized_name = None

            if normalized_name or normalized_email:
                pairs.append((normalized_name, normalized_email))

        # Fallback si rien d'utile, ou si certaines adresses ont été ratées
        fallback_emails = self._extract_fallback_emails_from_header(header_value)

        existing_emails = {email for _, email in pairs if email}
        for email in fallback_emails:
            if email not in existing_emails:
                pairs.append((None, email))
                existing_emails.add(email)

        return pairs

    def normalize_message_id(self, value: str | None) -> str | None:
        """
        Normalise un Message-ID ou une valeur de type In-Reply-To :
        - trim
        - extraction du premier token entre chevrons si présent
        - lower
        """
        value = self.normalize_text(value)
        if not value:
            return None

        match = self._MESSAGE_ID_RE.search(value)
        if match:
            return match.group(0).strip().lower()

        return value.lower()

    def parse_references_header(self, header_value: str | None) -> list[str]:
        """
        Extrait les Message-ID d'un header References dans l'ordre.
        """
        if not header_value:
            return []

        matches = self._MESSAGE_ID_RE.findall(header_value)
        if matches:
            return [item.strip().lower() for item in matches if item.strip()]

        normalized = self.normalize_message_id(header_value)
        return [normalized] if normalized else []

    def normalize_subject(self, subject: str | None) -> str | None:
        """
        Normalise légèrement le sujet :
        - trim / collapse espaces
        - retire les préfixes répétés Re:, Fw:, Fwd:
        - lower
        """
        subject = self.normalize_text(subject)
        if not subject:
            return None

        subject = self._SUBJECT_PREFIX_RE.sub("", subject).strip()
        subject = self._MULTISPACE_RE.sub(" ", subject)
        return subject.lower() or None

    def parse_email_date(self, value: str | None) -> datetime | None:
        """
        Parse une date email en datetime Python.
        """
        value = self.normalize_text(value)
        if not value:
            return None

        try:
            dt = parsedate_to_datetime(value)
            return dt
        except Exception:
            return None

    def normalize_body_text(self, value: str | None) -> str | None:
        """
        Normalise légèrement un body texte :
        - normalise les fins de ligne
        - retire les espaces inutiles en fin de ligne
        - trim global
        """
        if value is None:
            return None

        value = value.replace("\r\n", "\n").replace("\r", "\n")
        lines = [line.rstrip() for line in value.split("\n")]
        cleaned = "\n".join(lines).strip()

        return cleaned or None

    def clean_body_text(self, value: str | None) -> str | None:
        """
        Nettoyage plus agressif du body :
        - passe par normalize_body_text
        - supprime les lignes purement vides répétées
        """
        value = self.normalize_body_text(value)
        if not value:
            return None

        cleaned_lines: list[str] = []
        previous_blank = False

        for line in value.split("\n"):
            is_blank = not line.strip()
            if is_blank and previous_blank:
                continue
            cleaned_lines.append(line)
            previous_blank = is_blank

        cleaned = "\n".join(cleaned_lines).strip()
        return cleaned or None

    def count_quoted_lines(self, value: str | None) -> int:
        """
        Compte les lignes qui ressemblent à des citations (> ...).
        """
        value = self.normalize_body_text(value)
        if not value:
            return 0

        return sum(1 for line in value.splitlines() if line.lstrip().startswith(">"))

    def is_reply_subject(self, subject: str | None) -> bool:
        """
        Détecte si le sujet ressemble à une réponse.
        """
        if not subject:
            return False
        return bool(re.match(r"^\s*re\s*:", subject, flags=re.IGNORECASE))

    def is_forward_subject(self, subject: str | None) -> bool:
        """
        Détecte si le sujet ressemble à un transfert.
        """
        if not subject:
            return False
        return bool(re.match(r"^\s*(fw|fwd)\s*:", subject, flags=re.IGNORECASE))

    def count_addresses_in_header(self, header_value: str | None) -> int:
        """
        Compte le nombre d'adresses parsées dans un header.
        """
        return len(self.parse_address_header(header_value))
    
    def normalize_html_body(self, value: str | None) -> str | None:
        """
        Convertit un body HTML en texte simple puis le normalise.
        """
        if value is None:
            return None

        value = value.replace("\r\n", "\n").replace("\r", "\n")

        value = self._HTML_SCRIPT_STYLE_RE.sub(" ", value)
        value = self._HTML_BR_RE.sub("\n", value)
        value = self._HTML_P_RE.sub("\n", value)
        value = self._HTML_TAG_RE.sub(" ", value)

        html_entities = {
            "&nbsp;": " ",
            "&amp;": "&",
            "&lt;": "<",
            "&gt;": ">",
            "&quot;": '"',
            "&#39;": "'",
        }
        for src, dst in html_entities.items():
            value = value.replace(src, dst)

        return self.clean_body_text(value)
    
    def extract_email_localpart(self, email: str | None) -> str | None:
        if not email or "@" not in email:
            return None
        return email.split("@", 1)[0].lower()


    def derive_name_from_email(self, email: str | None, generic_localparts: set[str]) -> str | None:
        localpart = self.extract_email_localpart(email)
        if not localpart:
            return None

        cleaned = re.sub(r"[^a-zA-Z._ -]", " ", localpart)
        parts = [p for p in re.split(r"[._\-\s]+", cleaned) if p]
        parts = [p for p in parts if p.lower() not in generic_localparts]

        if not parts:
            return None

        return " ".join(part.capitalize() for part in parts[:3])
    
    def extract_xfrom_name(self, value: str | None) -> str | None:
        value = self.normalize_text(value)
        if not value:
            return None

        value = re.sub(r"\s*\(e-mail\)\s*$", "", value, flags=re.IGNORECASE).strip()
        value = re.sub(r"\s*<[^>]+>\s*$", "", value).strip()
        return self.normalize_text(value)
    
    def normalize_subject_for_threading(self, subject: str | None) -> str | None:
        return self.normalize_subject(subject)

    def body_looks_like_reply(self, value: str | None) -> bool:
        value = self.normalize_body_text(value)
        if not value:
            return False

        if self.count_quoted_lines(value) > 0:
            return True
        if self._ON_WROTE_RE.search(value):
            return True
        if self._ORIGINAL_MESSAGE_RE.search(value):
            return True
        if len(self.extract_quoted_header_lines(value)) >= 2:
            return True

        return False

    def body_looks_like_forward(self, value: str | None) -> bool:
        value = self.normalize_body_text(value)
        if not value:
            return False

        if self._FORWARDED_MESSAGE_RE.search(value):
            return True
        if self._ORIGINAL_MESSAGE_RE.search(value):
            return True

        header_lines = self.extract_quoted_header_lines(value)
        if len(header_lines) >= 3 and any(
            line.lower().startswith("subject:") for line in header_lines
        ):
            return True

        return False
    
    def extract_quoted_header_lines(self, value: str | None) -> list[str]:
        value = self.normalize_body_text(value)
        if not value:
            return []

        result: list[str] = []
        for line in value.splitlines():
            if self._QUOTED_HEADER_BLOCK_RE.match(line):
                result.append(line.strip())
        return result