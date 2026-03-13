from __future__ import annotations

from email import policy
from email.parser import BytesParser
from pathlib import Path
from email.message import EmailMessage


class EmailFilesystemExplorationService:
    def safe_parse_email(self, file_path: Path) -> EmailMessage | None:
        try:
            with file_path.open("rb") as f:
                return BytesParser(policy=policy.default).parse(f)
        except Exception:
            return None