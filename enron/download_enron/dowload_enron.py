from __future__ import annotations

from pathlib import Path

import requests

URL = "https://www.dropbox.com/s/y6dk4axijs34k3u/EnronDataOrg_AED_Mailbox-PSTs_20090122.7z?dl=1"


def download_enron() -> Path:
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)

    output_file = data_dir / "EnronDataOrg_AED_Mailbox-PSTs_20090122.7z"

    if output_file.exists():
        return output_file

    with requests.get(URL, stream=True, timeout=60) as response:
        response.raise_for_status()

        with output_file.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    return output_file