import os
import requests
from dotenv import load_dotenv

load_dotenv()

def upload_html(filepath):
    upload_url = os.getenv("EAJEE_UPLOAD_URL", "").strip()

    if not upload_url:
        return "SKIPPED_UPLOAD: EAJEE_UPLOAD_URL not set in .env"

    with open(filepath, "rb") as f:
        response = requests.post(
            upload_url,
            files={"file": (filepath.split("/")[-1], f, "text/html")},
            data={"source": "marketsmojo"},
            timeout=60,
        )

    response.raise_for_status()
    return response.text
