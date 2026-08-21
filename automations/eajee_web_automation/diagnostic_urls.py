"""Safe URL rendering for browser-automation diagnostics."""

from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit


_URL_IN_TEXT = re.compile(r"(?<!\w)(?:(?:https?:)?//|/)[^\s<>\"']+")


def safe_url_for_log(value: object) -> str:
    """Return URL identity without query or fragment material."""

    try:
        text = str(value or "")
    except Exception:
        return "<unavailable-url>"

    try:
        parts = urlsplit(text)
        if parts.scheme or parts.netloc:
            return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
        return parts.path
    except Exception:
        return "<unavailable-url>"


def safe_diagnostic_text(value: object) -> str:
    """Remove query and fragment material from URLs embedded in diagnostics."""

    try:
        text = str(value or "")
    except Exception:
        return "<unavailable-diagnostic>"
    return _URL_IN_TEXT.sub(lambda match: safe_url_for_log(match.group(0)), text)
