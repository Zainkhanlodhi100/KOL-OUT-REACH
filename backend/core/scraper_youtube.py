"""
core/scraper_youtube.py

ToS-compliant lead sourcing via the official YouTube Data API v3.

What this does NOT do: scrape pages, bypass rate limits, or extract private
data. It reads two things every channel owner explicitly chooses to publish:

  1. The channel's public "Description" field (search.list + channels.list),
     which creators frequently use to list a business-inquiries email —
     that's the entire point of the field existing.
  2. Public snippet/statistics data (subscriber count, title, channel URL).

This is meaningfully different from the X/Twitter scraping approach: the
data source is an official, rate-limited, ToS-sanctioned API, and the email
addresses extracted are ones the channel owner deliberately published FOR
business contact — which is also why every Lead created here is tagged
`consent_basis="public_business_contact"` for your GDPR audit trail.

Quota note: YouTube Data API v3 free tier is 10,000 units/day.
  - search.list  costs 100 units/call (50 results/call)
  - channels.list costs 1 unit/call (up to 50 IDs/call)
So ~90 search calls/day max on the free tier — plan query batches
accordingly. This module batches channel detail lookups to stay efficient.
"""

import logging
import re
from typing import Optional

import httpx

from backend.core.config import settings

logger = logging.getLogger(__name__)

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"

# Deliberately conservative email regex — precision over recall. False
# positives (matching a non-contact string) are worse than false negatives
# here, since a false positive means emailing an address that was never
# meant for business contact.
_EMAIL_RE = re.compile(
    r"[a-zA-Z0-9][a-zA-Z0-9._%+-]{1,63}@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
)

# Phrases that typically precede a genuine business-contact email in a
# channel description, used to bias extraction toward intentional business
# contacts rather than e.g. an email accidentally pasted elsewhere.
_BUSINESS_CONTEXT_HINTS = (
    "business inquiries", "business inquiry", "for business",
    "contact:", "contact us", "collab", "collaboration",
    "sponsorship", "partnerships", "management:",
)


def _extract_business_email(description: str) -> Optional[str]:
    """
    Extract an email from a channel description, preferring ones that
    appear near a business-contact hint phrase. Returns None if no email
    is found — we do NOT guess or construct addresses.
    """
    if not description:
        return None

    lower = description.lower()
    emails = _EMAIL_RE.findall(description)
    if not emails:
        return None

    # If any business-context hint appears, prefer the email closest to it.
    for hint in _BUSINESS_CONTEXT_HINTS:
        idx = lower.find(hint)
        if idx == -1:
            continue
        # find nearest email to this hint by character distance
        best = min(
            emails,
            key=lambda e: abs(description.lower().find(e.lower()) - idx),
        )
        return best

    # No explicit hint found — still return the first email present, since
    # many creators just list it plainly without a preceding label. This is
    # a judgment call; tighten to `return None` here if you'd rather be
    # stricter and only take explicitly-labeled business emails.
    return emails[0]


async def search_channels(niche: str, max_results: int = 25) -> list[dict]:
    """
    Search for channels matching a niche keyword. Returns raw channel IDs
    plus search snippet — does NOT yet contain the description/email, since
    search.list doesn't return full descriptions (channels.list does).
    """
    params = {
        "part": "snippet",
        "type": "channel",
        "q": niche,
        "maxResults": min(max_results, 50),
        "key": settings.YOUTUBE_API_KEY,
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(f"{YOUTUBE_API_BASE}/search", params=params)
        resp.raise_for_status()
        data = resp.json()

    return [
        {
            "channel_id": item["snippet"]["channelId"],
            "title": item["snippet"]["title"],
        }
        for item in data.get("items", [])
    ]


async def fetch_channel_details(channel_ids: list[str]) -> list[dict]:
    """
    Batch-fetch full channel details (description, subscriber count, URL)
    for up to 50 channel IDs per call — this is the 1-unit-cost endpoint,
    so always batch here rather than calling per-channel.
    """
    if not channel_ids:
        return []

    results = []
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i + 50]
        params = {
            "part": "snippet,statistics",
            "id": ",".join(batch),
            "key": settings.YOUTUBE_API_KEY,
        }
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"{YOUTUBE_API_BASE}/channels", params=params)
            resp.raise_for_status()
            data = resp.json()

        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            results.append({
                "channel_id": item["id"],
                "name": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "subscriber_count": int(stats.get("subscriberCount", 0) or 0),
                "profile_url": f"https://www.youtube.com/channel/{item['id']}",
            })

    return results


async def scrape_youtube_leads(niche: str, max_results: int = 25) -> list[dict]:
    """
    Full pipeline: search -> fetch details -> extract business email ->
    return only channels where a business-contact email was actually found.

    This is the function `/start-engine` should call for platform="youtube".
    Returns a list of dicts ready to insert as Lead rows.
    """
    channels = await search_channels(niche, max_results=max_results)
    if not channels:
        logger.warning(f"YouTube search returned 0 channels for niche='{niche}'")
        return []

    details = await fetch_channel_details([c["channel_id"] for c in channels])

    leads = []
    for ch in details:
        email = _extract_business_email(ch["description"])
        if not email:
            continue  # no published business contact — skip, don't guess

        leads.append({
            "name": ch["name"],
            "username": ch["channel_id"],
            "email": email,
            "platform": "youtube",
            "niche": niche,
            "followers": ch["subscriber_count"],
            "bio": ch["description"][:2000],
            "profile_url": ch["profile_url"],
            "status": "scraped",
            "consent_basis": "public_business_contact",
        })

    logger.info(
        f"YouTube scrape for niche='{niche}': {len(details)} channels checked, "
        f"{len(leads)} had a published business email"
    )
    return leads
