"""
core/scraper.py
Fused ingestion engine: Apify Google Search Scraper + YouTube Data API v3
+ DuckDuckGo regex fallback for Twitter/Instagram.
"""

import re
import time
import random
import logging
import requests
from apify_client import ApifyClient
from backend.core.config import settings

logger = logging.getLogger(__name__)

# ─── CONSTANTS ────────────────────────────────────────────────────────────────

SHORTS_INDICATORS = [
    "shorts", "#shorts", "short video", "tiktok", "reels only",
    "clips channel", "highlights only",
]

JUNK_EMAILS = [
    "noreply", "no-reply", "example", "youtube", "google",
    "donotreply", "support@", "admin@", "test@",
]

BLOCKED_PATHS = [
    "/status/", "/hashtag/", "/explore", "/search",
    "/p/", "/reel/", "/watch", "/shorts/",
]

REGIONS = ["US", "GB", "CA", "AU", "DE", "FR", "NL", "SG", "AE", "ZA", "NG", "KE", "PH", "MY", "IN"]

YOUTUBE_QUERIES = {
    "crypto": [
        "crypto trading channel", "bitcoin trading education", "cryptocurrency investing",
        "crypto signals channel", "altcoin trading strategies", "bitcoin analysis youtube",
        "crypto technical analysis channel", "ethereum trading channel",
        "crypto portfolio tutorial", "defi investing education",
        "blockchain trading channel", "crypto day trading", "bitcoin price analysis",
        "crypto market education", "web3 trading channel", "crypto swing trading",
        "best crypto educator", "crypto passive income channel", "bitcoin investing guide",
        "crypto trading tips", "altcoin season trading", "crypto scalping strategies",
        "crypto futures trading", "cryptocurrency news analysis", "crypto options trading",
    ],
    "forex": [
        "forex trading education", "forex signals channel", "fx trading strategies",
        "forex for beginners", "price action forex trading", "forex scalping channel",
        "funded trader forex", "prop firm trading education", "forex swing trading",
        "smart money forex", "forex technical analysis", "best forex educator",
        "forex trading income", "currency trading channel", "forex market analysis",
    ],
    "trading": [
        "day trading education channel", "options trading tutorial",
        "stock trading strategies", "futures trading channel", "trading chart patterns",
    ],
    "finance": [
        "passive income investing channel", "financial freedom education", "wealth building channel",
    ],
    "investing": [
        "stock market investing channel", "dividend investing education", "long term investing guide",
    ],
}

APIFY_KEYWORD_POOL = [
    'site:twitter.com "prop firm" OR "funded trader"',
    'site:twitter.com "forex trader" OR "forex mentor"',
    'site:twitter.com "crypto trader" OR "crypto analyst"',
    'site:twitter.com "smart money concepts" OR "SMC trader"',
    'site:twitter.com "AI crypto" OR "algo trading"',
    'site:twitter.com "day trader" OR "swing trader"',
    'site:instagram.com "prop firm" OR "funded trader"',
    'site:instagram.com "forex trader" OR "forex lifestyle"',
    'site:instagram.com "crypto trader" OR "web3 investor"',
    'site:instagram.com "day trader" OR "technical analysis"',
    'site:youtube.com "day in the life of a funded trader"',
    'site:youtube.com "forex trading strategy" OR "live trading"',
    'site:youtube.com "prop firm payout" OR "passing FTMO"',
    'site:youtube.com "crypto market analysis" OR "altcoin gems"',
    'site:linkedin.com/in "prop firm founder" OR "funded trader"',
    'site:linkedin.com/in "algo trader" OR "quantitative analyst"',
    'site:linkedin.com/in "crypto founder" OR "web3 builder"',
]

# Per-campaign rotating state (in-memory; keyed by campaign_id)
_campaign_state: dict = {}


# ─── PUBLIC ENTRY POINT ───────────────────────────────────────────────────────

def scrape_all(platforms: list, niches: list, campaign_id: str) -> list[dict]:
    """
    Returns a flat list of creator dicts:
    {name, username, email, followers, bio, profile_url, platform, niche, country}
    """
    results = []
    for platform in platforms:
        try:
            if platform == "youtube":
                results.extend(_scrape_youtube(niches, campaign_id))
            elif platform == "twitter":
                results.extend(_scrape_duckduckgo("twitter", niches))
            elif platform == "instagram":
                results.extend(_scrape_duckduckgo("instagram", niches))
            elif platform == "apify":
                results.extend(_scrape_apify(niches, campaign_id))
        except Exception as e:
            logger.error(f"Scrape error [{platform}]: {e}")
    return results


# ─── APIFY GOOGLE SEARCH SCRAPER ─────────────────────────────────────────────

def _scrape_apify(niches: list, campaign_id: str) -> list[dict]:
    client = ApifyClient(settings.APIFY_API_TOKEN)

    state = _campaign_state.get(campaign_id, {})
    pool_index = state.get("apify_pool_index", 0)
    keyword = APIFY_KEYWORD_POOL[pool_index % len(APIFY_KEYWORD_POOL)]
    _campaign_state[campaign_id] = {**state, "apify_pool_index": pool_index + 1}

    logger.info(f"Apify query: {keyword}")

    try:
        run = client.actor("apify/google-search-scraper").call(run_input={
            "queries": keyword,
            "maxPagesPerQuery": 3,
            "resultsPerPage": 50,
            "languageCode": "en",
            "countryCode": "us",
        })
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    except Exception as e:
        logger.error(f"Apify actor error: {e}")
        return []

    leads = []
    for item in items:
        for result in item.get("organicResults", []):
            url     = result.get("url", "")
            title   = result.get("title", "")
            snippet = result.get("description", "")

            clean_url = url.split("?")[0].strip()
            if any(b in clean_url.lower() for b in BLOCKED_PATHS):
                continue

            display_name = title.split(" - ")[0].split(" | ")[0].split(" (@")[0].strip()
            followers = _parse_followers(snippet)
            platform  = _detect_platform(clean_url)
            niche     = niches[0] if niches else "crypto"

            leads.append({
                "name":        display_name,
                "username":    "",
                "email":       None,
                "followers":   followers,
                "bio":         snippet[:500],
                "profile_url": clean_url,
                "platform":    platform,
                "niche":       niche,
                "country":     "",
            })

    logger.info(f"Apify extracted {len(leads)} raw profiles")
    return leads


# ─── YOUTUBE DATA API v3 ──────────────────────────────────────────────────────

def _scrape_youtube(niches: list, campaign_id: str) -> list[dict]:
    api_key = settings.YOUTUBE_API_KEY
    if not api_key:
        logger.error("YOUTUBE_API_KEY not set")
        return []

    query_pool: list[str] = []
    for niche in niches:
        query_pool.extend(YOUTUBE_QUERIES.get(niche.lower(), [f"{niche} youtube creator"]))

    state = _campaign_state.get(campaign_id, {})
    q_idx  = state.get("yt_query_index",  0)
    r_idx  = state.get("yt_region_index", 0)

    queries_this_cycle = [
        (query_pool[(q_idx + i) % len(query_pool)], REGIONS[(r_idx + i) % len(REGIONS)])
        for i in range(3)
    ]
    _campaign_state[campaign_id] = {
        **state,
        "yt_query_index":  (q_idx + 3) % len(query_pool),
        "yt_region_index": (r_idx + 1) % len(REGIONS),
    }

    creators: list[dict] = []

    for query, region in queries_this_cycle:
        try:
            logger.info(f"YouTube API [{region}]: {query}")
            search_resp = requests.get(
                "https://www.googleapis.com/youtube/v3/search",
                params={
                    "part": "snippet", "q": query, "type": "channel",
                    "maxResults": 20, "order": "relevance",
                    "regionCode": region, "relevanceLanguage": "en",
                    "key": api_key,
                },
                timeout=15,
            )
            if search_resp.status_code == 403:
                logger.error("YouTube API quota exceeded")
                break
            if search_resp.status_code != 200:
                logger.warning(f"YouTube search HTTP {search_resp.status_code}")
                continue

            channel_ids = [
                item["snippet"]["channelId"]
                for item in search_resp.json().get("items", [])
            ]
            if not channel_ids:
                continue

            detail_resp = requests.get(
                "https://www.googleapis.com/youtube/v3/channels",
                params={
                    "part": "snippet,statistics,brandingSettings",
                    "id": ",".join(channel_ids),
                    "key": api_key,
                },
                timeout=15,
            )
            if detail_resp.status_code != 200:
                continue

            for ch in detail_resp.json().get("items", []):
                parsed = _parse_youtube_channel(ch, niches)
                if parsed:
                    creators.append(parsed)

            time.sleep(random.uniform(0.5, 1.5))

        except Exception as e:
            logger.error(f"YouTube query error: {e}")

    logger.info(f"YouTube done — {len(creators)} qualified creators")
    return creators


def _parse_youtube_channel(ch: dict, niches: list) -> dict | None:
    try:
        snippet  = ch.get("snippet", {})
        stats    = ch.get("statistics", {})
        branding = ch.get("brandingSettings", {}).get("channel", {})

        name        = snippet.get("title", "").strip()
        username    = snippet.get("customUrl", "").lstrip("@")
        channel_id  = ch.get("id", "")
        description = snippet.get("description", "")
        keywords    = branding.get("keywords", "")
        country     = snippet.get("country", "")
        subs        = int(stats.get("subscriberCount", 0))

        full_text_lower = f"{name} {description} {keywords}".lower()
        if any(ind in full_text_lower for ind in SHORTS_INDICATORS):
            return None

        email_match = re.search(
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,7}\b',
            f"{description} {keywords}",
        )
        if not email_match:
            return None

        email = email_match.group(0).lower().strip()
        if _is_junk_email(email):
            return None

        profile_url = (
            f"https://youtube.com/@{username}"
            if username else
            f"https://youtube.com/channel/{channel_id}"
        )

        logger.info(f"✓ YouTube lead: {name} | {email} | {subs:,} subs | {country}")
        return {
            "name":        name,
            "username":    username or channel_id,
            "email":       email,
            "followers":   subs,
            "bio":         description[:500],
            "profile_url": profile_url,
            "platform":    "youtube",
            "niche":       niches[0] if niches else "crypto",
            "country":     country,
        }
    except Exception as e:
        logger.warning(f"Channel parse error: {e}")
        return None


# ─── DUCKDUCKGO REGEX FALLBACK (Twitter / Instagram) ─────────────────────────

def _scrape_duckduckgo(platform: str, niches: list) -> list[dict]:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    creators: list[dict] = []

    queries = [
        f'"{niche} trading" "business email" site:{platform}.com'
        for niche in niches[:2]
    ]

    JUNK_HANDLES = {
        "search", "home", "explore", "notifications", "messages",
        "i", "intent", "share", platform, "login", "signup",
        "p", "tv", "reel", "accounts", "about", "legal",
    }

    handle_pattern = (
        r'twitter\.com/([A-Za-z0-9_]{4,15})(?:/|"|\s)'
        if platform == "twitter" else
        r'instagram\.com/([A-Za-z0-9_.]{4,30})(?:/|"|\s)'
    )

    for query in queries:
        try:
            resp = requests.get(
                "https://duckduckgo.com/html/",
                params={"q": query},
                headers=headers,
                timeout=15,
            )
            if resp.status_code != 200:
                continue

            emails  = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,7}\b', resp.text)
            handles = re.findall(handle_pattern, resp.text)
            handles = [h for h in set(handles) if h.lower() not in JUNK_HANDLES]

            for i, handle in enumerate(handles[:5]):
                email = emails[i] if i < len(emails) else None
                if not email or _is_junk_email(email):
                    continue
                profile_url = f"https://{platform}.com/{handle}"
                niche = niches[0] if niches else "crypto"
                creators.append({
                    "name":        handle,
                    "username":    handle,
                    "email":       email,
                    "followers":   0,
                    "bio":         f"{niche} creator on {platform.capitalize()}",
                    "profile_url": profile_url,
                    "platform":    platform,
                    "niche":       niche,
                    "country":     "",
                })
                logger.info(f"✓ {platform.capitalize()} lead: @{handle} | {email}")

            time.sleep(random.uniform(2, 3))
        except Exception as e:
            logger.warning(f"{platform} DuckDuckGo error: {e}")

    return creators


# ─── GOOGLE SHEETS INJECTION ─────────────────────────────────────────────────

def inject_to_sheet(leads: list[dict]):
    """Push leads to Google Sheet (non-blocking; call inside executor)."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        # Expects GOOGLE_APPLICATION_CREDENTIALS env var pointing to service-account JSON
        creds = Credentials.from_service_account_file(
            "service_account.json",
            scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"],
        )
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(settings.GOOGLE_SHEET_KEY).worksheet("Leads")

        existing = {
            v.strip().lower().split("?")[0]
            for v in ws.col_values(2)
            if v.strip()
        }

        rows = []
        for lead in leads:
            url_norm = lead.get("profile_url", "").strip().lower().split("?")[0]
            if url_norm and url_norm not in existing:
                rows.append([
                    lead.get("name", ""),
                    lead.get("profile_url", ""),
                    lead.get("followers", 0),
                    lead.get("niche", ""),
                    lead.get("email", ""),
                ])
                existing.add(url_norm)

        if rows:
            next_row = len(ws.col_values(2)) + 1
            ws.update(
                f"A{next_row}:E{next_row + len(rows) - 1}",
                rows,
                value_input_option="USER_ENTERED",
            )
            logger.info(f"Google Sheets: injected {len(rows)} rows")
    except Exception as e:
        logger.error(f"Google Sheets injection failed: {e}")


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def _parse_followers(text: str) -> int:
    match = re.search(r'([0-9.,]+)([kKmM]?)\s*(?:Followers?|subscribers?)', text, re.IGNORECASE)
    if not match:
        return 0
    num_str, suf = match.groups()
    try:
        val = float(num_str.replace(",", ""))
        if suf.lower() == "k": val *= 1_000
        if suf.lower() == "m": val *= 1_000_000
        return int(val)
    except ValueError:
        return 0


def _is_junk_email(email: str) -> bool:
    if len(email.split("@")[0]) < 3:
        return True
    return any(k in email.lower() for k in JUNK_EMAILS)


def _detect_platform(url: str) -> str:
    url_l = url.lower()
    if "twitter.com" in url_l or "x.com" in url_l:
        return "twitter"
    if "instagram.com" in url_l:
        return "instagram"
    if "youtube.com" in url_l:
        return "youtube"
    if "linkedin.com" in url_l:
        return "linkedin"
    return "other"
