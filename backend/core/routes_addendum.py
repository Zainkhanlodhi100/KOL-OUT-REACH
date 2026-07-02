"""
Additions to wire into your existing main.py.

Two things land here:
  1. GET /unsubscribe/{token} — the actual CAN-SPAM enforcement endpoint.
     This MUST be a GET (not POST) so it works as a plain clicked link from
     any email client, and MUST NOT require login — the whole point of
     CAN-SPAM's opt-out mandate is that it works with zero friction.
  2. Updated /start-engine ingestion branch for platform == "youtube",
     replacing the Apify/X path with the compliant scraper.
"""

from fastapi import APIRouter, HTTPException
from sqlalchemy import select
from datetime import datetime

from backend.db.database import AsyncSessionLocal
from backend.db.models import Lead, ActivityLog
from backend.core.scraper_youtube import scrape_youtube_leads

router = APIRouter()


# ─── 1. UNSUBSCRIBE ENDPOINT ──────────────────────────────────────────────────

@router.get("/unsubscribe/{token}")
async def unsubscribe(token: str):
    """
    One-click, no-auth unsubscribe. Marks the lead permanently suppressed.
    Idempotent — clicking twice is harmless.
    """
    async with AsyncSessionLocal() as db:
        lead = await db.scalar(select(Lead).where(Lead.unsubscribe_token == token))
        if lead is None:
            # Do not leak whether a token is valid/invalid in detail — just
            # a generic confirmation either way, which is also friendlier
            # to a recipient who might have already unsubscribed before.
            return {"message": "You have been unsubscribed."}

        if not lead.unsubscribed:
            lead.unsubscribed = True
            lead.unsubscribed_at = datetime.utcnow()
            db.add(ActivityLog(
                user_id=lead.user_id,
                campaign_id=lead.campaign_id,
                level="info",
                message=f"Lead {lead.email} unsubscribed",
            ))
            await db.commit()

        return {"message": "You have been unsubscribed and will not receive further emails."}


# ─── 2. INGESTION: /start-engine youtube branch ──────────────────────────────
#
# Drop this into your existing /start-engine handler, replacing whatever
# Apify/X-scraping branch currently runs when platform == "youtube".

async def ingest_youtube(campaign, niche: str, db) -> int:
    """
    Runs the compliant YouTube ingestion and inserts new Lead rows.
    Returns the count of leads actually inserted (dedup'd against existing
    emails for this user, since re-running a campaign shouldn't create
    duplicate leads for the same address).
    """
    scraped = await scrape_youtube_leads(niche, max_results=50)

    if not scraped:
        db.add(ActivityLog(
            user_id=campaign.user_id, campaign_id=campaign.id, level="warning",
            message=f"YouTube ingestion returned 0 leads with published "
                    f"business emails for niche='{niche}'. Try a broader "
                    f"or more commercial niche keyword — many smaller "
                    f"channels don't list a business email at all.",
        ))
        await db.commit()
        return 0

    existing_emails = set(
        (await db.scalars(
            select(Lead.email).where(Lead.user_id == campaign.user_id)
        )).all()
    )

    inserted = 0
    for lead_data in scraped:
        if lead_data["email"] in existing_emails:
            continue
        db.add(Lead(
            campaign_id=campaign.id,
            user_id=campaign.user_id,
            **lead_data,
        ))
        existing_emails.add(lead_data["email"])
        inserted += 1

    campaign.leads_scraped += inserted
    db.add(ActivityLog(
        user_id=campaign.user_id, campaign_id=campaign.id, level="success",
        message=f"YouTube ingestion: {inserted} new leads saved "
                f"(status=scraped) for niche='{niche}'",
    ))
    await db.commit()
    return inserted
