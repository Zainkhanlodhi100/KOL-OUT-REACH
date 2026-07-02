"""
main.py — KOL Engine V5.0  FastAPI Gateway  [AGENTIC UPGRADE]
Run: uvicorn main:app --reload --port 8000
"""

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import httpx
import uvicorn
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from backend.core.routes_addendum import router as compliance_router
from pydantic import BaseModel
from sqlalchemy import select

from backend.core.agent import generate_icebreaker          # ← AGENTIC LAYER
from backend.core.config import settings
from backend.core.outreach import (
    FOLLOWUP_BODY,
    FOLLOWUP_SUBJECT,
    _first_name,
    _smtp_send,
    launch_outreach,
    stop_outreach,
)
from backend.core.scraper import inject_to_sheet, scrape_all
from backend.db.database import AsyncSessionLocal, init_db
from backend.db.models import ActivityLog, Campaign, Email, Lead

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ─── LIFESPAN ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    logger.info("Database initialised")

    try:
        from pyngrok import ngrok
        ngrok.set_auth_token(settings.NGROK_TOKEN)
        tunnel = ngrok.connect(8000)
        logger.info("=" * 60)
        logger.info(f"  NGROK LIVE → {tunnel.public_url}/start-engine")
        logger.info("=" * 60)
        app.state.ngrok_url = tunnel.public_url
    except Exception as e:
        logger.warning(f"Ngrok failed (running locally only): {e}")
        app.state.ngrok_url = "http://localhost:8000"

    yield


# ─── APP ──────────────────────────────────────────────────────────────────────

app = FastAPI(title="KOL Engine V5.0 — Agentic", lifespan=lifespan)
app.include_router(compliance_router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── SCHEMAS ──────────────────────────────────────────────────────────────────

class EngineRequest(BaseModel):
    platform: str
    keyword:  str
    niche:    str = "crypto"
    limit:    int = 200
    user_id:  str = "00000000-0000-0000-0000-000000000001"


class StopRequest(BaseModel):
    campaign_id: str


# ─── MAKE.COM WEBHOOK ─────────────────────────────────────────────────────────

async def _fire_make_webhook(lead: Lead, icebreaker: str) -> None:
    """
    POST lead data to the Make.com Custom Webhook.
    Non-blocking — called with asyncio.create_task() so it never delays
    the main scrape pipeline.
    """
    url = getattr(settings, "MAKE_WEBHOOK_URL", "")
    if not url:
        logger.debug("[MAKE] MAKE_WEBHOOK_URL not set — skipping webhook.")
        return

    payload = {
        "name":          lead.name or "",
        "username":      lead.username or "",
        "email":         lead.email or "",
        "platform":      lead.platform or "",
        "niche":         lead.niche or "",
        "followers":     lead.followers or 0,
        "profile_url":   lead.profile_url or "",
        "country":       lead.country or "",
        "ai_icebreaker": icebreaker,
        "lead_id":       str(lead.id),
        "campaign_id":   str(lead.campaign_id),
        "status":        lead.status,
    }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload)
            logger.info(f"[MAKE] Webhook fired for {lead.email} → HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"[MAKE] Webhook failed (non-critical): {e}")


# ─── BACKGROUND ENGINE TASK ───────────────────────────────────────────────────

async def _run_engine(campaign_id: str, platform: str, niche: str):
    """
    Pipeline:
      1. Scrape leads via Apify / YouTube / DuckDuckGo
      2. Deduplicate against DB
      3. Generate AI icebreaker per lead  ← AGENTIC STEP
      4. Persist leads (status='scraped')
      5. Fire Make.com webhook per lead   ← EXTERNAL TOOL INTEGRATION
      6. Inject to Google Sheets
      7. Start outreach loop (emails only 'queued' leads)
    """
    async with AsyncSessionLocal() as db:
        campaign = await db.get(Campaign, uuid.UUID(campaign_id))
        if not campaign:
            return

        await _log(db, campaign, "info", f"Engine started — platform={platform} niche={niche}")

        platforms = [platform] if platform != "all" else ["youtube", "apify", "twitter", "instagram"]

        # ── 1. SCRAPE ──────────────────────────────────────────────────────────
        creators = await asyncio.get_event_loop().run_in_executor(
            None, scrape_all, platforms, [niche], campaign_id
        )
        await _log(db, campaign, "info", f"Scraped {len(creators)} raw profiles")

        # ── 2. DEDUPLICATE + 3. AI ICEBREAKER + 4. PERSIST ───────────────────
        new_leads     = []
        webhook_tasks = []

        for data in creators:
            if not data.get("email") and not data.get("profile_url"):
                continue

            existing = await db.scalar(
                select(Lead).where(
                    Lead.user_id    == campaign.user_id,
                    Lead.profile_url == data.get("profile_url", ""),
                )
            )
            if existing:
                continue

            # ── AGENTIC STEP: generate personalised icebreaker ────────────────
            bio       = data.get("bio", "") or ""
            icebreaker = await asyncio.get_event_loop().run_in_executor(
                None,
                generate_icebreaker,
                bio,
                data.get("niche", niche),
                data.get("name", ""),
            )
            logger.info(f"[AGENT] Icebreaker ready for {data.get('name', 'unknown')}")

            lead = Lead(
                campaign_id   = campaign.id,
                user_id       = campaign.user_id,
                name          = data.get("name"),
                username      = data.get("username"),
                email         = data.get("email"),
                platform      = data.get("platform", platform),
                niche         = data.get("niche", niche),
                followers     = data.get("followers", 0),
                bio           = bio[:500],
                profile_url   = data.get("profile_url", ""),
                country       = data.get("country", ""),
                status        = "scraped",
                ai_icebreaker = icebreaker,   # ← stored in DB
            )
            db.add(lead)
            new_leads.append((lead, icebreaker))

        campaign.leads_scraped += len(new_leads)
        await db.commit()
        await _log(db, campaign, "success", f"{len(new_leads)} new leads saved with AI icebreakers")

        # ── 5. MAKE.COM WEBHOOKS (fire-and-forget per lead) ───────────────────
        for lead, icebreaker in new_leads:
            asyncio.create_task(_fire_make_webhook(lead, icebreaker))

        # ── 6. GOOGLE SHEETS ──────────────────────────────────────────────────
        if new_leads:
            sheet_data = [
                {
                    "name":          l.name,
                    "profile_url":   l.profile_url,
                    "followers":     l.followers,
                    "niche":         l.niche,
                    "email":         l.email,
                    "ai_icebreaker": ice,
                }
                for l, ice in new_leads
            ]
            asyncio.get_event_loop().run_in_executor(None, inject_to_sheet, sheet_data)

        # ── 7. START OUTREACH LOOP ────────────────────────────────────────────
        launch_outreach(campaign_id)
        await _log(db, campaign, "info", "Outreach loop started — awaiting human review to queue leads")


async def _log(db, campaign, level: str, message: str):
    try:
        db.add(ActivityLog(
            user_id=campaign.user_id,
            campaign_id=campaign.id,
            level=level,
            message=message,
        ))
        await db.commit()
        logger.info(f"[{level.upper()}] {message}")
    except Exception as e:
        logger.error(f"Activity log error: {e}")


async def _batch_followup(lead_ids: list[str]):
    async with AsyncSessionLocal() as db:
        for lid in lead_ids:
            lead = await db.get(Lead, uuid.UUID(lid))
            if not lead or not lead.email:
                continue

            first_name = _first_name(lead.name or lead.username or "there")
            niche      = lead.niche or "crypto trading"
            body       = FOLLOWUP_BODY.format(first_name=first_name, niche=niche)

            await asyncio.sleep(__import__("random").randint(15, 30))

            ok = await asyncio.get_event_loop().run_in_executor(
                None, _smtp_send, lead.email, FOLLOWUP_SUBJECT, body
            )

            db.add(Email(
                lead_id=lead.id,
                user_id=lead.user_id,
                subject=FOLLOWUP_SUBJECT,
                body=body,
                status="sent" if ok else "failed",
                sent_at=datetime.utcnow() if ok else None,
            ))
            if ok:
                db.add(ActivityLog(
                    user_id=lead.user_id,
                    level="success",
                    message=f"Follow-up sent → {lead.email}",
                ))
            await db.commit()


# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "online", "ngrok_url": getattr(app.state, "ngrok_url", "unknown")}


@app.post("/start-engine")
async def start_engine(req: EngineRequest, background_tasks: BackgroundTasks):
    async with AsyncSessionLocal() as db:
        campaign = Campaign(
            user_id          = uuid.UUID(req.user_id),
            name             = f"{req.platform.capitalize()} / {req.niche} — {req.keyword[:40]}",
            status           = "running",
            target_platforms = req.platform,
            target_niches    = req.niche,
            total_limit      = req.limit,
        )
        db.add(campaign)
        await db.commit()
        campaign_id = str(campaign.id)

    background_tasks.add_task(_run_engine, campaign_id, req.platform, req.niche)

    return {
        "status":          "started",
        "campaign_id":     campaign_id,
        "leads_extracted": 0,
        "message":         "Agentic engine running — AI icebreakers generating per lead",
    }


@app.get("/campaign/{campaign_id}")
async def get_campaign(campaign_id: str):
    async with AsyncSessionLocal() as db:
        campaign = await db.get(Campaign, uuid.UUID(campaign_id))
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        return {
            "id":            str(campaign.id),
            "name":          campaign.name,
            "status":        campaign.status,
            "leads_scraped": campaign.leads_scraped,
            "emails_sent":   campaign.emails_sent,
            "total_limit":   campaign.total_limit,
            "created_at":    campaign.created_at.isoformat(),
        }


@app.get("/get-leads/{campaign_id}")
async def get_leads(campaign_id: str):
    """Return all leads for a campaign — includes ai_icebreaker field."""
    async with AsyncSessionLocal() as db:
        rows = (await db.scalars(
            select(Lead).where(Lead.campaign_id == uuid.UUID(campaign_id))
        )).all()
        return [
            {
                "id":            str(r.id),
                "name":          r.name,
                "username":      r.username,
                "email":         r.email,
                "platform":      r.platform,
                "niche":         r.niche,
                "followers":     r.followers,
                "profile_url":   r.profile_url,
                "country":       r.country,
                "status":        r.status,
                "ai_icebreaker": r.ai_icebreaker,    # ← exposed to frontend
                "contacted_at":  r.contacted_at.isoformat() if r.contacted_at else None,
                "created_at":    r.created_at.isoformat()   if r.created_at   else None,
            }
            for r in rows
        ]


@app.post("/queue-lead/{lead_id}")
async def queue_lead(lead_id: str):
    """Promote a single lead from 'scraped' → 'queued'."""
    async with AsyncSessionLocal() as db:
        lead = await db.get(Lead, uuid.UUID(lead_id))
        if not lead:
            raise HTTPException(status_code=404, detail="Lead not found")
        if lead.status != "scraped":
            return {"status": "skipped", "reason": f"Lead is already '{lead.status}'"}
        lead.status = "queued"
        await db.commit()
        return {"status": "queued", "lead_id": lead_id}


@app.post("/stop-engine")
async def stop_engine(req: StopRequest):
    stop_outreach(req.campaign_id)
    async with AsyncSessionLocal() as db:
        campaign = await db.get(Campaign, uuid.UUID(req.campaign_id))
        if campaign:
            campaign.status = "paused"
            await db.commit()
    return {"status": "stopped", "campaign_id": req.campaign_id}


@app.post("/trigger-followups")
async def trigger_followups(background_tasks: BackgroundTasks):
    """Find leads emailed > 48 h ago and queue follow-up emails."""
    cutoff = datetime.utcnow() - timedelta(hours=48)

    async with AsyncSessionLocal() as db:
        emailed_leads = (await db.scalars(
            select(Lead).where(Lead.status == "emailed")
        )).all()

        eligible = []
        for lead in emailed_leads:
            last_email = await db.scalar(
                select(Email)
                .where(Email.lead_id == lead.id, Email.status == "sent")
                .order_by(Email.sent_at.desc())
            )
            if last_email and last_email.sent_at and last_email.sent_at < cutoff:
                eligible.append(str(lead.id))

    if not eligible:
        return {"status": "no_eligible_leads", "count": 0}

    background_tasks.add_task(_batch_followup, eligible)
    return {"status": "followups_queued", "count": len(eligible), "lead_ids": eligible}


# ─── ENTRYPOINT ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
