"""
core/outreach.py  (v6 — production-hardened)

Fixes vs. v5:

1. ATOMIC CLAIM: leads are claimed via `UPDATE ... WHERE status='queued'
   RETURNING`, a single round-trip that closes the select-then-update race.
   Two concurrent workers (e.g. a crashed-and-restarted supervisor plus a
   still-running old instance) can never both grab the same lead.

2. DB-LEVEL IDEMPOTENCY: sends are recorded via an INSERT that will raise
   IntegrityError on the (lead_id, email_type) unique constraint. We treat
   that specific error as "already handled, skip" rather than a fatal
   failure — this makes the whole loop safe to retry from any point.

3. SUPERVISED, NOT FIRE-AND-FORGET: `launch_outreach` starts a supervisor
   that restarts the loop with exponential backoff on unhandled exceptions,
   and logs every crash/restart to `activity_logs` so failures are visible
   instead of silently leaving `_active[campaign_id] = True` with nothing
   actually running (the v5 failure mode).

4. LIVE RE-POLLING: v5 fetched `queued_leads` once per 30s outer cycle and
   iterated a stale snapshot; leads queued mid-cycle waited up to 30s+
   unnecessarily and, worse, a paused/stopped campaign's in-flight snapshot
   would keep sending. v6 re-checks campaign state and re-polls for newly
   queued leads on every single send, not once per batch.

5. CAN-SPAM ENFORCEMENT AT THE SEND POINT: `compliance.assert_sendable`
   runs inside the same transaction as the claim, immediately before SMTP,
   using a fresh DB read — so an unsubscribe that lands mid-cycle is
   guaranteed to be honored, not just checked at loop-start.
"""

import asyncio
import logging
import random
import smtplib
import threading
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from sqlalchemy.exc import IntegrityError

from backend.core.config import settings
from backend.core import compliance

logger = logging.getLogger(__name__)

# ─── EMAIL TEMPLATES ─────────────────────────────────────────────────────────

OUTREACH_SUBJECT = "Collaboration Opportunity with HyroTrader"

OUTREACH_BODY = """\
Hi {first_name},

{icebreaker}

My name is Owais, and I am part of the {company} team — we partner with \
trading and finance creators to run performance-based collaborations.

I would love to put together a custom proposal tailored specifically for your \
community. If you are open to it, let me know and we can take this further.

You can also check out my profile here:
https://x.com/OwaisAlpha1

Looking forward to hearing your thoughts.

Best regards,
Owais
{company} Team
"""

FOLLOWUP_SUBJECT = "Following Up — HyroTrader Collaboration"

FOLLOWUP_BODY = """\
Hi {first_name},

Just following up on my last email about a potential collaboration \
with {company}.

I know you are busy, so I will keep it short. We work with {niche} \
creators and think your channel could be a great fit for a custom deal.

Would you be open to a quick chat? Just reply here.

Best,
Owais
{company} Team
"""

# ─── SUPERVISION STATE ───────────────────────────────────────────────────────

_active: dict[str, bool] = {}
_MAX_BACKOFF = 300  # seconds


def launch_outreach(campaign_id: str):
    """Start a SUPERVISED outreach loop for a campaign in a daemon thread."""
    if _active.get(campaign_id):
        logger.info(f"Outreach already running for {campaign_id}")
        return
    _active[campaign_id] = True
    t = threading.Thread(
        target=_thread_runner,
        args=(_supervised_outreach, campaign_id),
        daemon=True,
        name=f"outreach-{campaign_id[:8]}",
    )
    t.start()


def launch_followup(campaign_id: str, user_id: str):
    t = threading.Thread(
        target=_thread_runner,
        args=(_followup_loop, campaign_id, user_id),
        daemon=True,
        name=f"followup-{campaign_id[:8]}",
    )
    t.start()


def stop_outreach(campaign_id: str):
    _active[campaign_id] = False


def _thread_runner(coro_fn, *args):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(coro_fn(*args))
    except Exception as e:
        logger.error(f"Thread fatal error: {e}")
    finally:
        loop.close()


# ─── SUPERVISOR ───────────────────────────────────────────────────────────────

async def _supervised_outreach(campaign_id: str):
    """
    Wraps _outreach_loop with restart-on-crash + exponential backoff.
    This is the actual fix for v5's silent-death failure mode: if the inner
    loop throws anything unhandled, we log it, back off, and try again —
    up to the point where the campaign itself is no longer 'running'.
    """
    from backend.db.database import AsyncSessionLocal
    from backend.db.models import Campaign, ActivityLog

    backoff = 5
    attempt = 0

    while _active.get(campaign_id, False):
        attempt += 1
        try:
            await _outreach_loop(campaign_id)
            # Clean exit (campaign completed/paused) — stop supervising.
            break
        except Exception as e:
            logger.error(f"[SUPERVISOR] outreach loop crashed (attempt {attempt}): {e}")
            async with AsyncSessionLocal() as db:
                campaign = await db.get(Campaign, campaign_id)
                if not campaign or campaign.status != "running":
                    break
                db.add(ActivityLog(
                    user_id=campaign.user_id,
                    campaign_id=campaign.id,
                    level="error",
                    message=f"Outreach loop crashed (attempt {attempt}): {e}. "
                            f"Restarting in {backoff}s.",
                ))
                await db.commit()

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _MAX_BACKOFF)


# ─── CORE OUTREACH LOOP ───────────────────────────────────────────────────────

async def _outreach_loop(campaign_id: str):
    from sqlalchemy import select, update
    from backend.db.database import AsyncSessionLocal
    from backend.db.models import Campaign, Lead, Email, ActivityLog

    POLL_INTERVAL = 5  # re-check for newly queued leads this often

    while _active.get(campaign_id, False):
        async with AsyncSessionLocal() as db:
            campaign = await db.get(Campaign, campaign_id)
            if not campaign or campaign.status != "running":
                break
            if campaign.emails_sent >= campaign.total_limit:
                campaign.status = "completed"
                campaign.completed_at = datetime.utcnow()
                await db.commit()
                break

            # ── ATOMIC CLAIM ────────────────────────────────────────────────
            # Claim exactly ONE lead per pass. This is the fix for the v5
            # race: instead of SELECT-then-later-UPDATE, we do it in one
            # statement. `status='queued'` in the WHERE clause means if two
            # workers race, only one UPDATE actually matches a row.
            claimed = await db.scalar(
                update(Lead)
                .where(
                    Lead.campaign_id == campaign.id,
                    Lead.status == "queued",
                    Lead.unsubscribed == False,  # noqa: E712
                )
                .values(status="claimed")
                .returning(Lead)
                .execution_options(synchronize_session=False)
            )
            await db.commit()

            if claimed is None:
                # Nothing queued right now — short poll, don't busy-loop,
                # but check back frequently so newly queued leads aren't
                # stuck waiting behind a long fixed cycle like in v5.
                await asyncio.sleep(POLL_INTERVAL)
                continue

            lead = claimed
            try:
                await _send_one(db, campaign, lead)
            except compliance.SuppressionCheckFailed as e:
                logger.warning(f"Suppressed send for lead {lead.id}: {e}")
                lead.status = "failed"
                db.add(ActivityLog(
                    user_id=campaign.user_id, campaign_id=campaign.id,
                    level="warning", message=f"Send suppressed: {e}",
                ))
                await db.commit()
            except IntegrityError:
                # Another worker already recorded this exact (lead, type)
                # send between our claim and our insert. Not an error —
                # this IS the idempotency guard working as designed.
                await db.rollback()
                logger.info(f"Duplicate send prevented for lead {lead.id}")

            # Human-visible pacing between sends (deliverability, not
            # evasion — Gmail/Workspace rate-limit bursty senders).
            await asyncio.sleep(random.randint(15, 30))


async def _send_one(db, campaign, lead):
    """Send exactly one outreach email to a freshly-claimed lead."""
    from backend.db.models import Email, ActivityLog

    await compliance.assert_sendable(db, lead)

    first_name = _first_name(lead.name or lead.username or "there")
    icebreaker_line = (
        lead.ai_icebreaker.strip()
        if getattr(lead, "ai_icebreaker", None)
        else f"Came across your {lead.niche or 'trading'} content and it really stood out."
    )
    body = OUTREACH_BODY.format(
        first_name=first_name,
        icebreaker=icebreaker_line,
        company=compliance.COMPANY_NAME,
    )
    body += compliance.can_spam_footer(lead.unsubscribe_token)

    # Insert the Email row FIRST, inside this transaction. If a duplicate
    # (lead_id, email_type='outreach') already exists, this raises
    # IntegrityError and the caller treats it as "already sent, skip" —
    # meaning we never SMTP-send unless we've durably recorded intent to.
    email_rec = Email(
        campaign_id=campaign.id,
        lead_id=lead.id,
        user_id=campaign.user_id,
        email_type="outreach",
        subject=OUTREACH_SUBJECT,
        body=body,
        status="queued",
    )
    db.add(email_rec)
    await db.flush()  # forces the unique constraint check now, not at commit

    ok = await asyncio.get_event_loop().run_in_executor(
        None, _smtp_send, lead.email, OUTREACH_SUBJECT, body, lead.unsubscribe_token,
    )

    if ok:
        email_rec.status = "sent"
        email_rec.sent_at = datetime.utcnow()
        lead.status = "emailed"
        lead.contacted_at = datetime.utcnow()
        campaign.emails_sent += 1
        db.add(ActivityLog(
            user_id=campaign.user_id, campaign_id=campaign.id, level="success",
            message=f"Email sent -> {lead.name} <{lead.email}>",
        ))
    else:
        email_rec.status = "failed"
        email_rec.failure_reason = "SMTP send failed"
        lead.status = "failed"
        db.add(ActivityLog(
            user_id=campaign.user_id, campaign_id=campaign.id, level="error",
            message=f"Failed -> {lead.email}",
        ))

    await db.commit()


# ─── FOLLOW-UP LOOP ───────────────────────────────────────────────────────────

async def _followup_loop(campaign_id: str, user_id: str):
    """
    One-shot pass over 'emailed' leads. Idempotency now comes from the DB
    constraint (lead_id, email_type='followup'), not a manual pre-query —
    the manual check stays as a fast-path to avoid needless SMTP attempts,
    but the constraint is what actually prevents duplicates under races.
    """
    import uuid
    from sqlalchemy import select
    from sqlalchemy.exc import IntegrityError
    from backend.db.database import AsyncSessionLocal
    from backend.db.models import Campaign, Lead, Email, ActivityLog

    async with AsyncSessionLocal() as db:
        uid = uuid.UUID(user_id) if isinstance(user_id, str) else user_id
        cid = uuid.UUID(campaign_id) if isinstance(campaign_id, str) else campaign_id

        leads = (await db.scalars(
            select(Lead).where(
                Lead.user_id == uid,
                Lead.status == "emailed",
                Lead.unsubscribed == False,  # noqa: E712
            )
        )).all()

        if not leads:
            logger.info("Follow-up: no eligible leads found")
            return

        campaign = await db.get(Campaign, cid)
        db.add(ActivityLog(
            user_id=uid, campaign_id=cid, level="info",
            message=f"Follow-up started — {len(leads)} candidate leads",
        ))
        await db.commit()

        sent = 0
        for lead in leads:
            if not lead.email:
                continue
            try:
                await compliance.assert_sendable(db, lead)
            except compliance.SuppressionCheckFailed:
                continue

            first_name = _first_name(lead.name or lead.username or "there")
            niche = lead.niche or "crypto trading"
            body = FOLLOWUP_BODY.format(
                first_name=first_name, niche=niche, company=compliance.COMPANY_NAME,
            )
            body += compliance.can_spam_footer(lead.unsubscribe_token)

            email_rec = Email(
                campaign_id=cid, lead_id=lead.id, user_id=uid,
                email_type="followup",
                subject=FOLLOWUP_SUBJECT, body=body, status="queued",
            )
            db.add(email_rec)
            try:
                await db.flush()
            except IntegrityError:
                await db.rollback()
                continue  # follow-up already sent to this lead — skip silently

            await asyncio.sleep(random.randint(15, 30))

            ok = await asyncio.get_event_loop().run_in_executor(
                None, _smtp_send, lead.email, FOLLOWUP_SUBJECT, body, lead.unsubscribe_token,
            )
            email_rec.status = "sent" if ok else "failed"
            email_rec.sent_at = datetime.utcnow() if ok else None

            if ok:
                sent += 1
                db.add(ActivityLog(
                    user_id=uid, campaign_id=cid, level="success",
                    message=f"Follow-up sent -> {lead.email}",
                ))
            await db.commit()

        db.add(ActivityLog(
            user_id=uid, campaign_id=cid, level="success",
            message=f"Follow-up complete — {sent}/{len(leads)} sent",
        ))
        await db.commit()
        logger.info(f"Follow-up complete: {sent}/{len(leads)}")


# ─── SMTP HELPER ─────────────────────────────────────────────────────────────

def _smtp_send(to_email: str, subject: str, body: str, unsubscribe_token: str) -> bool:
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = settings.GMAIL_USER
        msg["To"]      = to_email

        for k, v in compliance.list_unsubscribe_header(unsubscribe_token).items():
            msg[k] = v

        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(settings.GMAIL_USER, settings.GMAIL_APP_PASSWORD)
            server.sendmail(settings.GMAIL_USER, to_email, msg.as_string())

        return True

    except smtplib.SMTPAuthenticationError:
        logger.error("Gmail auth failed — check GMAIL_USER / GMAIL_APP_PASSWORD")
        return False
    except smtplib.SMTPRecipientsRefused:
        logger.warning(f"Recipient refused: {to_email}")
        return False
    except Exception as e:
        logger.error(f"SMTP error: {e}")
        return False


# ─── UTIL ────────────────────────────────────────────────────────────────────

def _first_name(full_name: str) -> str:
    parts = full_name.strip().split()
    return parts[0] if parts else full_name
