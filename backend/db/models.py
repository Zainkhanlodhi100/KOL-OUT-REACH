import uuid
import secrets
from datetime import datetime
from sqlalchemy import (
    Column, String, Integer, BigInteger, Text, Boolean,
    DateTime, ForeignKey, UniqueConstraint, Index,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from backend.db.database import Base


class Campaign(Base):
    __tablename__ = "campaigns"

    id               = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id          = Column(UUID(as_uuid=True), nullable=False, index=True)
    name             = Column(String(255), nullable=False)
    status           = Column(String(50),  default="running")   # running | paused | completed
    target_platforms = Column(Text,        default="youtube")
    target_niches    = Column(Text,        default="crypto")
    total_limit      = Column(Integer,     default=500)
    leads_scraped    = Column(Integer,     default=0)
    emails_sent      = Column(Integer,     default=0)
    created_at       = Column(DateTime,    default=datetime.utcnow)
    completed_at     = Column(DateTime,    nullable=True)

    leads  = relationship("Lead",        back_populates="campaign", lazy="select")
    emails = relationship("Email",       back_populates="campaign", lazy="select")
    logs   = relationship("ActivityLog", back_populates="campaign", lazy="select")

    @property
    def target_platforms_list(self):
        return [p.strip() for p in (self.target_platforms or "").split(",") if p.strip()]

    @property
    def target_niches_list(self):
        return [n.strip() for n in (self.target_niches or "").split(",") if n.strip()]


class Lead(Base):
    __tablename__ = "leads"

    id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    campaign_id   = Column(UUID(as_uuid=True), ForeignKey("campaigns.id"), nullable=True)
    user_id       = Column(UUID(as_uuid=True), nullable=False, index=True)
    name          = Column(String(255), nullable=True)
    username      = Column(String(255), nullable=True)
    email         = Column(String(320), nullable=True, index=True)
    platform      = Column(String(50),  default="youtube")
    niche         = Column(String(100), default="crypto")
    followers     = Column(BigInteger,  default=0)
    bio           = Column(Text,        default="")
    profile_url   = Column(Text,        default="")
    country       = Column(String(10),  nullable=True)

    # status lifecycle: scraped -> queued -> claimed -> emailed | failed
    # "claimed" is a transient state held only for the duration of the send
    # attempt, closing the race window between "select" and "update".
    status        = Column(String(50),  default="scraped", index=True)
    contacted_at  = Column(DateTime,    nullable=True)
    created_at    = Column(DateTime,    default=datetime.utcnow)

    ai_icebreaker = Column(Text, nullable=True)

    # ── COMPLIANCE: CAN-SPAM / GDPR ─────────────────────────────────────────
    unsubscribed       = Column(Boolean, default=False, nullable=False, index=True)
    unsubscribed_at    = Column(DateTime, nullable=True)
    unsubscribe_token  = Column(String(64), unique=True, nullable=False,
                                 default=lambda: secrets.token_urlsafe(32))
    consent_basis      = Column(String(100), default="public_business_contact")
    # e.g. "public_business_contact" (email was published by the owner as a
    # business contact point) vs "opt_in_form" vs "prior_relationship".
    # Required for GDPR Art. 6 legitimate-interest documentation per lead.

    campaign = relationship("Campaign", back_populates="leads")
    emails   = relationship("Email",    back_populates="lead", lazy="select")


class Email(Base):
    __tablename__ = "emails"
    __table_args__ = (
        # ── IDEMPOTENCY GUARD ───────────────────────────────────────────────
        # A lead can receive at most ONE email of a given type, full stop.
        # This is enforced by Postgres itself, not application logic, so a
        # crashed/duplicated worker cannot double-send even under a race.
        UniqueConstraint("lead_id", "email_type", name="uq_lead_email_type"),
        Index("ix_emails_lead_type", "lead_id", "email_type"),
    )

    id             = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    campaign_id    = Column(UUID(as_uuid=True), ForeignKey("campaigns.id"), nullable=True)
    lead_id        = Column(UUID(as_uuid=True), ForeignKey("leads.id"),     nullable=False)
    user_id        = Column(UUID(as_uuid=True), nullable=False)

    # "outreach" | "followup" — the actual idempotency key alongside lead_id
    email_type     = Column(String(50), nullable=False, default="outreach")

    subject        = Column(String(500), nullable=False)
    body           = Column(Text,        nullable=False)
    status         = Column(String(50),  default="queued")  # queued | sent | failed
    sent_at        = Column(DateTime,    nullable=True)
    failure_reason = Column(Text,        nullable=True)
    created_at     = Column(DateTime,    default=datetime.utcnow)

    campaign = relationship("Campaign", back_populates="emails")
    lead     = relationship("Lead",     back_populates="emails")


class ActivityLog(Base):
    __tablename__ = "activity_logs"

    id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id     = Column(UUID(as_uuid=True), nullable=False, index=True)
    campaign_id = Column(UUID(as_uuid=True), ForeignKey("campaigns.id"), nullable=True)
    level       = Column(String(20), default="info")   # info | success | warning | error
    message     = Column(Text,       nullable=False)
    created_at  = Column(DateTime,   default=datetime.utcnow)

    campaign = relationship("Campaign", back_populates="logs")
