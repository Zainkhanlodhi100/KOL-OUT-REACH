"""
core/compliance.py

CAN-SPAM + GDPR compliance layer. This is not decorative — every function
here is load-bearing for legal exposure, not just deliverability.

CAN-SPAM (15 U.S.C. 7701 et seq.) requires, for every commercial email:
  1. Accurate From/Subject (no deception about content or sender)
  2. Identification as an ad is NOT required for B2B outreach of this kind,
     but a physical postal address IS required, always.
  3. A clear, working opt-out mechanism, honored within 10 business days
     (we honor it immediately since it's automated).
  4. No selling/transferring the recipient's address after opt-out.

GDPR (if any lead is in the EU/UK/EEA) additionally requires a documented
lawful basis (Art. 6) for processing the person's email address. We do NOT
attempt to classify residency automatically here — that requires either a
declared country field or IP-based inference at signup, neither of which
applies to scraped public data. Instead: (a) we only use publicly-published
*business* contact emails (a legitimate-interest-friendly basis), (b) we
tag `consent_basis` per lead for audit, (c) unsubscribe is instant and
permanent, satisfying the Right to Object (Art. 21) regardless of basis.

CONFIGURE THESE before going live — placeholders will fail CAN-SPAM audits:
"""

import os

# ── REQUIRED: physical postal address (CAN-SPAM mandate, no exceptions) ─────
COMPANY_NAME    = os.getenv("COMPANY_NAME", "HyroTrader")
COMPANY_ADDRESS = os.getenv(
    "COMPANY_ADDRESS",
    "REPLACE_ME: Street Address, City, State/Province, ZIP, Country",
)

# Base URL where your FastAPI app is publicly reachable (used to build the
# one-click unsubscribe link). e.g. https://api.yourapp.com
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://localhost:8000")


def unsubscribe_url(token: str) -> str:
    return f"{PUBLIC_BASE_URL.rstrip('/')}/unsubscribe/{token}"


def can_spam_footer(token: str) -> str:
    """
    Appended to EVERY outbound email body. Do not send anything without
    this footer attached.
    """
    return (
        f"\n\n---\n"
        f"{COMPANY_NAME}\n"
        f"{COMPANY_ADDRESS}\n\n"
        f"You are receiving this because your business contact email is "
        f"publicly listed for partnership inquiries. "
        f"If you'd rather not hear from us again, unsubscribe instantly here: "
        f"{unsubscribe_url(token)}\n"
    )


def list_unsubscribe_header(token: str) -> dict:
    """
    RFC 8058 one-click unsubscribe header. Gmail/Outlook surface this as a
    native 'Unsubscribe' button next to the sender name, which meaningfully
    reduces spam-complaint rates (complaints, not just deliverability, are
    what get sending domains blacklisted).
    """
    url = unsubscribe_url(token)
    return {
        "List-Unsubscribe": f"<{url}>",
        "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
    }


class SuppressionCheckFailed(Exception):
    """Raised when attempting to email a lead that must not be contacted."""


async def assert_sendable(db, lead) -> None:
    """
    Call this immediately before every SMTP send, inside the same
    transaction that claims the lead. This is the actual enforcement point
    for unsubscribe/suppression — everything else is best-effort until this
    check runs against fresh data.
    """
    # Re-fetch fresh row rather than trusting an in-memory object that may
    # be stale if the lead unsubscribed between claim and send.
    from backend.db.models import Lead
    fresh = await db.get(Lead, lead.id)
    if fresh is None:
        raise SuppressionCheckFailed(f"Lead {lead.id} no longer exists")
    if fresh.unsubscribed:
        raise SuppressionCheckFailed(f"Lead {lead.id} has unsubscribed")
    if not fresh.email:
        raise SuppressionCheckFailed(f"Lead {lead.id} has no email address")
