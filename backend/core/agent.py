"""
backend/core/agent.py

Agentic Layer — AI Icebreaker Generator (Gemini Edition)
----------------------------------------------------------
Uses LangChain + Google Gemini (gemini-1.5-flash) to generate a
personalised 2-line email icebreaker per lead.

Free tier: 15 RPM / 1,500 RPD — more than enough for scrape batches.
Get key:   https://aistudio.google.com/app/apikey  (free, no card needed)

Install:   pip install langchain langchain-google-genai
"""

import logging
import os

logger = logging.getLogger(__name__)

try:
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain.schema import HumanMessage, SystemMessage
    _GENAI_AVAILABLE = True
except ImportError:
    _GENAI_AVAILABLE = False
    logger.warning("[AGENT] langchain-google-genai not installed — using fallback icebreaker.")


_SYSTEM_PROMPT = """\
You are an elite B2B Sales Development Representative (SDR) for HyroTrader, \
a prop-trading firm that partners with content creators.

Your task: write a hyper-personalised 2-line email icebreaker that:
1. References something SPECIFIC from the creator's bio or content niche.
2. Bridges naturally toward a partnership conversation.

Hard rules:
- Exactly 2 sentences. No greeting. No sign-off.
- Sound warm and human, never corporate.
- Under 40 words total.
- Output ONLY the icebreaker — nothing else.
"""


def generate_icebreaker(bio: str, niche: str, name: str = "") -> str:
    """
    Generate a personalised 2-line icebreaker using Gemini 1.5 Flash.

    Gracefully falls back to a template if:
      - langchain-google-genai is not installed
      - GEMINI_API_KEY is missing
      - The API call fails for any reason

    Parameters
    ----------
    bio   : Scraped bio / snippet text from the creator's profile.
    niche : Content niche (e.g. "crypto", "forex").
    name  : Creator display name (used in fallback only).

    Returns
    -------
    str : 2-line personalised icebreaker (or clean fallback).
    """
    if not bio or len(bio.strip()) < 20:
        return _fallback(name, niche)

    if not _GENAI_AVAILABLE:
        return _fallback(name, niche)

    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        logger.warning("[AGENT] GEMINI_API_KEY not set — using fallback icebreaker.")
        return _fallback(name, niche)

    try:
        llm = ChatGoogleGenerativeAI(
            model="gemini-1.5-flash",   # free tier, extremely fast
            google_api_key=api_key,
            temperature=0.8,            # slight variance per lead
            max_output_tokens=80,       # 2 sentences is all we need
        )

        user_prompt = (
            f"Creator bio: {bio.strip()[:600]}\n"
            f"Content niche: {niche}\n\n"
            "Write the 2-line personalised icebreaker now:"
        )

        response = llm.invoke([
            SystemMessage(content=_SYSTEM_PROMPT),
            HumanMessage(content=user_prompt),
        ])

        icebreaker = response.content.strip().replace('"', '')
        logger.info(f"[AGENT] Icebreaker generated ({len(icebreaker)} chars)")
        return icebreaker

    except Exception as e:
        logger.error(f"[AGENT] Gemini call failed: {e} — using fallback.")
        return _fallback(name, niche)


def _fallback(name: str, niche: str) -> str:
    """Clean template fallback — still beats a generic cold email."""
    return (
        f"Came across your {niche} content and the depth you bring to your audience "
        f"genuinely stood out. "
        f"I think there's a strong fit between your community and what we're building "
        f"at HyroTrader, and I'd love to explore something together."
    )
