from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # ── Existing ──────────────────────────────────────────────────────────────
    DATABASE_URL:       str
    GMAIL_USER:         str
    GMAIL_APP_PASSWORD: str
    APIFY_API_TOKEN:    str
    YOUTUBE_API_KEY:    str
    GOOGLE_SHEET_KEY:   str
    NGROK_TOKEN:        str

    # ── NEW: Agentic AI Layer ─────────────────────────────────────────────────
    # Optional — if missing, agent.py falls back to template icebreaker
    GEMINI_API_KEY: Optional[str] = None

    # ── NEW: Make.com External Tool Integration ───────────────────────────────
    # Optional — if missing, webhook silently skips (non-blocking)
    MAKE_WEBHOOK_URL: Optional[str] = None

    class Config:
        env_file          = ".env"
        env_file_encoding = "utf-8"
        extra             = "ignore"   # silently drop any unknown .env vars


settings = Settings()
