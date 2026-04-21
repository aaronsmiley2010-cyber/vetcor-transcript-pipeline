"""
VetCor Transcript Evaluation Pipeline — Configuration
======================================================
Credentials are loaded from environment variables.

For local runs, set them in your terminal first:
    export ANTHROPIC_API_KEY="sk-ant-..."
    export AWS_ACCESS_KEY_ID="AKIA..."
    export AWS_SECRET_ACCESS_KEY="..."

For GitHub Actions, they're injected automatically from repo Secrets.
"""
import os

# ── AWS S3 (read-only) ──────────────────────────────────────────────
S3_BUCKET        = "vetsoap-exports"
S3_PREFIX        = "PERF/"                       # folder inside the bucket
AWS_ACCESS_KEY   = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY   = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION       = "us-east-1"

# ── Anthropic ────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL             = "claude-haiku-4-5-20251001"    # fast + cheap
MAX_TOKENS        = 1024                           # response cap per call

# ── Evaluation settings ─────────────────────────────────────────────
SAMPLE_SIZE       = 0           # transcripts to evaluate per DVM (0 = all)
MIN_TRANSCRIPT_LINES = 4       # skip very short transcripts
MAX_CONCURRENT    = 4           # parallel Haiku calls (lower = fewer 429s, higher = faster)
FILTER_YEAR       = 2026        # only process transcripts from this year (0 = no filter)

# ── Paths ────────────────────────────────────────────────────────────
WORK_DIR          = "/tmp/vetcor_pipeline"         # temp workspace (transcripts, zips)
TEMPLATE_PATH     = "template.html"                # CVO dashboard template (same folder as this script)
OUTPUT_PATH       = "CVO_PERF_Intelligence_Dashboard.html"  # output file
