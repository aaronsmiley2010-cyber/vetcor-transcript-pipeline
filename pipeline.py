#!/usr/bin/env python3
"""
VetCor Transcript Evaluation Pipeline
======================================
Pulls veterinary transcripts from S3, evaluates each with Claude Haiku,
aggregates per-DVM scores, and outputs an updated CVO Intelligence Dashboard.

Usage:
    python pipeline.py                  # full run
    python pipeline.py --skip-download  # reuse previously downloaded transcripts
    python pipeline.py --dry-run        # download + parse only, no Haiku calls
"""

import argparse
import asyncio
import io
import json
import logging
import os
import random
import re
import sys
import time
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any

import boto3
import anthropic

import config

# ── Logging ──────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")


# ═══════════════════════════════════════════════════════════════════════
#  STEP 1 — Download & extract transcripts from S3
# ═══════════════════════════════════════════════════════════════════════

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=config.AWS_ACCESS_KEY,
        aws_secret_access_key=config.AWS_SECRET_KEY,
        region_name=config.AWS_REGION,
    )


def download_and_extract(s3) -> dict[str, Path]:
    """
    Download every .zip under S3_PREFIX, extract to WORK_DIR.
    Returns {dvm_key: Path_to_extracted_folder}.
    """
    work = Path(config.WORK_DIR)
    work.mkdir(parents=True, exist_ok=True)
    extracted_root = work / "extracted"
    extracted_root.mkdir(exist_ok=True)

    # List all objects under prefix
    paginator = s3.get_paginator("list_objects_v2")
    zips = []
    for page in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=config.S3_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".zip"):
                zips.append(obj["Key"])

    log.info(f"Found {len(zips)} zip files in s3://{config.S3_BUCKET}/{config.S3_PREFIX}")

    dvm_dirs: dict[str, Path] = {}
    for key in zips:
        fname = key.split("/")[-1]
        dvm_key = fname.replace(".zip", "")
        dest = extracted_root / dvm_key

        if dest.exists() and any(dest.iterdir()):
            log.info(f"  [cached] {dvm_key} ({sum(1 for _ in dest.glob('*.txt'))} files)")
            dvm_dirs[dvm_key] = dest
            continue

        log.info(f"  Downloading {fname}...")
        buf = io.BytesIO()
        s3.download_fileobj(config.S3_BUCKET, key, buf)
        buf.seek(0)

        dest.mkdir(exist_ok=True)
        with zipfile.ZipFile(buf) as zf:
            zf.extractall(dest)

        txt_count = sum(1 for _ in dest.rglob("*.txt"))
        log.info(f"  Extracted {txt_count} transcripts → {dvm_key}")
        dvm_dirs[dvm_key] = dest

    return dvm_dirs


# ═══════════════════════════════════════════════════════════════════════
#  STEP 2 — Parse transcript files
# ═══════════════════════════════════════════════════════════════════════

def parse_transcript(path: Path) -> dict | None:
    """
    Parse a VetSOAP transcript file.
    Returns {metadata: {...}, dialogue: str} or None if too short.
    """
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None

    lines = text.strip().split("\n")
    if len(lines) < config.MIN_TRANSCRIPT_LINES:
        return None

    # First line is JSON metadata
    metadata = {}
    try:
        metadata = json.loads(lines[0])
    except (json.JSONDecodeError, IndexError):
        pass

    # Rest is dialogue
    dialogue = "\n".join(lines[1:]).strip()
    if not dialogue:
        return None

    return {"metadata": metadata, "dialogue": dialogue}


def load_dvm_transcripts(dvm_dirs: dict[str, Path]) -> dict[str, list[dict]]:
    """
    Load and parse all transcripts per DVM.
    Returns {dvm_key: [parsed_transcripts]}.
    """
    result = {}
    for dvm_key, folder in dvm_dirs.items():
        txts = sorted(folder.rglob("*.txt"))
        parsed = []
        for t in txts:
            p = parse_transcript(t)
            if p:
                parsed.append(p)
        result[dvm_key] = parsed
        log.info(f"  {dvm_key}: {len(parsed)} valid transcripts (of {len(txts)} files)")
    return result


def filter_by_year(all_transcripts: dict[str, list[dict]]) -> dict[str, list[dict]]:
    """Filter transcripts to only those created in FILTER_YEAR."""
    if not getattr(config, "FILTER_YEAR", 0):
        return all_transcripts

    year_str = str(config.FILTER_YEAR)
    filtered = {}
    for dvm_key, txs in all_transcripts.items():
        kept = [
            tx for tx in txs
            if tx.get("metadata", {}).get("created", "").startswith(year_str)
        ]
        if kept:
            filtered[dvm_key] = kept
        log.info(f"  {dvm_key}: {len(kept)}/{len(txs)} transcripts from {year_str}")
    return filtered


def sample_transcripts(all_transcripts: dict[str, list[dict]]) -> dict[str, list[dict]]:
    """Sample N transcripts per DVM if SAMPLE_SIZE > 0."""
    if config.SAMPLE_SIZE <= 0:
        return all_transcripts

    sampled = {}
    for dvm_key, txs in all_transcripts.items():
        if len(txs) <= config.SAMPLE_SIZE:
            sampled[dvm_key] = txs
        else:
            sampled[dvm_key] = random.sample(txs, config.SAMPLE_SIZE)
        log.info(f"  {dvm_key}: sampled {len(sampled[dvm_key])} of {len(txs)}")
    return sampled


# ═══════════════════════════════════════════════════════════════════════
#  STEP 3 — Claude Haiku evaluation
# ═══════════════════════════════════════════════════════════════════════

EVAL_SYSTEM_PROMPT = """You are VetCor's clinical transcript evaluator. You analyze veterinary appointment transcripts and extract structured signals about clinical behavior.

Speaker 0 is ALWAYS the veterinarian (DVM). Other speakers are pet owners or staff.

For each transcript, evaluate these four clinical domains and return a JSON object. Be conservative — only mark something as TRUE if there is clear evidence in the dialogue.

Return ONLY valid JSON, no markdown fences, no explanation."""

EVAL_USER_PROMPT = """Evaluate this veterinary appointment transcript:

<transcript>
{dialogue}
</transcript>

Return a JSON object with exactly these fields:

{{
  "lab": {{
    "recommended": true/false,       // DVM recommended any diagnostics (bloodwork, urinalysis, x-ray, cytology, biopsy, fecal, etc.)
    "completed": true/false,          // Evidence the diagnostics were accepted/run during the visit
    "hedging": true/false             // DVM used weak/optional language ("we could", "you might want to", "it's up to you", "optional but")
  }},
  "dental": {{
    "mentioned": true/false,          // DVM mentioned teeth, dental, oral health, tartar, gingivitis, or periodontal disease
    "recommended": true/false,        // DVM recommended a dental cleaning, extraction, or dental procedure
    "graded": true/false              // DVM assigned a dental grade (grade 1-4, stage 1-4, mild/moderate/severe)
  }},
  "prevention": {{
    "discussed": true/false,          // DVM discussed heartworm, flea, tick, or parasite prevention
    "recommended": true/false,        // DVM actively recommended starting, continuing, or switching a prevention product
    "dispensed": true/false            // Evidence that prevention product was sent home, prescribed, or dispensed
  }},
  "forward_booking": {{
    "type": "definite" | "vague" | "none"
    // "definite" = specific date/timeframe mentioned ("come back in 2 weeks", "schedule for March", "recheck Monday")
    // "vague" = general follow-up without date ("call us if", "keep an eye on it", "let us know")
    // "none" = no follow-up plan discussed at all
  }}
}}"""


async def _call_haiku(client: anthropic.AsyncAnthropic, dialogue: str) -> dict:
    """Single Haiku API call. Returns parsed JSON or raises."""
    resp = await client.messages.create(
        model=config.MODEL,
        max_tokens=config.MAX_TOKENS,
        system=EVAL_SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": EVAL_USER_PROMPT.format(dialogue=dialogue),
        }],
    )
    text = resp.content[0].text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return json.loads(text)


async def evaluate_transcript(
    client: anthropic.AsyncAnthropic,
    dialogue: str,
    semaphore: asyncio.Semaphore,
) -> dict | None:
    """Send one transcript to Haiku with exponential backoff retries."""
    if len(dialogue) > 12000:
        dialogue = dialogue[:12000] + "\n[... transcript truncated ...]"

    max_retries = 4
    async with semaphore:
        for attempt in range(max_retries):
            try:
                return await _call_haiku(client, dialogue)
            except json.JSONDecodeError as e:
                log.warning(f"  JSON parse error: {e}")
                return None
            except anthropic.RateLimitError:
                wait = 10 * (2 ** attempt)  # 10s, 20s, 40s, 80s
                log.warning(f"  Rate limited — attempt {attempt+1}/{max_retries}, waiting {wait}s...")
                await asyncio.sleep(wait)
            except Exception as e:
                log.warning(f"  API error: {e}")
                return None
        log.warning("  Max retries exhausted")
        return None


async def evaluate_dvm(
    client: anthropic.AsyncAnthropic,
    dvm_key: str,
    transcripts: list[dict],
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """Evaluate all sampled transcripts for one DVM."""
    log.info(f"  Evaluating {dvm_key} ({len(transcripts)} transcripts)...")
    tasks = [
        evaluate_transcript(client, tx["dialogue"], semaphore)
        for tx in transcripts
    ]
    results = await asyncio.gather(*tasks)
    valid = [r for r in results if r is not None]
    log.info(f"  {dvm_key}: {len(valid)}/{len(transcripts)} successful evaluations")
    return valid


async def evaluate_all(sampled: dict[str, list[dict]]) -> dict[str, list[dict]]:
    """Run Haiku evaluation across all DVMs."""
    client = anthropic.AsyncAnthropic(api_key=config.ANTHROPIC_API_KEY)
    semaphore = asyncio.Semaphore(config.MAX_CONCURRENT)

    results = {}
    # Process DVMs sequentially but transcripts within each DVM concurrently
    for dvm_key, transcripts in sampled.items():
        results[dvm_key] = await evaluate_dvm(client, dvm_key, transcripts, semaphore)

    return results


# ═══════════════════════════════════════════════════════════════════════
#  STEP 4 — Aggregate into dashboard data
# ═══════════════════════════════════════════════════════════════════════

def parse_dvm_name(dvm_key: str) -> str:
    """Convert '225-321-wynnpalmer' → 'Dr. Wynn Palmer'"""
    parts = dvm_key.split("-", 2)
    if len(parts) < 3:
        return dvm_key
    raw = parts[2]
    # Split camelCase-ish name: find where lowercase meets uppercase
    # For 'wynnpalmer' we need heuristics
    # Try common first-name lengths
    name = raw.lower()
    # Simple approach: try to split at common boundaries
    for i in range(2, len(name) - 1):
        if name[i:].capitalize() == name[i:]:
            continue
    # Fallback: use a simple heuristic — find the split point
    # where the first name ends. Try lengths 3-8 for first name.
    best_split = len(name) // 2
    common_first = [
        "joe", "ilana", "teresa", "madison", "michelle", "wynn", "avery",
        "benigno", "julia", "katherine", "emily", "crystal", "john",
        "candace", "rachael", "kimberly", "celeste", "lynda", "ned", "stevie", "em",
    ]
    for first in common_first:
        if name.startswith(first):
            best_split = len(first)
            break

    first_name = name[:best_split].capitalize()
    last_name = name[best_split:].capitalize()
    return f"Dr. {first_name} {last_name}"


def parse_hospital_id(dvm_key: str) -> str:
    """Extract hospital ID prefix from dvm_key."""
    parts = dvm_key.split("-")
    return parts[0] if parts else dvm_key


# Known hospital mappings from the users CSV cross-reference
HOSPITAL_MAP = {
    "111": "Cresskill Animal Hospital",
    "189": "Cat Care of Rochester Hills",
    "225": "Animal Care Vet Hospital - Roy",
    "249": "Columbus Central Vet Hospital",
    "289": "KC Cat Clinic",
    "358": "Animal Clinic at New Lenox",
    "403": "VetCor Hospital 403",
    "547": "Springfield Animal Hospital",
    "550": "Animal Medical Center of McKinney",
    "581": "Tech Ridge Pet Hospital",
    "648": "Eastside Animal Medical Center",
    "785": "Oakdale Veterinary Group",
}


def aggregate_dvm(dvm_key: str, evals: list[dict], total_transcripts: int) -> dict | None:
    """
    Aggregate Haiku evaluations into a single DVM dashboard record.
    Returns a dict matching the DVMS array schema.
    """
    if not evals:
        return None

    n = len(evals)

    # ── Lab ──
    lab_recommended = sum(1 for e in evals if e.get("lab", {}).get("recommended"))
    lab_completed   = sum(1 for e in evals if e.get("lab", {}).get("completed"))
    lab_hedging     = sum(1 for e in evals if e.get("lab", {}).get("hedging"))

    lr = round(lab_recommended / n, 3)          # recommendation rate
    lc = round(lab_completed / n, 3)             # completion rate
    lh = round(lab_hedging / max(lab_recommended, 1), 3)  # hedging rate (of recommended)

    # Lab profile assignment
    if lr < 0.15:
        lp = "forgetter"
    elif lh > 0.35:
        lp = "hedger"
    elif lc / max(lr, 0.01) > 0.7:
        lp = "strong"
    else:
        lp = "moderate"

    # ── Dental ──
    dental_mentioned   = sum(1 for e in evals if e.get("dental", {}).get("mentioned"))
    dental_recommended = sum(1 for e in evals if e.get("dental", {}).get("recommended"))
    dental_graded      = sum(1 for e in evals if e.get("dental", {}).get("graded"))

    dm = round(dental_mentioned / n, 3)
    dr = round(dental_recommended / n, 3)
    dg = round(dental_graded / n, 3)

    # Dental profile
    if dm < 0.15:
        dp = "ignorer"
    elif dr / max(dm, 0.01) < 0.25:
        dp = "observer"
    else:
        dp = "recommender"

    # ── Prevention ──
    prev_discussed   = sum(1 for e in evals if e.get("prevention", {}).get("discussed"))
    prev_recommended = sum(1 for e in evals if e.get("prevention", {}).get("recommended"))
    prev_dispensed   = sum(1 for e in evals if e.get("prevention", {}).get("dispensed"))

    pm = round(prev_discussed / n, 3)
    pr = round(prev_recommended / n, 3)
    pd = round(prev_dispensed / n, 3)

    # Prevention profile
    if pm < 0.15:
        pp = "passive"
    elif pr / max(pm, 0.01) > 0.5:
        pp = "proactive"
    else:
        pp = "reactive"

    # ── Forward Booking ──
    fwd_definite = sum(1 for e in evals if e.get("forward_booking", {}).get("type") == "definite")
    fwd_vague    = sum(1 for e in evals if e.get("forward_booking", {}).get("type") == "vague")
    fwd_none     = sum(1 for e in evals if e.get("forward_booking", {}).get("type") == "none")
    fwd_total    = fwd_definite + fwd_vague + fwd_none or 1

    fd = round(fwd_definite / fwd_total, 3)
    fv = round(fwd_vague / fwd_total, 3)
    fn = round(fwd_none / fwd_total, 3)

    # Forward booking profile
    if fd > 0.3:
        fp = "definite"
    elif fv > 0.4:
        fp = "vague"
    else:
        fp = "none"

    hospital_id = parse_hospital_id(dvm_key)
    hospital = HOSPITAL_MAP.get(hospital_id, f"VetCor Hospital {hospital_id}")

    return {
        "n": parse_dvm_name(dvm_key),
        "h": hospital,
        "v": total_transcripts,
        "ye": 0,
        "lr": lr, "lc": lc, "lh": lh, "lp": lp,
        "dm": dm, "dr": dr, "dg": dg, "dp": dp,
        "pm": pm, "pr": pr, "pd": pd, "pp": pp,
        "fd": fd, "fv": fv, "fn": fn, "fp": fp,
    }


def compute_cause_distributions(all_evals: dict[str, list[dict]]) -> dict:
    """
    Compute root-cause distributions across all DVMs.
    Returns {lab_causes, dental_causes, prev_causes, fwd_causes}.
    """
    flat = []
    for evals in all_evals.values():
        flat.extend(evals)

    n = len(flat) or 1

    # Lab causes — derive from signal patterns
    lab_rec = sum(1 for e in flat if e.get("lab", {}).get("recommended"))
    lab_hedge = sum(1 for e in flat if e.get("lab", {}).get("hedging"))
    lab_comp = sum(1 for e in flat if e.get("lab", {}).get("completed"))
    lab_not_rec = n - lab_rec

    lab_causes = {
        "not_recommended": round(lab_not_rec / n * 100, 1),
        "hedging_language": round(lab_hedge / n * 100, 1),
        "recommended_not_completed": round((lab_rec - lab_comp) / n * 100, 1),
        "recommended_and_completed": round(lab_comp / n * 100, 1),
    }

    # Dental causes
    den_ment = sum(1 for e in flat if e.get("dental", {}).get("mentioned"))
    den_rec = sum(1 for e in flat if e.get("dental", {}).get("recommended"))
    den_grade = sum(1 for e in flat if e.get("dental", {}).get("graded"))
    den_not_ment = n - den_ment

    dental_causes = {
        "not_mentioned": round(den_not_ment / n * 100, 1),
        "mentioned_not_recommended": round((den_ment - den_rec) / n * 100, 1),
        "recommended": round(den_rec / n * 100, 1),
        "graded": round(den_grade / n * 100, 1),
    }

    # Prevention causes
    prev_disc = sum(1 for e in flat if e.get("prevention", {}).get("discussed"))
    prev_rec = sum(1 for e in flat if e.get("prevention", {}).get("recommended"))
    prev_disp = sum(1 for e in flat if e.get("prevention", {}).get("dispensed"))
    prev_not_disc = n - prev_disc

    prev_causes = {
        "not_discussed": round(prev_not_disc / n * 100, 1),
        "discussed_not_recommended": round((prev_disc - prev_rec) / n * 100, 1),
        "recommended_not_dispensed": round((prev_rec - prev_disp) / n * 100, 1),
        "dispensed": round(prev_disp / n * 100, 1),
    }

    # Forward booking causes
    fwd_def = sum(1 for e in flat if e.get("forward_booking", {}).get("type") == "definite")
    fwd_vag = sum(1 for e in flat if e.get("forward_booking", {}).get("type") == "vague")
    fwd_non = sum(1 for e in flat if e.get("forward_booking", {}).get("type") == "none")

    fwd_causes = {
        "no_follow_up": round(fwd_non / n * 100, 1),
        "vague_follow_up": round(fwd_vag / n * 100, 1),
        "specific_date": round(fwd_def / n * 100, 1),
    }

    return {
        "lab": lab_causes,
        "dental": dental_causes,
        "prevention": prev_causes,
        "forward_booking": fwd_causes,
    }


def compute_profile_distributions(dvms: list[dict]) -> dict:
    """Count DVMs in each profile bucket."""
    lab_profiles = defaultdict(int)
    dental_profiles = defaultdict(int)
    prev_profiles = defaultdict(int)
    fwd_profiles = defaultdict(int)

    for d in dvms:
        lab_profiles[d["lp"]] += 1
        dental_profiles[d["dp"]] += 1
        prev_profiles[d["pp"]] += 1
        fwd_profiles[d["fp"]] += 1

    return {
        "lab": dict(lab_profiles),
        "dental": dict(dental_profiles),
        "prevention": dict(prev_profiles),
        "forward_booking": dict(fwd_profiles),
    }


# ═══════════════════════════════════════════════════════════════════════
#  STEP 5 — Inject data into CVO Dashboard template
# ═══════════════════════════════════════════════════════════════════════

def inject_dashboard(dvms: list[dict], causes: dict, profiles: dict) -> str:
    """
    Read the CVO Dashboard template and replace all data constants.
    Returns the updated HTML string.
    """
    script_dir = Path(__file__).parent
    template = script_dir / config.TEMPLATE_PATH

    if not template.exists():
        log.error(f"Template not found at {template}")
        log.error("Copy your CVO_PERF_Intelligence_Dashboard.html to template.html in the pipeline folder.")
        sys.exit(1)

    html = template.read_text(encoding="utf-8")

    total_visits = sum(d["v"] for d in dvms)
    total_transcripts = total_visits  # approximation

    # Replace DVMS array
    dvms_json = json.dumps(dvms)
    html = re.sub(r"const DVMS = \[.*?\];", f"const DVMS = {dvms_json};", html, flags=re.DOTALL)

    # Replace REGIONS (empty — we removed regions)
    html = re.sub(r"const REGIONS = \{.*?\};", "const REGIONS = {};", html)

    # Replace cause constants
    html = re.sub(r"const LAB_CAUSES = \{.*?\};", f"const LAB_CAUSES = {json.dumps(causes['lab'])};", html)
    html = re.sub(r"const DENTAL_CAUSES = \{.*?\};", f"const DENTAL_CAUSES = {json.dumps(causes['dental'])};", html)
    html = re.sub(r"const PREV_CAUSES = \{.*?\};", f"const PREV_CAUSES = {json.dumps(causes['prevention'])};", html)
    html = re.sub(r"const FWD_CAUSES = \{.*?\};", f"const FWD_CAUSES = {json.dumps(causes['forward_booking'])};", html)

    # Replace profile distributions
    html = re.sub(r"const LAB_PROFILES = \{.*?\};", f"const LAB_PROFILES = {json.dumps(profiles['lab'])};", html)
    html = re.sub(r"const DENTAL_PROFILES = \{.*?\};", f"const DENTAL_PROFILES = {json.dumps(profiles['dental'])};", html)
    html = re.sub(r"const PREV_PROFILES = \{.*?\};", f"const PREV_PROFILES = {json.dumps(profiles['prevention'])};", html)
    html = re.sub(r"const FWD_PROFILES = \{.*?\};", f"const FWD_PROFILES = {json.dumps(profiles['forward_booking'])};", html)

    # Update header meta
    html = re.sub(
        r'<div class="header-meta">.*?</div>',
        f'<div class="header-meta">{len(dvms)} PERF DVMs &bull; ~{total_visits:,} Monthly Visits &bull; '
        f'{total_transcripts:,} Transcripts Analyzed &bull; PERF Cohort &bull; CVO Eyes Only</div>',
        html,
    )

    # Update footer
    html = re.sub(
        r"Real data:.*?CVO Eyes Only",
        f"Real data: {len(dvms)} PERF DVMs &bull; {total_transcripts:,} transcripts from S3 PERF export "
        f"&bull; {time.strftime('%B %Y')} &bull; CVO Eyes Only",
        html,
    )

    return html


# ═══════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="VetCor Transcript Evaluation Pipeline")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached transcripts")
    parser.add_argument("--dry-run", action="store_true", help="Download + parse only, no API calls")
    args = parser.parse_args()

    t0 = time.time()
    log.info("=" * 60)
    log.info("VetCor Transcript Evaluation Pipeline")
    log.info("=" * 60)

    # Step 1: Download
    if args.skip_download:
        log.info("\n[1/5] Using cached transcripts...")
        extracted = Path(config.WORK_DIR) / "extracted"
        dvm_dirs = {d.name: d for d in sorted(extracted.iterdir()) if d.is_dir()}
        log.info(f"  Found {len(dvm_dirs)} DVM folders")
    else:
        log.info("\n[1/5] Downloading transcripts from S3...")
        s3 = get_s3_client()
        dvm_dirs = download_and_extract(s3)

    # Step 2: Parse
    log.info("\n[2/5] Parsing transcripts...")
    all_transcripts = load_dvm_transcripts(dvm_dirs)

    # Filter by year if configured
    if getattr(config, "FILTER_YEAR", 0):
        log.info(f"\n[2b/5] Filtering to {config.FILTER_YEAR} transcripts...")
        all_transcripts = filter_by_year(all_transcripts)

    # Filter out DVMs with too few transcripts
    all_transcripts = {k: v for k, v in all_transcripts.items() if len(v) >= 5}
    log.info(f"  {len(all_transcripts)} DVMs with ≥5 valid transcripts")

    # Step 3: Sample
    log.info("\n[3/5] Sampling transcripts...")
    sampled = sample_transcripts(all_transcripts)

    total_to_eval = sum(len(v) for v in sampled.values())
    est_cost = total_to_eval * 0.001  # rough estimate for Haiku
    log.info(f"  Total transcripts to evaluate: {total_to_eval}")
    log.info(f"  Estimated API cost: ~${est_cost:.2f}")

    if args.dry_run:
        log.info("\n  [DRY RUN] Skipping API calls.")
        log.info(f"  Elapsed: {time.time() - t0:.1f}s")
        return

    # Step 4: Evaluate with Haiku
    log.info("\n[4/5] Evaluating with Claude Haiku...")
    eval_results = asyncio.run(evaluate_all(sampled))

    # Step 5: Aggregate + build dashboard
    log.info("\n[5/5] Aggregating results and building dashboard...")
    dvms = []
    for dvm_key, evals in eval_results.items():
        total = len(all_transcripts.get(dvm_key, []))
        record = aggregate_dvm(dvm_key, evals, total)
        if record:
            dvms.append(record)

    dvms.sort(key=lambda d: d["n"])
    log.info(f"  {len(dvms)} DVMs in final dashboard")

    # Safety check: don't overwrite dashboard with empty results
    if not dvms:
        log.error("  NO DVMs produced results — skipping dashboard update to protect existing data.")
        log.error("  Check API key credits and rate limits.")
        sys.exit(1)

    causes = compute_cause_distributions(eval_results)
    profiles = compute_profile_distributions(dvms)

    log.info("\n  Profile distributions:")
    for cat, dist in profiles.items():
        log.info(f"    {cat}: {dict(dist)}")

    html = inject_dashboard(dvms, causes, profiles)

    # Write output
    script_dir = Path(__file__).parent
    out_path = script_dir / config.OUTPUT_PATH
    out_path.write_text(html, encoding="utf-8")
    log.info(f"\n  Dashboard saved: {out_path} ({len(html):,} bytes)")

    # Also save raw evaluation data for debugging
    raw_path = script_dir / "last_run_evaluations.json"
    raw_path.write_text(json.dumps(eval_results, indent=2), encoding="utf-8")
    log.info(f"  Raw evaluations saved: {raw_path}")

    elapsed = time.time() - t0
    log.info(f"\n  Done in {elapsed:.1f}s")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
