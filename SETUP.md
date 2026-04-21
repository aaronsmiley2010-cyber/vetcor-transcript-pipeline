# VetCor Transcript Evaluation Pipeline — Setup Guide

This runs your PERF transcript evaluation every night at 2 AM Eastern, completely hands-free. The output is a live dashboard URL you can bookmark and share with Dr. Shoemaker.

---

## One-Time Setup (about 10 minutes)

### Step 1: Create a GitHub account (skip if you have one)

Go to [github.com](https://github.com) and sign up. Free account is all you need.

### Step 2: Create a new repository

1. Click the **+** button (top right) → **New repository**
2. Name it: `vetcor-transcript-pipeline`
3. Set it to **Private** (this contains clinical data references)
4. Click **Create repository**

### Step 3: Upload the pipeline files

The easiest way — on the repo page, click **"uploading an existing file"** and drag in everything from the `vetcor-transcript-pipeline` folder:

- `pipeline.py`
- `config.py`
- `requirements.txt`
- `template.html`
- `.github/workflows/nightly-eval.yml`

Click **Commit changes**.

### Step 4: Add your secrets

This keeps your API keys safe — they're encrypted and never visible in the code.

1. Go to your repo → **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret** and add these three:

| Name                    | Value                           |
|-------------------------|---------------------------------|
| `ANTHROPIC_API_KEY`     | Your Anthropic API key (sk-ant-...) |
| `AWS_ACCESS_KEY_ID`     | Your S3 access key (AKIA...)    |
| `AWS_SECRET_ACCESS_KEY` | Your S3 secret key              |

### Step 5: Enable GitHub Pages

1. Go to repo → **Settings** → **Pages**
2. Under **Source**, select **Deploy from a branch**
3. Set branch to `main` and folder to `/docs`
4. Click **Save**

### Step 6: Run it for the first time

1. Go to repo → **Actions** tab
2. Click **Nightly Transcript Evaluation** on the left
3. Click **Run workflow** → **Run workflow**
4. Wait ~5-10 minutes for it to complete (you'll see a green checkmark)

### Step 7: Bookmark your dashboard

Your live dashboard URL will be:

```
https://YOUR-GITHUB-USERNAME.github.io/vetcor-transcript-pipeline/
```

This URL updates automatically every night at 2 AM. Share it with Dr. Shoemaker — she can bookmark it on any device.

---

## What Happens Every Night

1. GitHub spins up a fresh Linux machine at 2 AM Eastern
2. Downloads all PERF transcripts from S3
3. Samples 50 transcripts per DVM
4. Sends each to Claude Haiku for structured clinical evaluation
5. Aggregates scores per DVM
6. Injects data into the CVO Dashboard template
7. Publishes the updated dashboard to your GitHub Pages URL
8. Shuts down the machine (you pay nothing)

---

## Costs

| Item | Cost |
|------|------|
| GitHub Actions | Free (2,000 min/month included) |
| GitHub Pages hosting | Free |
| Anthropic API (Haiku) | ~$1-2 per run |
| **Monthly total** | **~$30-60** (API only) |

---

## Troubleshooting

**Pipeline failed?** Go to Actions tab → click the failed run → read the red error message.

**Dashboard not updating?** Check that GitHub Pages is set to `main` branch, `/docs` folder.

**New DVMs added to S3?** They'll be picked up automatically on the next run. If you need to update the hospital name mapping, edit `HOSPITAL_MAP` in `pipeline.py`.

**Want to change the schedule?** Edit `.github/workflows/nightly-eval.yml` — the cron line. Use [crontab.guru](https://crontab.guru) to pick your time.

**Want to run it manually?** Actions tab → Nightly Transcript Evaluation → Run workflow.
