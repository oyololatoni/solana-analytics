---
description: Deploy Solana Analytics across Neon Postgres, Fly.io, and verify Helius webhooks
---

# Full-Stack Deployment

Deploys in order: **Neon DB → Fly.io → Helius verification**

## Prerequisites
- `DATABASE_URL` environment variable set (or in `.env.local`)
- `fly` CLI authenticated (`fly auth login`)
- Working directory: `/home/dlip-24/projects/solana-analytics`

## Steps

1. Load environment variables
```bash
export $(cat .env.local | xargs)
```

// turbo
2. Preview database migrations (dry run)
```bash
python tools/deploy.py --dry-run
```

3. Run full deployment (DB + Fly.io + Helius check)
```bash
python tools/deploy.py
```

4. Monitor deployment logs
// turbo
```bash
fly logs -a solana-analytics --no-tail
```

5. Verify health endpoint
// turbo
```bash
curl -s https://solana-analytics.fly.dev/metrics/ingestion-health | python -m json.tool
```

## If you only need to update one layer:

### Database only (no code deploy):
```bash
python tools/deploy.py --db-only
```

### Code only (skip migration):
```bash
python tools/deploy.py --skip-db
```

### Manual migration:
```bash
python tools/migrate_schema.py          # apply
python tools/verify_schema.py           # verify
```
