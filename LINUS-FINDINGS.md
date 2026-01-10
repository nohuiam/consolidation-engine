# Linus Audit Findings: consolidation-engine

**Auditor:** Claude Code Instance 8
**Date:** 2026-01-08
**Server:** consolidation-engine
**Ports:** 3032 (UDP) / 8032 (HTTP) / 9032 (WS)

---

## Executive Summary

| Metric | Value |
|--------|-------|
| Critical Issues | 0 |
| Major Issues | 2 (FIXED) |
| Minor Issues | 4 (Documented) |
| Existing Tests | 217 (210 passing, 7 pre-existing failures) |

---

## Issues Found

### Major (FIXED)

| ID | Issue | Status |
|----|-------|--------|
| M1 | CORS wildcard (`*`) allowing any origin | FIXED |
| M2 | No rate limiting on any endpoint | FIXED |

### Minor (Documented)

| ID | Issue | Notes |
|----|-------|-------|
| m1 | No file backup before merge operations | Design consideration |
| m2 | No atomic file writes | Direct writeFileSync |
| m3 | Race conditions in concurrent merges | No file locking |
| m4 | Pre-existing InterLock test failures | 7 tumbler/protocol tests failing |

---

## Fixes Applied

### CORS (M1)

**Before:**
```typescript
res.header('Access-Control-Allow-Origin', '*');
```

**After:**
```typescript
import cors from 'cors';

const ALLOWED_ORIGINS = [
  'http://localhost:5173',
  'http://127.0.0.1:5173',
  'http://localhost:3099',
  'http://localhost:8032'
];

app.use(cors({
  origin: (origin, callback) => {
    if (!origin) return callback(null, true);
    if (ALLOWED_ORIGINS.includes(origin)) return callback(null, true);
    callback(null, false);
  },
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));
```

### Rate Limiting (M2)

**Before:** No rate limiting

**After:**
```typescript
import rateLimit from 'express-rate-limit';

const generalLimiter = rateLimit({
  windowMs: 60 * 1000,
  limit: 100,
  standardHeaders: 'draft-7',
  legacyHeaders: false
});

const mergeLimiter = rateLimit({
  windowMs: 60 * 1000,
  limit: 20,  // Conservative for file ops
  standardHeaders: 'draft-7',
  legacyHeaders: false
});

app.use(generalLimiter);
app.post('/api/merge', mergeLimiter, ...);
```

---

## Test Status

**Existing Tests:** 217 total
- 210 passing
- 7 failing (pre-existing InterLock tumbler/protocol issues)

Pre-existing failures are in:
- `src/__tests__/interlock/protocol.test.ts` (2 failures)
- `src/__tests__/interlock/tumbler.test.ts` (3 failures)
- `src/__tests__/http/server.test.ts` (2 failures - expected 200, got 204)

These failures existed before CORS/rate limiting changes and are unrelated to security fixes.

---

## Files Modified

- `src/http/server.ts` - CORS whitelist + rate limiting
- `package.json` - Added cors, express-rate-limit, @types/cors

---

## Positive Observations

- Comprehensive test coverage (89% in handoff)
- Plan manager with validation
- Conflict resolution system
- Well-structured merge engine
- SQLite database with proper schema
