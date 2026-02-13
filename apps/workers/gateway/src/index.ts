export interface Env {
  ENVIRONMENT: string;
  INTERNAL_API_TOKEN: string;
  NONCE_GUARD: DurableObjectNamespace;
  JOB_QUEUE: DurableObjectNamespace;
}

type EnqueueBody = {
  type: "crawl" | "rank";
  target: string;
  maxAttempts?: number;
};

type Job = {
  id: string;
  type: "crawl" | "rank";
  target: string;
  createdAt: number;
};

type CompleteBody = { id: string; result: unknown };
type FailBody = { id: string; error: string };

function json(data: unknown, init: ResponseInit = {}) {
  return new Response(JSON.stringify(data), {
    ...init,
    headers: { "content-type": "application/json", ...(init.headers ?? {}) },
  });
}

function getRequestId(req: Request) {
  return req.headers.get("x-request-id") ?? crypto.randomUUID();
}

function requireAuth(req: Request, env: Env): Response | null {
  const auth = req.headers.get("authorization") ?? "";
  const expected = `Bearer ${env.INTERNAL_API_TOKEN}`;
  if (auth !== expected) return new Response("unauthorized", { status: 401 });
  return null;
}

function safeParseJson(maybeJson: string | null): unknown {
  if (!maybeJson) return null;
  try {
    return JSON.parse(maybeJson);
  } catch {
    return maybeJson;
  }
}

function toIso(ms: number | null | undefined): string | null {
  if (typeof ms !== "number") return null;
  try {
    return new Date(ms).toISOString();
  } catch {
    return null;
  }
}

function clampInt(n: unknown, min: number, max: number): number | null {
  if (typeof n !== "number" || !Number.isFinite(n)) return null;
  const x = Math.trunc(n);
  if (x < min) return min;
  if (x > max) return max;
  return x;
}

function clampIntFromString(s: string | null, fallback: number, min: number, max: number) {
  const n = parseInt(s ?? "", 10);
  const v = Number.isFinite(n) ? n : fallback;
  return Math.max(min, Math.min(max, v));
}

function encodeCursor(obj: unknown): string {
  return btoa(unescape(encodeURIComponent(JSON.stringify(obj))));
}

function decodeCursor(cursor: string | null): any | null {
  if (!cursor) return null;
  try {
    const jsonStr = decodeURIComponent(escape(atob(cursor)));
    return JSON.parse(jsonStr);
  } catch {
    return null;
  }
}

export class NonceGuard {
  constructor(private state: DurableObjectState) {}
  async fetch(): Promise<Response> {
    return new Response("nonce-guard ok");
  }
}

type JobRow = {
  id: string;
  type: string;
  target: string;
  createdAt: number;
  status: string;
  updatedAt: number | null;
  leaseUntil: number | null;
  result: string | null;
  error: string | null;
  attempts: number | null;
  maxAttempts: number | null;
  nextRunAt: number | null;
  sortAt: number | null;
};

function normalizeStatus(s: string | null): string | null {
  if (!s) return null;
  if (s === "queued" || s === "processing" || s === "done" || s === "failed") return s;
  return null;
}

export class JobQueue {
  constructor(private state: DurableObjectState) {}

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const sql = (this.state as any).storage.sql;

    // Base schema
    sql.exec(`
      CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        target TEXT NOT NULL,
        createdAt INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'queued',
        updatedAt INTEGER,
        leaseUntil INTEGER,
        result TEXT,
        error TEXT,
        attempts INTEGER NOT NULL DEFAULT 0,
        maxAttempts INTEGER NOT NULL DEFAULT 3,
        nextRunAt INTEGER,
        sortAt INTEGER
      );
    `);

    // Best-effort upgrades
    try { sql.exec("ALTER TABLE jobs ADD COLUMN status TEXT NOT NULL DEFAULT 'queued'"); } catch {}
    try { sql.exec("ALTER TABLE jobs ADD COLUMN updatedAt INTEGER"); } catch {}
    try { sql.exec("ALTER TABLE jobs ADD COLUMN leaseUntil INTEGER"); } catch {}
    try { sql.exec("ALTER TABLE jobs ADD COLUMN result TEXT"); } catch {}
    try { sql.exec("ALTER TABLE jobs ADD COLUMN error TEXT"); } catch {}
    try { sql.exec("ALTER TABLE jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0"); } catch {}
    try { sql.exec("ALTER TABLE jobs ADD COLUMN maxAttempts INTEGER NOT NULL DEFAULT 3"); } catch {}
    try { sql.exec("ALTER TABLE jobs ADD COLUMN nextRunAt INTEGER"); } catch {}
    try { sql.exec("ALTER TABLE jobs ADD COLUMN sortAt INTEGER"); } catch {}

    // Backfill sortAt for older rows (idempotent)
    sql.exec(`UPDATE jobs SET sortAt = COALESCE(updatedAt, createdAt) WHERE sortAt IS NULL`);

    // Indexes for list/purge
    sql.exec("CREATE INDEX IF NOT EXISTS idx_jobs_status_sortAt_id ON jobs(status, sortAt DESC, id DESC)");
    sql.exec("CREATE INDEX IF NOT EXISTS idx_jobs_sortAt_id ON jobs(sortAt DESC, id DESC)");
    sql.exec("CREATE INDEX IF NOT EXISTS idx_jobs_nextRunAt_status ON jobs(nextRunAt, status)");

    // ----- INSPECT: GET /get?id=... -----
    if (url.pathname === "/get" && req.method === "GET") {
      const id = url.searchParams.get("id");
      if (!id) return new Response("missing id", { status: 400 });

      const row = sql
        .exec(
          `SELECT id, type, target, createdAt, status, updatedAt, leaseUntil, result, error, attempts, maxAttempts, nextRunAt, sortAt
           FROM jobs
           WHERE id = ?
           LIMIT 1`,
          id
        )
        .toArray()[0] as JobRow | undefined;

      if (!row) return json({ ok: true, job: null });

      return json({
        ok: true,
        job: {
          ...row,
          result: safeParseJson(row.result),
          createdAtIso: toIso(row.createdAt),
          updatedAtIso: toIso(row.updatedAt),
          leaseUntilIso: toIso(row.leaseUntil),
          nextRunAtIso: toIso(row.nextRunAt),
          sortAtIso: toIso(row.sortAt),
        },
      });
    }

    // ----- INSPECT: GET /stats -----
    if (url.pathname === "/stats" && req.method === "GET") {
      const rows = sql
        .exec(
          `SELECT status, COUNT(*) as count
           FROM jobs
           GROUP BY status
           ORDER BY status ASC`
        )
        .toArray() as Array<{ status: string; count: number }>;

      return json({ ok: true, stats: rows });
    }

    // ----- INSPECT: GET /list -----
    // Query:
    //  - status (optional)
    //  - limit (default 50, max 200)
    //  - cursor (opaque): {"sortAt":number,"id":string}
    if (url.pathname === "/list" && req.method === "GET") {
      const status = normalizeStatus(url.searchParams.get("status"));
      const limit = clampIntFromString(url.searchParams.get("limit"), 50, 1, 200);

      const c = decodeCursor(url.searchParams.get("cursor"));
      const cursorSortAt =
        c && typeof c.sortAt === "number" && Number.isFinite(c.sortAt) ? Math.trunc(c.sortAt) : null;
      const cursorId = c && typeof c.id === "string" ? c.id : null;

      let rows: JobRow[] = [];

      if (status) {
        if (cursorSortAt !== null && cursorId) {
          rows = sql.exec(
            `SELECT id, type, target, createdAt, status, updatedAt, leaseUntil, result, error, attempts, maxAttempts, nextRunAt, sortAt
             FROM jobs
             WHERE status = ?
               AND (
                 sortAt < ?
                 OR (sortAt = ? AND id < ?)
               )
             ORDER BY sortAt DESC, id DESC
             LIMIT ?`,
            status,
            cursorSortAt,
            cursorSortAt,
            cursorId,
            limit
          ).toArray() as JobRow[];
        } else {
          rows = sql.exec(
            `SELECT id, type, target, createdAt, status, updatedAt, leaseUntil, result, error, attempts, maxAttempts, nextRunAt, sortAt
             FROM jobs
             WHERE status = ?
             ORDER BY sortAt DESC, id DESC
             LIMIT ?`,
            status,
            limit
          ).toArray() as JobRow[];
        }
      } else {
        if (cursorSortAt !== null && cursorId) {
          rows = sql.exec(
            `SELECT id, type, target, createdAt, status, updatedAt, leaseUntil, result, error, attempts, maxAttempts, nextRunAt, sortAt
             FROM jobs
             WHERE
               sortAt < ?
               OR (sortAt = ? AND id < ?)
             ORDER BY sortAt DESC, id DESC
             LIMIT ?`,
            cursorSortAt,
            cursorSortAt,
            cursorId,
            limit
          ).toArray() as JobRow[];
        } else {
          rows = sql.exec(
            `SELECT id, type, target, createdAt, status, updatedAt, leaseUntil, result, error, attempts, maxAttempts, nextRunAt, sortAt
             FROM jobs
             ORDER BY sortAt DESC, id DESC
             LIMIT ?`,
            limit
          ).toArray() as JobRow[];
        }
      }

      const items = rows.map((r) => ({
        ...r,
        result: safeParseJson(r.result),
        createdAtIso: toIso(r.createdAt),
        updatedAtIso: toIso(r.updatedAt),
        leaseUntilIso: toIso(r.leaseUntil),
        nextRunAtIso: toIso(r.nextRunAt),
        sortAtIso: toIso(r.sortAt),
      }));

      const last = items[items.length - 1] as any | undefined;
      const nextCursor =
        last
          ? encodeCursor({
              sortAt: (last.sortAt ?? last.updatedAt ?? last.createdAt) as number,
              id: last.id as string,
            })
          : null;

      return json({ ok: true, items, nextCursor });
    }

    // ----- MAINTENANCE: POST /purge -----
    if (url.pathname === "/purge" && req.method === "POST") {
      let beforeMs: number | null = null;
      try {
        const body = (await req.json()) as { beforeMs?: unknown };
        if (typeof body?.beforeMs === "number") beforeMs = body.beforeMs;
      } catch {}

      if (beforeMs === null) return new Response("missing beforeMs", { status: 400 });

      sql.exec(
        `DELETE FROM jobs
         WHERE (status = 'done' OR status = 'failed')
           AND sortAt < ?`,
        beforeMs
      );

      const deletedRow = sql.exec("SELECT changes() AS deleted").toArray()[0] as { deleted: number } | undefined;
      const deleted = deletedRow?.deleted ?? 0;

      const statsAfter = sql
        .exec(
          `SELECT status, COUNT(*) as count
           FROM jobs
           GROUP BY status
           ORDER BY status ASC`
        )
        .toArray() as Array<{ status: string; count: number }>;

      return json({ ok: true, beforeMs, deleted, statsAfter });
    }

    // ----- WRITE: POST /enqueue -----
    if (url.pathname === "/enqueue" && req.method === "POST") {
      const job = (await req.json()) as Partial<Job> & { maxAttempts?: unknown };

      const id = typeof job.id === "string" ? job.id : "";
      const type = job.type === "crawl" || job.type === "rank" ? job.type : null;
      const target = typeof job.target === "string" ? job.target : "";
      const createdAt = typeof job.createdAt === "number" ? job.createdAt : null;

      if (!id || !type || !target || createdAt === null) {
        return new Response("invalid job payload", { status: 400 });
      }

      const now = Date.now();
      const maxAttempts = clampInt(job.maxAttempts, 1, 10) ?? 3;

      // sortAt tracks "last updated time" for listing/auditing
      sql.exec(
        `INSERT INTO jobs (id, type, target, createdAt, status, updatedAt, attempts, maxAttempts, nextRunAt, sortAt)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)`,
        id,
        type,
        target,
        createdAt,
        "queued",
        now,
        0,
        maxAttempts,
        now
      );

      return new Response("ok");
    }

    // ----- LEASE: POST /dequeue -----
    if (url.pathname === "/dequeue" && req.method === "POST") {
      const now = Date.now();
      const leaseMs = 2 * 60 * 1000;
      const leaseUntil = now + leaseMs;

      const row = sql
        .exec(
          `SELECT id, type, target, createdAt
           FROM jobs
           WHERE
             (
               status = 'queued'
               AND (nextRunAt IS NULL OR nextRunAt <= ?)
             )
             OR
             (
               status = 'processing'
               AND leaseUntil IS NOT NULL
               AND leaseUntil < ?
             )
           ORDER BY createdAt ASC
           LIMIT 1`,
          now,
          now
        )
        .toArray()[0] as Job | undefined;

      if (!row) return json({ ok: true, job: null });

      sql.exec(
        "UPDATE jobs SET status = ?, updatedAt = ?, sortAt = ?, leaseUntil = ?, nextRunAt = NULL WHERE id = ?",
        "processing",
        now,
        now,
        leaseUntil,
        row.id
      );

      return json({ ok: true, job: row, leaseUntil });
    }

    // ----- ACK: POST /complete -----
    if (url.pathname === "/complete" && req.method === "POST") {
      const body = (await req.json()) as Partial<CompleteBody>;
      if (!body?.id) return new Response("missing id", { status: 400 });

      const now = Date.now();
      const resultJson = JSON.stringify(body.result ?? null);

      sql.exec(
        `UPDATE jobs
         SET status = ?, updatedAt = ?, sortAt = ?, leaseUntil = NULL, result = ?, error = NULL
         WHERE id = ?`,
        "done",
        now,
        now,
        resultJson,
        body.id
      );

      return new Response("ok");
    }

    // ----- ACK: POST /fail (retry) -----
    if (url.pathname === "/fail" && req.method === "POST") {
      const body = (await req.json()) as Partial<FailBody>;
      if (!body?.id) return new Response("missing id", { status: 400 });

      const now = Date.now();
      const err = String(body.error ?? "unknown");

      const row = sql
        .exec(
          `SELECT attempts, maxAttempts
           FROM jobs
           WHERE id = ?
           LIMIT 1`,
          body.id
        )
        .toArray()[0] as { attempts: number; maxAttempts: number } | undefined;

      if (!row) return new Response("not found", { status: 404 });

      const currentAttempts = typeof row.attempts === "number" ? row.attempts : 0;
      const maxAttempts = typeof row.maxAttempts === "number" ? row.maxAttempts : 3;

      const nextAttempts = currentAttempts + 1;

      const backoffMs =
        nextAttempts === 1 ? 10_000 :
        nextAttempts === 2 ? 60_000 :
        300_000;

      if (nextAttempts < maxAttempts) {
        const nextRunAt = now + backoffMs;

        sql.exec(
          `UPDATE jobs
           SET status = 'queued',
               updatedAt = ?,
               sortAt = ?,
               leaseUntil = NULL,
               error = ?,
               attempts = ?,
               nextRunAt = ?
           WHERE id = ?`,
          now,
          now,
          err,
          nextAttempts,
          nextRunAt,
          body.id
        );

        return json({ ok: true, retried: true, attempts: nextAttempts, maxAttempts, nextRunAt });
      }

      sql.exec(
        `UPDATE jobs
         SET status = 'failed',
             updatedAt = ?,
             sortAt = ?,
             leaseUntil = NULL,
             error = ?,
             attempts = ?,
             nextRunAt = NULL
         WHERE id = ?`,
        now,
        now,
        err,
        nextAttempts,
        body.id
      );

      return json({ ok: true, retried: false, attempts: nextAttempts, maxAttempts });
    }

    return new Response("not found", { status: 404 });
  }
}

export class JobQueueRouter {
  constructor(private env: Env) {}
  stub() {
    const doId = this.env.JOB_QUEUE.idFromName("main");
    return this.env.JOB_QUEUE.get(doId);
  }
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const requestId = getRequestId(req);

    if (url.pathname === "/health") {
      return json(
        { ok: true, service: "rankpublic-gateway", env: env.ENVIRONMENT, requestId },
        { status: 200, headers: { "x-request-id": requestId } }
      );
    }

    if (url.pathname === "/v1/ping") {
      const authErr = requireAuth(req, env);
      if (authErr) return authErr;
      return new Response("pong", { headers: { "x-request-id": requestId } });
    }

    const router = new JobQueueRouter(env);
    const stub = router.stub();

    const forward = async (doUrl: string) => {
      const res = await stub.fetch(doUrl, {
        method: req.method,
        headers: req.method === "GET" ? undefined : { "content-type": req.headers.get("content-type") ?? "application/json" },
        body: req.method === "GET" ? undefined : await req.text(),
      });

      const text = await res.text();
      return new Response(text, {
        status: res.status,
        headers: { "content-type": "application/json", "x-request-id": requestId },
      });
    };

    // All /v1/jobs/* require auth
    if (url.pathname.startsWith("/v1/jobs/")) {
      const authErr = requireAuth(req, env);
      if (authErr) return authErr;
    }

    if (url.pathname === "/v1/jobs/enqueue") {
      if (req.method !== "POST") return json({ error: "method_not_allowed", requestId }, { status: 405 });

      let body: EnqueueBody;
      try {
        body = (await req.json()) as EnqueueBody;
      } catch {
        return json({ error: "invalid_json", requestId }, { status: 400 });
      }

      if (!body?.type || !body?.target) return json({ error: "invalid_body", requestId }, { status: 400 });

      const job: Job = { id: crypto.randomUUID(), type: body.type, target: body.target, createdAt: Date.now() };

      try {
        await stub.fetch("https://do/enqueue", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ ...job, maxAttempts: body.maxAttempts }),
        });
      } catch (err) {
        console.error(JSON.stringify({ requestId, path: url.pathname, enqueueError: String(err) }));
        return json({ ok: false, requestId, error: "enqueue_failed" }, { status: 500 });
      }

      console.log(JSON.stringify({ requestId, path: url.pathname, enqueued: true, job }));
      return json({ ok: true, requestId, accepted: true, job }, { status: 202 });
    }

    if (url.pathname === "/v1/jobs/dequeue") {
      if (req.method !== "POST") return json({ error: "method_not_allowed", requestId }, { status: 405 });
      const res = await forward("https://do/dequeue");
      console.log(JSON.stringify({ requestId, path: url.pathname, ok: res.ok }));
      return res;
    }

    if (url.pathname === "/v1/jobs/complete") {
      if (req.method !== "POST") return json({ error: "method_not_allowed", requestId }, { status: 405 });
      return forward("https://do/complete");
    }

    if (url.pathname === "/v1/jobs/fail") {
      if (req.method !== "POST") return json({ error: "method_not_allowed", requestId }, { status: 405 });
      return forward("https://do/fail");
    }

    if (url.pathname === "/v1/jobs/get") {
      if (req.method !== "GET") return json({ error: "method_not_allowed", requestId }, { status: 405 });
      const id = url.searchParams.get("id");
      if (!id) return json({ error: "missing_id", requestId }, { status: 400 });
      return forward(`https://do/get?id=${encodeURIComponent(id)}`);
    }

    if (url.pathname === "/v1/jobs/stats") {
      if (req.method !== "GET") return json({ error: "method_not_allowed", requestId }, { status: 405 });
      return forward("https://do/stats");
    }

    if (url.pathname === "/v1/jobs/purge") {
      if (req.method !== "POST") return json({ error: "method_not_allowed", requestId }, { status: 405 });
      return forward("https://do/purge");
    }

    if (url.pathname === "/v1/jobs/list") {
      if (req.method !== "GET") return json({ error: "method_not_allowed", requestId }, { status: 405 });

      const status = url.searchParams.get("status");
      const limit = url.searchParams.get("limit");
      const cursor = url.searchParams.get("cursor");

      const qs = new URLSearchParams();
      if (status) qs.set("status", status);
      if (limit) qs.set("limit", limit);
      if (cursor) qs.set("cursor", cursor);

      const doUrl = qs.toString() ? `https://do/list?${qs.toString()}` : "https://do/list";
      return forward(doUrl);
    }

    return new Response("not found", { status: 404, headers: { "x-request-id": requestId } });
  },
};
