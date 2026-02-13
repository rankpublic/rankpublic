export interface Env {
  ENVIRONMENT: string;
  INTERNAL_API_TOKEN: string;
  NONCE_GUARD: DurableObjectNamespace;
  JOB_QUEUE: DurableObjectNamespace;
}

type EnqueueBody = {
  type: "crawl" | "rank";
  target: string;
};

type Job = {
  id: string;
  type: "crawl" | "rank";
  target: string;
  createdAt: number;
};

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

export class NonceGuard {
  constructor(private state: DurableObjectState) {}
  async fetch(): Promise<Response> {
    return new Response("nonce-guard ok");
  }
}

export class JobQueue {
  constructor(private state: DurableObjectState) {}

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);

    // SQLite-backed DO exposes SQL API at state.storage.sql
    const sql = (this.state as any).storage.sql;

    sql.exec(`
      CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        target TEXT NOT NULL,
        createdAt INTEGER NOT NULL
      );
    `);

    if (url.pathname === "/enqueue" && req.method === "POST") {
      const job = (await req.json()) as Partial<Job>;

      const id = typeof job.id === "string" ? job.id : "";
      const type = job.type === "crawl" || job.type === "rank" ? job.type : null;
      const target = typeof job.target === "string" ? job.target : "";
      const createdAt = typeof job.createdAt === "number" ? job.createdAt : null;

      if (!id || !type || !target || createdAt === null) {
        return new Response("invalid job payload", { status: 400 });
      }

      // IMPORTANT: bindings are variadic, not an array
      sql.exec(
        "INSERT INTO jobs (id, type, target, createdAt) VALUES (?, ?, ?, ?)",
        id,
        type,
        target,
        createdAt
      );

      return new Response("ok");
    }

    if (url.pathname === "/dequeue" && req.method === "POST") {
      const row = sql
        .exec("SELECT id, type, target, createdAt FROM jobs ORDER BY createdAt ASC LIMIT 1")
        .toArray()[0] as Job | undefined;

      if (!row) {
        return json({ ok: true, job: null });
      }

      sql.exec("DELETE FROM jobs WHERE id = ?", row.id);
      return json({ ok: true, job: row });
    }

    return new Response("not found", { status: 404 });
  }
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const requestId = getRequestId(req);

    // Public
    if (url.pathname === "/health") {
      console.log(JSON.stringify({ requestId, path: url.pathname, ok: true }));
      return json(
        { ok: true, service: "rankpublic-gateway", env: env.ENVIRONMENT, requestId },
        { status: 200, headers: { "x-request-id": requestId } }
      );
    }

    // Step A: Drop browser noise fast
    // Any GET (/, /favicon.ico, /js/*, /css/*, etc.) => 404 (no further routing)
    if (req.method === "GET") {
      return new Response("not found", { status: 404, headers: { "x-request-id": requestId } });
    }

    // Internal: /v1/ping
    if (url.pathname === "/v1/ping") {
      const authErr = requireAuth(req, env);
      if (authErr) return authErr;

      console.log(JSON.stringify({ requestId, path: url.pathname, ok: true }));
      return new Response("pong", { headers: { "x-request-id": requestId } });
    }

    // Internal: POST /v1/jobs/enqueue
    if (url.pathname === "/v1/jobs/enqueue") {
      const authErr = requireAuth(req, env);
      if (authErr) return authErr;

      if (req.method !== "POST") {
        return json(
          { error: "method_not_allowed", requestId },
          { status: 405, headers: { "x-request-id": requestId } }
        );
      }

      let body: EnqueueBody;
      try {
        body = (await req.json()) as EnqueueBody;
      } catch {
        return json(
          { error: "invalid_json", requestId },
          { status: 400, headers: { "x-request-id": requestId } }
        );
      }

      if (!body?.type || !body?.target || typeof body.target !== "string") {
        return json(
          { error: "invalid_body", requestId },
          { status: 400, headers: { "x-request-id": requestId } }
        );
      }

      const job: Job = {
        id: crypto.randomUUID(),
        type: body.type,
        target: body.target,
        createdAt: Date.now(),
      };

      const doId = env.JOB_QUEUE.idFromName("main");
      const stub = env.JOB_QUEUE.get(doId);

      try {
        await stub.fetch("https://do/enqueue", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(job),
        });
      } catch (err) {
        console.log(JSON.stringify({ requestId, path: url.pathname, enqueueError: String(err) }));
        return json(
          { ok: false, requestId, error: "enqueue_failed" },
          { status: 500, headers: { "x-request-id": requestId } }
        );
      }

      console.log(JSON.stringify({ requestId, path: url.pathname, enqueued: true, job }));

      return json(
        { ok: true, requestId, accepted: true, job },
        { status: 202, headers: { "x-request-id": requestId } }
      );
    }

    // Internal: POST /v1/jobs/dequeue
    if (url.pathname === "/v1/jobs/dequeue") {
      const authErr = requireAuth(req, env);
      if (authErr) return authErr;

      if (req.method !== "POST") {
        return json(
          { error: "method_not_allowed", requestId },
          { status: 405, headers: { "x-request-id": requestId } }
        );
      }

      const doId = env.JOB_QUEUE.idFromName("main");
      const stub = env.JOB_QUEUE.get(doId);

      const res = await stub.fetch("https://do/dequeue", { method: "POST" });
      const text = await res.text();

      console.log(JSON.stringify({ requestId, path: url.pathname, ok: res.ok }));

      return new Response(text, {
        status: res.status,
        headers: { "content-type": "application/json", "x-request-id": requestId },
      });
    }

    // Everything else is OFF
    return new Response("not found", { status: 404, headers: { "x-request-id": requestId } });
  },
};
