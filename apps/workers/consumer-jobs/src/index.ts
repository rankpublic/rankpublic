export interface Env {
  INTERNAL_API_TOKEN: string;
  GATEWAY_BASE_URL: string;
  MAX_JOBS_PER_TICK: string;
}

type Job = {
  id: string;
  type: "crawl" | "rank";
  target: string;
  createdAt: number;
};

async function postJson(env: Env, path: string, body: unknown) {
  const res = await fetch(`${env.GATEWAY_BASE_URL}${path}`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${env.INTERNAL_API_TOKEN}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`request_failed path=${path} status=${res.status} body=${text}`);
  }

  return res;
}

async function dequeue(env: Env): Promise<Job | null> {
  const res = await fetch(`${env.GATEWAY_BASE_URL}/v1/jobs/dequeue`, {
    method: "POST",
    headers: { authorization: `Bearer ${env.INTERNAL_API_TOKEN}` },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`dequeue_failed status=${res.status} body=${text}`);
  }

  const data = (await res.json()) as { ok: boolean; job: Job | null };
  return data.job ?? null;
}

function clampIntFromString(s: string | undefined, fallback: number, min: number, max: number) {
  const n = parseInt(s || "", 10);
  const v = Number.isFinite(n) ? n : fallback;
  return Math.max(min, Math.min(max, v));
}

async function processJob(env: Env, job: Job) {
  if (job.type === "crawl") {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), 15_000);

    try {
      const startedAt = Date.now();
      const res = await fetch(job.target, {
        method: "GET",
        redirect: "follow",
        signal: controller.signal,
      });
      const finishedAt = Date.now();

      // Key change: non-2xx is FAIL (so DO retry kicks in)
      if (!res.ok) {
        await postJson(env, "/v1/jobs/fail", {
          id: job.id,
          error: `crawl_http_error status=${res.status} finalUrl=${res.url} durationMs=${finishedAt - startedAt}`,
        });
        return;
      }

      await postJson(env, "/v1/jobs/complete", {
        id: job.id,
        result: {
          kind: "crawl",
          status: res.status,
          finalUrl: res.url,
          durationMs: finishedAt - startedAt,
        },
      });
      return;
    } catch (err) {
      await postJson(env, "/v1/jobs/fail", {
        id: job.id,
        error: `crawl_failed: ${String(err)}`,
      });
      return;
    } finally {
      clearTimeout(t);
    }
  }

  await postJson(env, "/v1/jobs/fail", {
    id: job.id,
    error: `unsupported_type: ${job.type}`,
  });
}

export default {
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    const max = clampIntFromString(env.MAX_JOBS_PER_TICK, 10, 1, 50);

    ctx.waitUntil(
      (async () => {
        for (let i = 0; i < max; i++) {
          const job = await dequeue(env);
          if (!job) {
            console.log(JSON.stringify({ msg: "no_jobs" }));
            return;
          }

          console.log(JSON.stringify({ msg: "processing_job", jobId: job.id, type: job.type, target: job.target }));

          try {
            await processJob(env, job);
            console.log(JSON.stringify({ msg: "processed_job", jobId: job.id }));
          } catch (err) {
            // If postJson throws (gateway down, etc), we at least log it.
            // The job will reappear after lease expiry because we didn't complete it.
            console.log(JSON.stringify({ msg: "process_job_throw", jobId: job.id, error: String(err) }));
          }
        }

        console.log(JSON.stringify({ msg: "tick_done", processed: max }));
      })()
    );
  },
};
