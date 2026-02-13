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

async function dequeue(env: Env): Promise<Job | null> {
  const res = await fetch(`${env.GATEWAY_BASE_URL}/v1/jobs/dequeue`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${env.INTERNAL_API_TOKEN}`,
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`dequeue_failed status=${res.status} body=${text}`);
  }

  const data = (await res.json()) as { ok: boolean; job: Job | null };
  return data.job ?? null;
}

export default {
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    const max = Math.max(1, Math.min(50, parseInt(env.MAX_JOBS_PER_TICK || "10", 10) || 10));
    ctx.waitUntil(
      (async () => {
        for (let i = 0; i < max; i++) {
          const job = await dequeue(env);
          if (!job) {
            console.log(JSON.stringify({ msg: "no_jobs" }));
            return;
          }

          // For now: just prove consumption works
          console.log(JSON.stringify({ msg: "processing_job", job }));

          // TODO next: actually run crawl/rank and store results
        }
        console.log(JSON.stringify({ msg: "tick_done", processed: max }));
      })()
    );
  },
};
