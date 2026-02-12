export interface Env {
  ENVIRONMENT: string;
  INTERNAL_API_TOKEN: string;
  NONCE_GUARD: DurableObjectNamespace;
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

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);

    if (url.pathname === "/health") {
      return new Response(
        JSON.stringify({ ok: true, service: "rankpublic-gateway", env: env.ENVIRONMENT }),
        { headers: { "content-type": "application/json" } }
      );
    }

    const authErr = requireAuth(req, env);
    if (authErr) return authErr;

    if (url.pathname === "/do") {
      const id = env.NONCE_GUARD.idFromName("gateway");
      const stub = env.NONCE_GUARD.get(id);
      return stub.fetch("https://do/");
    }

    return new Response("not found", { status: 404 });
  },
};
