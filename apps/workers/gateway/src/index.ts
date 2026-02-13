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

// Routes
const PUBLIC_PATHS = new Set<string>(["/health"]);
const INTERNAL_PATHS = new Set<string>(["/v1/ping"]);

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);

    // 1) Public routes
    if (PUBLIC_PATHS.has(url.pathname)) {
      // Saat ini cuma /health
      return new Response(
        JSON.stringify({ ok: true, service: "rankpublic-gateway", env: env.ENVIRONMENT }),
        { headers: { "content-type": "application/json" } }
      );
    }

    // 2) Internal routes (explicit allowlist)
    if (INTERNAL_PATHS.has(url.pathname)) {
      const authErr = requireAuth(req, env);
      if (authErr) return authErr;

      if (url.pathname === "/v1/ping") return new Response("pong");
    }

    // 3) Everything else is OFF
    return new Response("not found", { status: 404 });
  },
};
