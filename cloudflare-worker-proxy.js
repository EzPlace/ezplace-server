/**
 * EzPlace backup proxy — Cloudflare Worker
 * ----------------------------------------
 * Forwards ALL HTTP requests and WebSocket connections to the real Render
 * backend. Use this when the Render domain is blocked on a network (e.g. a
 * school filter): the client probes /api/health, and if the primary host
 * fails it routes everything through this Worker's *.workers.dev domain
 * instead — a different domain that filters usually don't block.
 *
 * Cost: free. Cloudflare's free Workers plan covers 100k requests/day, and
 * WebSocket messages don't each count as a request (the upgrade does). No
 * data is stored in the Worker; it's a pass-through pipe.
 *
 * ── Deploy steps ──────────────────────────────────────────────────────────
 * 1. Make a free account at https://dash.cloudflare.com
 * 2. Workers & Pages  ->  Create  ->  Create Worker.
 * 3. Name it e.g. "ezplace-proxy"  ->  Deploy  ->  Edit code.
 * 4. Replace the entire default file with the contents of THIS file. Deploy.
 * 5. Your proxy URL is shown, e.g.  ezplace-proxy.YOURNAME.workers.dev
 * 6. In index.html, find BACKEND_CANDIDATES and uncomment / set that host:
 *        const BACKEND_CANDIDATES = [
 *          'ezplace-server.onrender.com',
 *          'ezplace-proxy.YOURNAME.workers.dev',
 *        ];
 *    Push index.html (and sync the GitHub Pages mirror). Done — failover is
 *    automatic; nothing else to configure.
 *
 * Tip: deploy two or three Workers under different account names/subdomains
 * and list them all in BACKEND_CANDIDATES for extra resilience.
 */

const TARGET_HOST = 'ezplace-server.onrender.com';

export default {
  async fetch(request) {
    const url = new URL(request.url);
    url.protocol = 'https:';
    url.hostname = TARGET_HOST;
    url.port = '';

    // Rebuild the request against the target. Passing the original `request`
    // as init preserves method, body, headers, and — crucially — the
    // `Upgrade: websocket` header, so WebSocket connections tunnel through.
    const proxied = new Request(url.toString(), request);
    proxied.headers.set('Host', TARGET_HOST);

    const resp = await fetch(proxied);

    // For WebSocket upgrades Cloudflare returns a 101 response carrying the
    // `webSocket` handle; returning it as-is completes the tunnel.
    if (resp.webSocket) {
      return new Response(null, {
        status: resp.status,
        statusText: resp.statusText,
        headers: resp.headers,
        webSocket: resp.webSocket,
      });
    }

    // Normal HTTP: stream the response back. The backend already sends
    // Access-Control-Allow-Origin: * so CORS keeps working through the proxy.
    const out = new Response(resp.body, resp);
    out.headers.set('Access-Control-Allow-Origin', '*');
    return out;
  },
};
