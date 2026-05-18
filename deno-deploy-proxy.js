/**
 * EzPlace backup proxy — Deno Deploy
 * ----------------------------------
 * Same purpose as cloudflare-worker-proxy.js: tunnels ALL HTTP requests and
 * WebSocket connections to the real Render backend, on a different domain
 * (*.deno.dev) so a network filter that blocks the Render host can be
 * bypassed. Free, no storage, pure pass-through.
 *
 * Use this if the Cloudflare dashboard keeps 403-ing your deploy. Deno Deploy
 * signs in with GitHub, so there's no email-verification step to get stuck on.
 *
 * ── Deploy steps ──────────────────────────────────────────────────────────
 * 1. Go to https://dash.deno.com  ->  sign in with GitHub.
 * 2. New Project  ->  choose "Playground" (instant, no repo needed).
 * 3. Delete the sample code, paste THIS entire file, click Save & Deploy.
 * 4. It gives you a URL like  https://ezplace-proxy-abc123.deno.dev
 *    Your proxy HOST is the part after https://  (e.g. ezplace-proxy-abc123.deno.dev)
 * 5. In index.html, add that host to BACKEND_CANDIDATES:
 *        const BACKEND_CANDIDATES = [
 *          'ezplace-server.onrender.com',
 *          'ezplace-proxy-abc123.deno.dev',
 *        ];
 *    Push index.html + sync the GitHub Pages mirror. Failover is automatic.
 *
 * (Send me the .deno.dev URL and I'll wire it into BACKEND_CANDIDATES + push.)
 */

const TARGET = "ezplace-server.onrender.com";

// WebSocket close codes outside 1000 / 3000-4999 throw if you pass them along.
function safeClose(sock, code, reason) {
  try {
    if (code === 1000 || (code >= 3000 && code <= 4999)) sock.close(code, reason);
    else sock.close();
  } catch { /* already closing/closed */ }
}

Deno.serve(async (req) => {
  const url = new URL(req.url);

  // ── WebSocket tunnel ───────────────────────────────────────────────────
  if ((req.headers.get("upgrade") || "").toLowerCase() === "websocket") {
    const { socket: client, response } = Deno.upgradeWebSocket(req);
    const upstream = new WebSocket("wss://" + TARGET + url.pathname + url.search);
    client.binaryType = "arraybuffer";
    upstream.binaryType = "arraybuffer";

    const pending = [];
    let upOpen = false;

    upstream.onopen = () => { upOpen = true; for (const m of pending) upstream.send(m); pending.length = 0; };
    upstream.onmessage = (e) => { try { client.send(e.data); } catch { /* client gone */ } };
    upstream.onclose = (e) => safeClose(client, e.code, e.reason);
    upstream.onerror = () => safeClose(client);

    client.onmessage = (e) => { if (upOpen) { try { upstream.send(e.data); } catch {} } else pending.push(e.data); };
    client.onclose = (e) => safeClose(upstream, e.code, e.reason);
    client.onerror = () => safeClose(upstream);

    return response;
  }

  // ── Plain HTTP ─────────────────────────────────────────────────────────
  // Buffer the body (uploads are <=6MB) so this works across runtime versions.
  const method = req.method;
  let body;
  if (method !== "GET" && method !== "HEAD") body = await req.arrayBuffer();

  const headers = new Headers(req.headers);
  headers.delete("host"); // let fetch derive it from the target URL

  let resp;
  try {
    resp = await fetch("https://" + TARGET + url.pathname + url.search, {
      method, headers, body, redirect: "manual",
    });
  } catch {
    return new Response("proxy upstream error", { status: 502 });
  }

  const out = new Headers(resp.headers);
  out.set("Access-Control-Allow-Origin", "*");
  return new Response(resp.body, { status: resp.status, statusText: resp.statusText, headers: out });
});
