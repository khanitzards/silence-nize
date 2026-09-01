import { DurableObject } from "cloudflare:workers";

export class TunnelHub extends DurableObject {
  constructor(state, env) {
    super(state, env);
    this.state = state;
    this.env = env;
    this.activeTunnels = new Map();     // tunnelId -> WebSocket
    this.dynamicRouteTable = new Map(); // routePath -> tunnelId
    this.pendingRequests = new Map();   // requestId -> { resolve, reject, timeoutId, tunnelId }
  }

  async fetch(request) {
    const url = new URL(request.url);

    // ------------------------------------------------------
    // 1.1 WebSocket จาก C++ Tunnel Client
    // ------------------------------------------------------
    if (url.pathname === "/_tunnel") {
      const upgradeHeader = request.headers.get("Upgrade");
      if (upgradeHeader !== "websocket") {
        return new Response("Expected Upgrade: websocket", { status: 426 });
      }

      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      server.accept();

      let registeredTunnelId = null;

      server.addEventListener("message", async (event) => {
        try {
          const data = JSON.parse(event.data);

          // ===== REGISTER =====
          if (data.type === "register") {
            const token = data.secret_token;
            if (token !== this.env.TUNNEL_SECRET) {
              server.send(JSON.stringify({ type: "error", message: "Unauthorized" }));
              server.close(1008, "Unauthorized");
              return;
            }

            // ลบของเก่าถ้ามี tunnelId เดิม (กรณี reconnect)
            if (registeredTunnelId) {
              this._cleanupTunnel(registeredTunnelId);
            }

            registeredTunnelId = data.tunnelId;
            this.activeTunnels.set(registeredTunnelId, server);

            if (Array.isArray(data.routes)) {
              for (const route of data.routes) {
                this.dynamicRouteTable.set(route, registeredTunnelId);
              }
            }

            console.log(`[DO] Tunnel Registered → ${registeredTunnelId}`, data.routes);
            server.send(JSON.stringify({ type: "registered", tunnelId: registeredTunnelId }));
          }

          // ===== HTTP RESPONSE จาก C++ =====
          else if (data.type === "http_response") {
            const reqId = data.requestId;
            if (!this.pendingRequests.has(reqId)) return;

            const { resolve, timeoutId } = this.pendingRequests.get(reqId);
            clearTimeout(timeoutId);
            this.pendingRequests.delete(reqId);

            const responseHeaders = new Headers();
            const rawHeaders = data.headers || {};

            for (const [key, value] of Object.entries(rawHeaders)) {
              if (key.toLowerCase() === "set-cookie") {
                if (Array.isArray(value)) {
                  value.forEach(c => responseHeaders.append("Set-Cookie", c));
                } else {
                  responseHeaders.append("Set-Cookie", value);
                }
              } else {
                if (Array.isArray(value)) {
                  value.forEach(v => responseHeaders.append(key, v));
                } else {
                  responseHeaders.set(key, value);
                }
              }
            }

            // ถอด Base64 → Binary
            let body = null;
            if (data.bodyBase64) {
              const binary = atob(data.bodyBase64);
              const bytes = new Uint8Array(binary.length);
              for (let i = 0; i < binary.length; i++) {
                bytes[i] = binary.charCodeAt(i);
              }
              body = bytes;
            } else if (data.body) {
              body = data.body;
            }

            resolve(new Response(body, {
              status: data.status || 200,
              headers: responseHeaders
            }));
          }
        } catch (err) {
          console.error("[DO] Message error:", err);
        }
      });

      const cleanup = () => {
        if (registeredTunnelId) {
          this._cleanupTunnel(registeredTunnelId);
          console.log(`[DO] Tunnel Disconnected → ${registeredTunnelId}`);
        }
      };

      server.addEventListener("close", cleanup);
      server.addEventListener("error", cleanup);

      return new Response(null, { status: 101, webSocket: client });
    }

    // ------------------------------------------------------
    // 1.2 Proxy HTTP Request จาก Worker หลัก
    // ------------------------------------------------------
    if (url.pathname === "/_proxy") {
      const reqData = await request.json();
      const pathname = reqData.pathname;

      // Prefix matching (ยาวที่สุดก่อน)
      let targetTunnelId = null;
      const sortedRoutes = Array.from(this.dynamicRouteTable.keys())
        .sort((a, b) => b.length - a.length);

      for (const route of sortedRoutes) {
        const clean = route.endsWith("*") ? route.slice(0, -1) : route;
        if (pathname.startsWith(clean)) {
          targetTunnelId = this.dynamicRouteTable.get(route);
          break;
        }
      }

      if (!targetTunnelId) {
        return new Response(JSON.stringify({
          error: "Not Found: No active tunnel handles this route",
          path: pathname
        }), { status: 404, headers: { "Content-Type": "application/json" } });
      }

      const tunnelWs = this.activeTunnels.get(targetTunnelId);
      if (!tunnelWs || tunnelWs.readyState !== 1) {
        return new Response(JSON.stringify({
          error: `Bad Gateway: Tunnel '${targetTunnelId}' offline`
        }), { status: 502 });
      }

      return new Promise((resolve) => {
        const requestId = reqData.requestId;

        const timeoutId = setTimeout(() => {
          if (this.pendingRequests.has(requestId)) {
            this.pendingRequests.delete(requestId);
            resolve(new Response(JSON.stringify({
              error: "Gateway Timeout"
            }), { status: 504 }));
          }
        }, 30000);

        this.pendingRequests.set(requestId, {
          resolve,
          timeoutId,
          tunnelId: targetTunnelId
        });

        // กรอง hop-by-hop headers
        const safeHeaders = {};
        const hopByHop = new Set([
          "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
          "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length"
        ]);

        for (const [k, v] of Object.entries(reqData.headers || {})) {
          if (!hopByHop.has(k.toLowerCase())) {
            safeHeaders[k] = v;
          }
        }

        const payload = {
          type: "http_request",
          requestId,
          method: reqData.method,
          path: pathname + (reqData.search || ""),
          headers: safeHeaders,
          body: reqData.body || null,
          bodyBase64: reqData.bodyBase64 || null   // รองรับ binary request
        };

        try {
          tunnelWs.send(JSON.stringify(payload));
        } catch (err) {
          clearTimeout(timeoutId);
          this.pendingRequests.delete(requestId);
          resolve(new Response(JSON.stringify({
            error: "Websocket write failed"
          }), { status: 500 }));
        }
      });
    }

    return new Response("Not found", { status: 404 });
  }

  // ลบ tunnel + reject pending requests ที่เกี่ยวข้อง
  _cleanupTunnel(tunnelId) {
    this.activeTunnels.delete(tunnelId);

    for (const [path, tId] of this.dynamicRouteTable.entries()) {
      if (tId === tunnelId) this.dynamicRouteTable.delete(path);
    }

    // reject pending ที่ผูกกับ tunnel นี้
    for (const [reqId, pending] of this.pendingRequests.entries()) {
      if (pending.tunnelId === tunnelId) {
        clearTimeout(pending.timeoutId);
        pending.resolve(new Response(JSON.stringify({
          error: "Tunnel disconnected"
        }), { status: 502 }));
        this.pendingRequests.delete(reqId);
      }
    }
  }
}

// ==========================================================
// Worker Entrypoint
// ==========================================================
export default {
  async fetch(request, env, ctx) {
    const id = env.TUNNEL_HUB.idFromName("global_tunnel_hub");
    const stub = env.TUNNEL_HUB.get(id);
    const url = new URL(request.url);

    if (url.pathname === "/_tunnel") {
      return stub.fetch(request);
    }

    // เตรียม body (รองรับ binary)
    let bodyText = null;
    let bodyBase64 = null;

    if (request.method !== "GET" && request.method !== "HEAD") {
      const buf = await request.arrayBuffer();
      if (buf.byteLength > 0) {
        // ถ้าเป็น text-like ก็ส่ง text, ไม่งั้น base64
        const contentType = request.headers.get("content-type") || "";
        if (contentType.startsWith("text/") ||
            contentType.includes("json") ||
            contentType.includes("xml") ||
            contentType.includes("javascript")) {
          bodyText = new TextDecoder().decode(buf);
        } else {
          // binary → base64
          const bytes = new Uint8Array(buf);
          let binary = "";
          for (let i = 0; i < bytes.length; i++) {
            binary += String.fromCharCode(bytes[i]);
          }
          bodyBase64 = btoa(binary);
        }
      }
    }

    const proxyRequestData = {
      method: request.method,
      pathname: url.pathname,
      search: url.search,
      headers: Object.fromEntries(request.headers.entries()),
      body: bodyText,
      bodyBase64: bodyBase64,
      requestId: crypto.randomUUID()
    };

    return stub.fetch(new Request("https://internal/_proxy", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(proxyRequestData)
    }));
  }
};
