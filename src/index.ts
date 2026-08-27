import { DurableObject } from "cloudflare:workers";

// ==========================================================
// 1. Durable Object Class (Global State Management)
// ==========================================================
export class TunnelHub {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.activeTunnels = new Map();     // tunnelId -> WebSocket
    this.dynamicRouteTable = new Map(); // routePath -> tunnelId
    this.pendingRequests = new Map();   // requestId -> { resolve, reject, timeoutId }
  }

  async fetch(request) {
    const url = new URL(request.url);

    // ------------------------------------------------------
    // 1.1 รับการเชื่อมต่อ WebSocket จาก C++ Tunnel Client
    // ------------------------------------------------------
    if (url.pathname === "/_tunnel") {
      const upgradeHeader = request.headers.get("Upgrade");
      if (upgradeHeader !== "websocket") {
        return new Response("Expected Upgrade: websocket", { status: 426 });
      }

      const pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];
      server.accept();

      let registeredTunnelId = null;

      server.addEventListener("message", async (event) => {
        try {
          const data = JSON.parse(event.data);

          // จัดการการลงทะเบียน Route จาก C++ Tunnel
          if (data.type === "register") {
            const token = data.secret_token;
            const expectedToken = this.env.TUNNEL_SECRET || "super_secret_hash_123";
            
            if (token !== expectedToken) {
              server.send(JSON.stringify({ type: "error", message: "Unauthorized" }));
              server.close(1008, "Unauthorized");
              return;
            }

            registeredTunnelId = data.tunnelId;
            this.activeTunnels.set(registeredTunnelId, server);

            if (Array.isArray(data.routes)) {
              for (const route of data.routes) {
                this.dynamicRouteTable.set(route, registeredTunnelId);
              }
            }
            console.log(`[DO Tunnel Registered] ID: ${registeredTunnelId}, Routes:`, data.routes);
          }
          
          // จัดการ Response ที่ C++ ส่งกลับมาเพื่อตอบสนอง Browser
          else if (data.type === "http_response") {
            const reqId = data.requestId;
            if (this.pendingRequests.has(reqId)) {
              const { resolve, timeoutId } = this.pendingRequests.get(reqId);
              clearTimeout(timeoutId);
              this.pendingRequests.delete(reqId);

              // 🛠️ แก้ไขจุดนี้: แปลง Header จาก C++ ให้ให้อยู่ในรูปแบบที่ Response ของ Workers ยอมรับ
              const rawHeaders = data.headers || { "Content-Type": "text/html; charset=UTF-8" };
              const responseHeaders = new Headers();
              for (const [key, value] of Object.entries(rawHeaders)) {
                responseHeaders.set(key, value);
              }

              resolve(new Response(data.body, {
                status: data.status || 200,
                headers: responseHeaders // ส่ง Header ที่ถูกต้องกลับหา Browser
              }));
            }
          }
        } catch (err) {
          console.error("DO Message Parse Error:", err);
        }
      });

      const cleanup = () => {
        if (registeredTunnelId) {
          this.activeTunnels.delete(registeredTunnelId);
          for (const [path, tId] of this.dynamicRouteTable.entries()) {
            if (tId === registeredTunnelId) {
              this.dynamicRouteTable.delete(path);
            }
          }
          console.log(`[DO Tunnel Disconnected] ID: ${registeredTunnelId}`);
        }
      };

      server.addEventListener("close", cleanup);
      server.addEventListener("error", cleanup);

      return new Response(null, { status: 101, webSocket: client });
    }

    // ------------------------------------------------------
    // 1.2 รับ HTTP Request จาก Worker หลัก แล้วส่งผ่าน WebSocket
    // ------------------------------------------------------
    if (url.pathname === "/_proxy") {
      const reqData = await request.json();
      const pathname = reqData.pathname;

      // ค้นหา Route แบบ Prefix Matching (เรียงจากยาวไปสั้น)
      let targetTunnelId = null;
      const sortedRoutes = Array.from(this.dynamicRouteTable.keys()).sort((a, b) => b.length - a.length);
      for (const route of sortedRoutes) {
        if (pathname.startsWith(route)) {
          targetTunnelId = this.dynamicRouteTable.get(route);
          break;
        }
      }

      if (!targetTunnelId) {
        return new Response(JSON.stringify({
          error: "Not Found: No active tunnel handles this route.",
          diagnostics: {
            activeTunnelsCount: this.activeTunnels.size,
            registeredRoutes: Array.from(this.dynamicRouteTable.keys()),
            requestedPath: pathname
          }
        }), { status: 404, headers: { "Content-Type": "application/json" } });
      }

      const tunnelWs = this.activeTunnels.get(targetTunnelId);
      if (!tunnelWs || tunnelWs.readyState !== 1) {
        return new Response(JSON.stringify({ error: `Bad Gateway: Tunnel '${targetTunnelId}' is offline.` }), { status: 502 });
      }

      // สร้าง Promise รอรับค่า Response ขา戻กลับจาก C++
      return new Promise(async (resolve) => {
        const requestId = reqData.requestId;
        
        const timeoutId = setTimeout(() => {
          if (this.pendingRequests.has(requestId)) {
            this.pendingRequests.delete(requestId);
            resolve(new Response(JSON.stringify({ error: "Gateway Timeout: Local C++ server did not respond." }), { status: 504 }));
          }
        }, 30000);

        this.pendingRequests.set(requestId, { resolve, timeoutId });

        const payload = {
          type: "http_request",
          requestId: requestId,
          method: reqData.method,
          path: pathname + reqData.search,
          headers: reqData.headers,
          body: reqData.body
        };

        try {
          tunnelWs.send(JSON.stringify(payload));
        } catch (err) {
          clearTimeout(timeoutId);
          this.pendingRequests.delete(requestId);
          resolve(new Response(JSON.stringify({ error: "Internal Server Error: Websocket write failed." }), { status: 500 }));
        }
      });
    }

    return new Response("Not found", { status: 404 });
  }
}

// ==========================================================
// 2. Worker Entrypoint (Global Router)
// ==========================================================
export default {
  async fetch(request, env, ctx) {
    // ดึง Durable Object Instance ออกมา (ใช้ ID คงที่ เพื่อให้ทุก Edge Node ชี้มาที่ Hub ตัวเดียวกัน)
    const id = env.TUNNEL_HUB.idFromName("global_tunnel_hub");
    const stub = env.TUNNEL_HUB.get(id);

    const url = new URL(request.url);

    // หากเป็นการเชื่อมต่อ Tunnel หรือ Request Proxy ให้ส่งต่อไปยัง Durable Object ทั้งหมด
    if (url.pathname === "/_tunnel") {
      return stub.fetch(request);
    }

    // สำหรับ Request ทั่วไปจาก Browser นำมาแปลงร่างแล้วส่งเข้า Durable Object
    let bodyData = null;
    if (request.method !== "GET" && request.method !== "HEAD") {
      bodyData = await request.text();
    }

    const proxyRequestData = {
      method: request.method,
      pathname: url.pathname,
      search: url.search,
      headers: Object.fromEntries(request.headers.entries()),
      body: bodyData,
      requestId: crypto.randomUUID()
    };

    // ส่งต่อไปยัง DO ผ่าน Internal Fetch
    const doResponse = await stub.fetch(new Request("https://internal/_proxy", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(proxyRequestData)
    }));

    return doResponse;
  }
};
