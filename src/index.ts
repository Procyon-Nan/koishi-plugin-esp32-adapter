import { Bot, Context, Schema, Universal, h } from "koishi";
import { randomUUID } from "crypto";
import type {} from "@koishijs/plugin-server";

export const name = "esp32-adapter";

export const inject = {
  required: ["server"],
};

const PROTOCOL_NAME = "Utsuho";
const PROTOCOL_VERSION = 1;
const HEARTBEAT_MS = 30000;
const WS_PATH = "/esp32";
const HANDSHAKE_TIMEOUT_MS = 10000;

const CAPABILITY_LIST = [
  "audio_tx",
  "audio_rx",
  "text_tx",
  "text_rx",
  "image_tx",
  "image_rx",
] as const;

type Capability = (typeof CAPABILITY_LIST)[number];
type PeerRole = "device" | "server";
type MessageType =
  | "hello"
  | "hello_ack"
  | "heartbeat"
  | "command"
  | "response"
  | "event"
  | "stream_open"
  | "stream_chunk_meta"
  | "stream_close"
  | "error";

interface Config {
  wsHost: string;
  wsPort: number;
  serverId: string;
  audioTxEnable: boolean;
  audioRxEnable: boolean;
  textTxEnable: boolean;
  textRxEnable: boolean;
  imageTxEnable: boolean;
  imageRxEnable: boolean;
}

interface UtsuhoMessage {
  v: number;
  type: MessageType;
  action: string;
  id: string;
  session_id: string;
  device_id: string;
  peer_role: PeerRole;
  ts: number;
  payload: Record<string, any>;
  reply_to?: string;
}

interface AudioStreamSnapshot {
  streamId: string;
  mediaKind: string;
  codec: string;
  sampleRate: number | null;
  channels: number | null;
  lastSeq: number | null;
  lastLength: number | null;
  lastChunkAt: number | null;
  loopbackActive: boolean;
  bufferedChunkCount: number;
  bufferedBytes: number;
}

interface SessionState {
  sessionId: string | null;
  remoteDeviceId: string | null;
  remoteRole: PeerRole | null;
  handshakeDone: boolean;
  declaredCapabilities: Capability[];
  effectiveCapabilities: Capability[];
  lastHeartbeatAt: number | null;
  pendingBinaryMeta: UtsuhoMessage | null;
  audioStream: AudioStreamSnapshot | null;
  audioLoopbackChunks: Buffer[];
  messageCounter: number;
  handshakeTimer: NodeJS.Timeout | null;
}

interface SessionSnapshot {
  sessionId: string | null;
  remoteDeviceId: string | null;
  remoteRole: PeerRole | null;
  handshakeDone: boolean;
  declaredCapabilities: Capability[];
  effectiveCapabilities: Capability[];
  lastHeartbeatAt: number | null;
  audioStream: AudioStreamSnapshot | null;
}

interface LiveSessionEntry {
  socket: any;
  session: SessionState;
}

const PRIVATE_PFX = "private:";

// DeviceBot：使用真实 Koishi Bot 承载设备消息与回复派发。
class DeviceBot extends Bot<Context, any> {
  constructor(
    ctx: Context,
    private readonly _liveSessions: Map<string, LiveSessionEntry>,
    private readonly _serverConfig: Config,
  ) {
    super(ctx, {} as any, "device");
    this.selfId = _serverConfig.serverId;
    this.user.id = _serverConfig.serverId;
    this.user.name = "Device Bridge";
  }

  async createDirectChannel(userId: string) {
    return {
      id: `${PRIVATE_PFX}${userId}`,
      type: Universal.Channel.Type.DIRECT,
    };
  }

  async sendMessage(channelId: string, content: any) {
    const deviceId = channelId.startsWith(PRIVATE_PFX)
      ? channelId.slice(PRIVATE_PFX.length)
      : channelId;

    const entry = this._liveSessions.get(deviceId);
    if (!entry?.session.handshakeDone) return [randomUUID()];
    if (!entry.session.effectiveCapabilities.includes("text_tx")) {
      return [randomUUID()];
    }

    const text = extractPlainText(h.normalize(content));
    if (!text) return [randomUUID()];

    sendMessage(
      entry.socket,
      buildMessage(
        entry.session,
        this._serverConfig.serverId,
        "server",
        "command",
        "text_send",
        { text },
      ),
    );

    return [randomUUID()];
  }

  async editMessage(channelId: string, messageId: string, content: any) {
    await this.sendMessage(channelId, content);
  }

  async deleteMessage(channelId: string, messageId: string) {}
}

function createDeviceSession(
  deviceBot: DeviceBot,
  session: SessionState,
  text: string,
  messageId: string,
) {
  const deviceId = session.remoteDeviceId || "unknown-device";
  const channelId = `${PRIVATE_PFX}${deviceId}`;
  const next = deviceBot.session();

  next.type = "message";
  next.subtype = "private";
  next.selfId = deviceBot.selfId;
  next.userId = deviceId;
  next.channelId = channelId;
  next.guildId = undefined;
  next.isDirect = true;
  next.content = text;
  next.username = deviceId;
  next.event.user = { id: deviceId, name: deviceId, nick: deviceId } as any;
  next.event.channel = {
    id: channelId,
    type: Universal.Channel.Type.DIRECT,
  } as any;
  next.event.message = {
    id: messageId,
    content: text,
  } as any;
  next.messageId = messageId;
  next.elements = [h.text(text)] as any;

  return next;
}

function dispatchDeviceText(
  logger: ReturnType<Context["logger"]>,
  deviceBot: DeviceBot,
  session: SessionState,
  text: string,
  messageId: string,
  sourceLabel: string,
) {
  if (!text) {
    logger.warn("device Bot 分发跳过：text 为空");
    return;
  }

  const deviceId = session.remoteDeviceId || "unknown-device";
  logger.info(`准备通过 device Bot 分发${sourceLabel} <- ${deviceId}: ${text}`);

  const deviceSession = createDeviceSession(
    deviceBot,
    session,
    text,
    messageId,
  );

  Promise.resolve(deviceBot.dispatch(deviceSession))
    .then(() => {
      logger.info(`device Bot 已派发${sourceLabel} -> ${deviceId}`);
    })
    .catch((err: unknown) =>
      logger.warn(`device Bot 派发${sourceLabel}失败: ${err}`),
    );
}

// 从 Koishi h 元素树中递归提取纯文本。
function extractPlainText(elements: h[]): string {
  return elements
    .map((el) => {
      if (el.type === "text") return String(el.attrs?.["content"] ?? "");
      if (el.children?.length) return extractPlainText(el.children);
      return "";
    })
    .join("")
    .trim();
}

function cleanupSocketSession(
  socket: any,
  session: SessionState,
  sessionSnapshots: Map<any, SessionSnapshot>,
  liveSessions: Map<string, LiveSessionEntry>,
) {
  sessionSnapshots.delete(socket);
  if (session.remoteDeviceId) {
    const current = liveSessions.get(session.remoteDeviceId);
    if (current?.socket === socket) {
      liveSessions.delete(session.remoteDeviceId);
    }
  }
}

const MESSAGE_TYPES = new Set<MessageType>([
  "hello",
  "hello_ack",
  "heartbeat",
  "command",
  "response",
  "event",
  "stream_open",
  "stream_chunk_meta",
  "stream_close",
  "error",
]);

export const Config: Schema<Config> = Schema.object({
  wsHost: Schema.string()
    .default("0.0.0.0")
    .description("WebSocket 服务监听地址"),
  wsPort: Schema.number()
    .role("port")
    .min(1)
    .max(65535)
    .default(5140)
    .description("WebSocket 服务监听端口"),
  serverId: Schema.string()
    .default("koishi-node-01")
    .description("Utsuho 服务端稳定身份标识（device_id）"),
  audioTxEnable: Schema.boolean()
    .default(true)
    .description("允许服务端发送音频"),
  audioRxEnable: Schema.boolean()
    .default(true)
    .description("允许服务端接收音频"),
  textTxEnable: Schema.boolean()
    .default(true)
    .description("允许服务端发送文本"),
  textRxEnable: Schema.boolean()
    .default(true)
    .description("允许服务端接收文本"),
  imageTxEnable: Schema.boolean()
    .default(false)
    .description("允许服务端发送图片"),
  imageRxEnable: Schema.boolean()
    .default(false)
    .description("允许服务端接收图片"),
});

export function apply(ctx: Context, config: Config) {
  const logger = ctx.logger("esp32");
  const serverConfig = (ctx.server as any).config;
  const localCapabilities = getLocalCapabilities(config);
  const sessionSnapshots = new Map<any, SessionSnapshot>();
  const liveSessions = new Map<string, LiveSessionEntry>();
  const deviceBot = new DeviceBot(ctx, liveSessions, config);

  ctx.effect(() => {
    deviceBot.online();
    return () => deviceBot.offline();
  });

  if (serverConfig) {
    serverConfig.host = config.wsHost;
    serverConfig.port = config.wsPort;
  } else {
    logger.warn("未检测到 koishi server 配置对象，无法写入 host/port");
  }

  logger.info(
    `已挂载 Utsuho WS 路由: ws://${config.wsHost}:${config.wsPort}${WS_PATH}`,
  );
  logger.info(`当前服务端 ID: ${config.serverId}`);
  logger.info(`当前本地能力: ${localCapabilities.join(", ") || "(none)"}`);

  ctx.server.ws(WS_PATH, (socket) => {
    const session = createSessionState();
    const remoteLabel = () => session.remoteDeviceId || "unknown-device";
    sessionSnapshots.set(socket, snapshotSession(session));

    logger.info("ESP32 终端已连接，等待 Utsuho hello");

    session.handshakeTimer = setTimeout(() => {
      if (!session.handshakeDone) {
        logger.warn("握手超时，关闭连接");
        sendError(
          socket,
          session,
          config.serverId,
          "ERR_BAD_REQUEST",
          "hello timeout",
          undefined,
          "reject",
        );
        socket.close();
      }
    }, HANDSHAKE_TIMEOUT_MS);

    socket.on("message", (data, isBinary) => {
      if (isBinary) {
        const binary = Buffer.isBuffer(data)
          ? data
          : Buffer.from(data as ArrayBuffer);
        handleBinaryFrame(logger, socket, session, config, binary);
        return;
      }

      let message: UtsuhoMessage;
      try {
        const text =
          typeof data === "string"
            ? data
            : Buffer.from(data as ArrayBuffer).toString("utf8");
        message = parseMessage(text);
      } catch (error) {
        logger.warn(`无法解析的文本帧: ${String(error)}`);
        sendError(
          socket,
          session,
          config.serverId,
          "ERR_BAD_REQUEST",
          "invalid json message",
        );
        return;
      }

      logger.info(
        `收到 ${message.type}/${message.action} <- ${message.device_id}`,
      );

      if (message.type === "hello") {
        if (message.device_id) {
          const existing = liveSessions.get(message.device_id);
          if (existing && existing.socket !== socket) {
            logger.warn(`检测到设备重新连接，清理旧会话: ${message.device_id}`);
            cleanupSocketSession(
              existing.socket,
              existing.session,
              sessionSnapshots,
              liveSessions,
            );
            try {
              existing.socket.close();
            } catch {}
          }
        }

        handleHello(
          logger,
          socket,
          session,
          config,
          localCapabilities,
          message,
        );
        sessionSnapshots.set(socket, snapshotSession(session));
        if (session.remoteDeviceId) {
          liveSessions.set(session.remoteDeviceId, { socket, session });
        }
        return;
      }

      if (!session.handshakeDone) {
        sendError(
          socket,
          session,
          config.serverId,
          "ERR_INVALID_SESSION",
          "handshake required before business messages",
          message.id,
        );
        return;
      }

      if (!isCurrentSession(session, message)) {
        sendError(
          socket,
          session,
          config.serverId,
          "ERR_INVALID_SESSION",
          "session_id mismatch",
          message.id,
        );
        return;
      }

      switch (message.type) {
        case "heartbeat":
          session.lastHeartbeatAt = Date.now();
          sessionSnapshots.set(socket, snapshotSession(session));
          sendMessage(
            socket,
            buildMessage(
              session,
              config.serverId,
              "server",
              "response",
              "heartbeat_ack",
              {
                ok: true,
              },
              message.id,
            ),
          );
          break;
        case "stream_chunk_meta":
          session.pendingBinaryMeta = message;
          sessionSnapshots.set(socket, snapshotSession(session));
          logger.info(
            `已登记二进制元信息: ${message.action} from ${remoteLabel()}`,
          );
          break;
        case "command":
          handleCommand(logger, socket, session, config, message, deviceBot);
          break;
        case "event":
          handleEvent(logger, socket, session, config, message, deviceBot);
          break;
        case "response":
          logger.info(`收到响应消息 ${message.action} <- ${remoteLabel()}`);
          break;
        case "stream_open":
          logger.info(`收到流开启请求 ${message.action} <- ${remoteLabel()}`);
          handleStreamOpen(logger, socket, session, config, message);
          sessionSnapshots.set(socket, snapshotSession(session));
          break;
        case "stream_close":
          logger.info(`收到流关闭请求 ${message.action} <- ${remoteLabel()}`);
          handleStreamClose(socket, session, config, message);
          session.pendingBinaryMeta = null;
          sessionSnapshots.set(socket, snapshotSession(session));
          break;
        case "error":
          logger.warn(
            `收到对端错误 ${message.payload?.code || "UNKNOWN"} <- ${remoteLabel()}`,
          );
          break;
        case "hello_ack":
          sendError(
            socket,
            session,
            config.serverId,
            "ERR_BAD_REQUEST",
            "server should not receive hello_ack in v1 handshake flow",
            message.id,
          );
          break;
      }
    });

    socket.on("close", () => {
      clearHandshakeTimer(session);
      cleanupSocketSession(socket, session, sessionSnapshots, liveSessions);
      logger.info(`ESP32 终端已断开: ${remoteLabel()}`);
    });
  });

  ctx.server.get("/esp32/status", (koa) => {
    const dedupedSessions = new Map<string, SessionSnapshot>();
    const anonymousSessions: SessionSnapshot[] = [];

    for (const item of sessionSnapshots.values()) {
      if (item.remoteDeviceId) {
        dedupedSessions.set(item.remoteDeviceId, item);
      } else {
        anonymousSessions.push(item);
      }
    }

    const sessions = [...dedupedSessions.values(), ...anonymousSessions].map(
      (item) => ({
        sessionId: item.sessionId,
        remoteDeviceId: item.remoteDeviceId,
        remoteRole: item.remoteRole,
        handshakeDone: item.handshakeDone,
        declaredCapabilities: item.declaredCapabilities,
        effectiveCapabilities: item.effectiveCapabilities,
        lastHeartbeatAt: item.lastHeartbeatAt,
        audioStream: item.audioStream,
      }),
    );

    koa.body = {
      protocol: PROTOCOL_NAME,
      version: PROTOCOL_VERSION,
      wsPath: WS_PATH,
      serverId: config.serverId,
      localCapabilities,
      sessions,
    };
  });

  ctx.server.get("/esp32/console", (koa) => {
    koa.type = "text/html; charset=utf-8";
    koa.body = `<!DOCTYPE html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Utsuho Console</title>
<style>
body{font-family:Arial,sans-serif;margin:0;background:#eef3f7;color:#17212b}
.box{max-width:840px;margin:0 auto;padding:24px}
.panel{background:#fff;border:1px solid #dbe3ec;border-radius:14px;padding:18px;margin-bottom:18px;box-shadow:0 8px 24px rgba(20,35,60,.06)}
.row{display:flex;gap:10px;flex-wrap:wrap}
label{display:block;font-weight:bold;margin:12px 0 6px}
input,select,textarea{width:100%;padding:10px;border:1px solid #ccd6e2;border-radius:8px;box-sizing:border-box}
textarea{min-height:88px;resize:vertical}
button{padding:10px 16px;border:none;border-radius:8px;background:#1677ff;color:#fff;cursor:pointer}
button.secondary{background:#66768a}
.muted{color:#607080;font-size:14px}
ul{padding-left:18px}
</style>
<script>
async function loadStatus(){
  try{
    const resp = await fetch('/esp32/status')
    const data = await resp.json()
    const list = document.getElementById('session-list')
    const select = document.getElementById('device-id')
    list.innerHTML = ''
    const current = select.value
    select.innerHTML = '<option value="">请选择在线设备</option>'
    if (!Array.isArray(data.sessions) || data.sessions.length === 0) {
      list.innerHTML = '<li>当前没有在线设备</li>'
      return
    }
    data.sessions.forEach(item => {
      const li = document.createElement('li')
      const audio = item.audioStream
      const audioText = audio ? (' | audio=' + (audio.streamId || 'unknown') + ' seq=' + (audio.lastSeq ?? '-') + ' bytes=' + (audio.lastLength ?? '-') + ' rate=' + (audio.sampleRate ?? '-') + ' loopback=' + (audio.loopbackActive ? 'on' : 'off') + ' buffered=' + (audio.bufferedChunkCount ?? 0) + '/' + (audio.bufferedBytes ?? 0)) : ''
      li.textContent = (item.remoteDeviceId || 'unknown') + ' | handshake=' + (item.handshakeDone ? 'yes' : 'no') + ' | effective=' + ((item.effectiveCapabilities || []).join(', ') || '(none)') + audioText
      list.appendChild(li)
      const option = document.createElement('option')
      option.value = item.remoteDeviceId || ''
      option.textContent = item.remoteDeviceId || 'unknown'
      if (current && current === option.value) option.selected = true
      select.appendChild(option)
    })
  } catch (e) {
    document.getElementById('session-list').innerHTML = '<li>状态读取失败</li>'
  }
}

async function sendText(){
  const deviceId = document.getElementById('device-id').value.trim()
  const text = document.getElementById('text').value.trim()
  if (!deviceId || !text) {
    alert('请选择设备并输入测试文本')
    return
  }
  try {
    const body = new URLSearchParams({ deviceId, text })
    const resp = await fetch('/esp32/send-text', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body,
    })
    const data = await resp.json()
    if (!resp.ok || !data.ok) {
      alert(data.error || '发送失败')
      return
    }
    alert('已发送测试文本')
  } catch (e) {
    alert('发送失败')
  }
}

window.addEventListener('load', loadStatus)
</script>
</head><body><div class="box">
<div class="panel">
  <h2>Utsuho 控制台</h2>
  <p class="muted">用于查看当前在线 ESP32 设备，并向指定设备发送 \`command/text_send\` 测试文本。</p>
  <div class="row">
    <button type="button" class="secondary" onclick="loadStatus()">刷新设备状态</button>
  </div>
</div>
<div class="panel">
  <h3>在线设备</h3>
  <ul id="session-list"><li>正在读取状态...</li></ul>
</div>
<div class="panel">
  <h3>发送测试文本</h3>
  <label for="device-id">目标设备</label>
  <select id="device-id"><option value="">请选择在线设备</option></select>
  <label for="text">文本内容</label>
  <textarea id="text" placeholder="hello from koishi"></textarea>
  <div class="row">
    <button type="button" onclick="sendText()">发送测试文本</button>
  </div>
</div>
</div></body></html>`;
  });

  ctx.server.post("/esp32/send-text", async (koa) => {
    const requestBody = await readRequestBody(koa);
    const deviceId = String(requestBody.deviceId || "").trim();
    const text = String(requestBody.text || "").trim();

    if (!deviceId || !text) {
      koa.status = 400;
      koa.body = { ok: false, error: "deviceId and text are required" };
      return;
    }

    const entry = liveSessions.get(deviceId);
    if (!entry) {
      koa.status = 404;
      koa.body = { ok: false, error: "device not connected" };
      return;
    }

    if (!entry.session.handshakeDone) {
      koa.status = 409;
      koa.body = { ok: false, error: "device handshake not completed" };
      return;
    }

    if (!entry.session.effectiveCapabilities.includes("text_tx")) {
      koa.status = 409;
      koa.body = { ok: false, error: "text_tx not enabled for this session" };
      return;
    }

    sendMessage(
      entry.socket,
      buildMessage(
        entry.session,
        config.serverId,
        "server",
        "command",
        "text_send",
        { text },
      ),
    );

    koa.body = { ok: true, deviceId, text };
  });
}

async function readRequestBody(koa: any): Promise<Record<string, any>> {
  const body = (koa.request as any).body;
  if (body && typeof body === "object" && Object.keys(body).length) {
    return body;
  }

  const raw = await new Promise<string>((resolve, reject) => {
    let data = "";
    koa.req.on("data", (chunk: Buffer | string) => {
      data += chunk.toString();
    });
    koa.req.on("end", () => resolve(data));
    koa.req.on("error", reject);
  });

  const params = new URLSearchParams(raw);
  const parsed: Record<string, string> = {};
  for (const [key, value] of params.entries()) {
    parsed[key] = value;
  }
  return parsed;
}

function createSessionState(): SessionState {
  return {
    sessionId: null,
    remoteDeviceId: null,
    remoteRole: null,
    handshakeDone: false,
    declaredCapabilities: [],
    effectiveCapabilities: [],
    lastHeartbeatAt: null,
    pendingBinaryMeta: null,
    audioStream: null,
    audioLoopbackChunks: [],
    messageCounter: 0,
    handshakeTimer: null,
  };
}

function clearHandshakeTimer(session: SessionState) {
  if (session.handshakeTimer) {
    clearTimeout(session.handshakeTimer);
    session.handshakeTimer = null;
  }
}

function snapshotSession(session: SessionState): SessionSnapshot {
  return {
    sessionId: session.sessionId,
    remoteDeviceId: session.remoteDeviceId,
    remoteRole: session.remoteRole,
    handshakeDone: session.handshakeDone,
    declaredCapabilities: [...session.declaredCapabilities],
    effectiveCapabilities: [...session.effectiveCapabilities],
    lastHeartbeatAt: session.lastHeartbeatAt,
    audioStream: session.audioStream ? { ...session.audioStream } : null,
  };
}

function getLocalCapabilities(config: Config): Capability[] {
  const capabilities: Capability[] = [];

  if (config.audioTxEnable) capabilities.push("audio_tx");
  if (config.audioRxEnable) capabilities.push("audio_rx");
  if (config.textTxEnable) capabilities.push("text_tx");
  if (config.textRxEnable) capabilities.push("text_rx");
  if (config.imageTxEnable) capabilities.push("image_tx");
  if (config.imageRxEnable) capabilities.push("image_rx");

  return capabilities;
}

function parseMessage(raw: string): UtsuhoMessage {
  const parsed = JSON.parse(raw);

  if (!parsed || typeof parsed !== "object") {
    throw new Error("message must be an object");
  }
  if (parsed.v !== PROTOCOL_VERSION) {
    throw new Error(`unsupported version ${String(parsed.v)}`);
  }
  if (!MESSAGE_TYPES.has(parsed.type)) {
    throw new Error(`unsupported type ${String(parsed.type)}`);
  }
  if (typeof parsed.action !== "string" || !parsed.action) {
    throw new Error("action is required");
  }
  if (typeof parsed.id !== "string" || !parsed.id) {
    throw new Error("id is required");
  }
  if (typeof parsed.session_id !== "string" || !parsed.session_id) {
    throw new Error("session_id is required");
  }
  if (typeof parsed.device_id !== "string" || !parsed.device_id) {
    throw new Error("device_id is required");
  }
  if (parsed.peer_role !== "device" && parsed.peer_role !== "server") {
    throw new Error("peer_role is invalid");
  }
  if (typeof parsed.ts !== "number") {
    throw new Error("ts is required");
  }
  if (
    !parsed.payload ||
    typeof parsed.payload !== "object" ||
    Array.isArray(parsed.payload)
  ) {
    throw new Error("payload must be an object");
  }

  return parsed as UtsuhoMessage;
}

function isCurrentSession(session: SessionState, message: UtsuhoMessage) {
  return (
    session.sessionId === message.session_id &&
    session.remoteDeviceId === message.device_id
  );
}

function handleHello(
  logger: ReturnType<Context["logger"]>,
  socket: any,
  session: SessionState,
  config: Config,
  localCapabilities: Capability[],
  message: UtsuhoMessage,
) {
  if (message.peer_role !== "device") {
    sendError(
      socket,
      session,
      config.serverId,
      "ERR_BAD_REQUEST",
      "hello must come from device role",
      message.id,
    );
    socket.close();
    return;
  }

  if (message.payload.protocol !== PROTOCOL_NAME) {
    sendError(
      socket,
      session,
      config.serverId,
      "ERR_UNSUPPORTED_VERSION",
      "unsupported protocol name",
      message.id,
    );
    socket.close();
    return;
  }

  if (message.payload.heartbeat_ms !== HEARTBEAT_MS) {
    sendError(
      socket,
      session,
      config.serverId,
      "ERR_UNSUPPORTED_VERSION",
      "unsupported heartbeat interval",
      message.id,
    );
    socket.close();
    return;
  }

  const remoteCapabilities = normalizeCapabilities(
    message.payload.capabilities,
  );
  session.sessionId = message.session_id;
  session.remoteDeviceId = message.device_id;
  session.remoteRole = message.peer_role;
  session.declaredCapabilities = remoteCapabilities;
  session.effectiveCapabilities = intersectCapabilities(
    localCapabilities,
    remoteCapabilities,
  );
  session.handshakeDone = true;
  session.lastHeartbeatAt = Date.now();
  session.pendingBinaryMeta = null;

  clearHandshakeTimer(session);

  logger.info(
    `Utsuho 会话已建立: device=${session.remoteDeviceId}, session=${session.sessionId}, effective=${session.effectiveCapabilities.join(", ") || "(none)"}`,
  );

  sendMessage(
    socket,
    buildMessage(
      session,
      config.serverId,
      "server",
      "hello_ack",
      "accept",
      {
        protocol: PROTOCOL_NAME,
        heartbeat_ms: HEARTBEAT_MS,
        capabilities: localCapabilities,
        effective_capabilities: session.effectiveCapabilities,
      },
      message.id,
    ),
  );
}

function handleCommand(
  logger: ReturnType<Context["logger"]>,
  socket: any,
  session: SessionState,
  config: Config,
  message: UtsuhoMessage,
  deviceBot: DeviceBot,
) {
  if (message.action === "text_send") {
    if (!session.effectiveCapabilities.includes("text_rx")) {
      sendError(
        socket,
        session,
        config.serverId,
        "ERR_CAPABILITY_DISABLED",
        "text_rx is disabled on server side",
        message.id,
      );
      return;
    }

    const text = String(message.payload?.text ?? "").trim();
    logger.info(`收到文本命令 <- ${session.remoteDeviceId}: ${text}`);

    sendMessage(
      socket,
      buildMessage(
        session,
        config.serverId,
        "server",
        "response",
        "text_send_result",
        {
          ok: true,
        },
        message.id,
      ),
    );

    dispatchDeviceText(
      logger,
      deviceBot,
      session,
      text,
      message.id,
      "命令文本",
    );

    return;
  }

  logger.info(`收到未处理命令 ${message.action} <- ${session.remoteDeviceId}`);
  sendError(
    socket,
    session,
    config.serverId,
    "ERR_BAD_REQUEST",
    `unsupported command action: ${message.action}`,
    message.id,
  );
}

function handleEvent(
  logger: ReturnType<Context["logger"]>,
  socket: any,
  session: SessionState,
  config: Config,
  message: UtsuhoMessage,
  deviceBot: DeviceBot,
) {
  if (message.action === "text_received") {
    const text = String(message.payload.text || "").trim();

    logger.info(`收到文本事件 <- ${session.remoteDeviceId}: ${text}`);

    dispatchDeviceText(
      logger,
      deviceBot,
      session,
      text,
      message.id,
      "文本事件",
    );

    return;
  }

  logger.info(`收到事件 ${message.action} <- ${session.remoteDeviceId}`);

  if (
    message.action.startsWith("image_") &&
    !session.effectiveCapabilities.includes("image_rx")
  ) {
    sendError(
      socket,
      session,
      config.serverId,
      "ERR_CAPABILITY_DISABLED",
      "image_rx is disabled on server side",
      message.id,
    );
  }
}

function handleStreamOpen(
  logger: ReturnType<Context["logger"]>,
  socket: any,
  session: SessionState,
  config: Config,
  message: UtsuhoMessage,
) {
  const mediaKind = String(message.payload?.media_kind || "");

  if (mediaKind === "audio") {
    if (!session.effectiveCapabilities.includes("audio_rx")) {
      sendError(
        socket,
        session,
        config.serverId,
        "ERR_CAPABILITY_DISABLED",
        "audio_rx is disabled on server side",
        message.id,
      );
      return;
    }

    const canLoopback = session.effectiveCapabilities.includes("audio_tx");

    session.audioStream = {
      streamId: String(message.payload?.stream_id || ""),
      mediaKind,
      codec: String(message.payload?.codec || ""),
      sampleRate: Number.isFinite(Number(message.payload?.sample_rate))
        ? Number(message.payload?.sample_rate)
        : null,
      channels: Number.isFinite(Number(message.payload?.channels))
        ? Number(message.payload?.channels)
        : null,
      lastSeq: null,
      lastLength: null,
      lastChunkAt: null,
      loopbackActive: canLoopback,
      bufferedChunkCount: 0,
      bufferedBytes: 0,
    };
    logger.info(
      `已登记音频流 <- ${session.remoteDeviceId}: stream=${session.audioStream.streamId}, codec=${session.audioStream.codec}, rate=${String(session.audioStream.sampleRate ?? "-")}`,
    );

    session.audioLoopbackChunks = [];
  }
}

function handleStreamClose(
  socket: any,
  session: SessionState,
  config: Config,
  message: UtsuhoMessage,
) {
  const mediaKind = String(message.payload?.media_kind || "");
  const streamId = String(message.payload?.stream_id || "");

  if (
    mediaKind === "audio" ||
    (!mediaKind && session.audioStream?.streamId === streamId)
  ) {
    if (session.audioStream?.loopbackActive) {
      sendMessage(
        socket,
        buildMessage(
          session,
          config.serverId,
          "server",
          "stream_open",
          "audio_start",
          {
            stream_id: session.audioStream.streamId,
            media_kind: "audio",
            direction: "tx",
            codec: session.audioStream.codec || "pcm_s16le",
            sample_rate: session.audioStream.sampleRate || 16000,
            channels: session.audioStream.channels || 1,
            frame_ms: 20,
          },
          message.id,
        ),
      );

      let seq = 1;
      for (const chunk of session.audioLoopbackChunks) {
        sendMessage(
          socket,
          buildMessage(
            session,
            config.serverId,
            "server",
            "stream_chunk_meta",
            "audio_data",
            {
              stream_id: session.audioStream.streamId,
              media_kind: "audio",
              codec: session.audioStream.codec || "pcm_s16le",
              sample_rate: session.audioStream.sampleRate || 16000,
              channels: session.audioStream.channels || 1,
              seq,
              length: chunk.length,
            },
          ),
        );
        socket.send(chunk, { binary: true });
        seq++;
      }

      sendMessage(
        socket,
        buildMessage(
          session,
          config.serverId,
          "server",
          "stream_close",
          "audio_stop",
          {
            stream_id: session.audioStream.streamId,
            media_kind: "audio",
            reason: "normal",
          },
          message.id,
        ),
      );
    }
    session.audioLoopbackChunks = [];
    session.audioStream = null;
  }
}

function handleBinaryFrame(
  logger: ReturnType<Context["logger"]>,
  socket: any,
  session: SessionState,
  config: Config,
  data: Buffer,
) {
  if (!session.handshakeDone || !session.pendingBinaryMeta) {
    sendError(
      socket,
      session,
      config.serverId,
      "ERR_BINARY_WITHOUT_META",
      "binary frame without pending stream_chunk_meta",
    );
    return;
  }

  const meta = session.pendingBinaryMeta;
  const length = Number(meta.payload?.length || 0);
  const mediaKind = String(meta.payload?.media_kind || "");

  if (!Number.isFinite(length) || length <= 0) {
    sendError(
      socket,
      session,
      config.serverId,
      "ERR_BAD_REQUEST",
      "invalid binary meta length",
      meta.id,
    );
    session.pendingBinaryMeta = null;
    return;
  }

  if (data.length !== length) {
    sendError(
      socket,
      session,
      config.serverId,
      "ERR_BINARY_LENGTH_MISMATCH",
      `expected ${length} bytes but got ${data.length}`,
      meta.id,
    );
    session.pendingBinaryMeta = null;
    return;
  }

  if (
    mediaKind === "audio" &&
    !session.effectiveCapabilities.includes("audio_rx")
  ) {
    sendError(
      socket,
      session,
      config.serverId,
      "ERR_CAPABILITY_DISABLED",
      "audio_rx is disabled on server side",
      meta.id,
    );
    session.pendingBinaryMeta = null;
    return;
  }

  if (
    mediaKind === "image" &&
    !session.effectiveCapabilities.includes("image_rx")
  ) {
    sendError(
      socket,
      session,
      config.serverId,
      "ERR_CAPABILITY_DISABLED",
      "image_rx is disabled on server side",
      meta.id,
    );
    session.pendingBinaryMeta = null;
    return;
  }

  logger.info(
    `收到二进制数据块 <- ${session.remoteDeviceId}: kind=${mediaKind}, seq=${String(meta.payload?.seq ?? "-")}, bytes=${data.length}`,
  );

  if (mediaKind === "audio" && session.audioStream) {
    session.audioStream.lastSeq = Number.isFinite(Number(meta.payload?.seq))
      ? Number(meta.payload?.seq)
      : null;
    session.audioStream.lastLength = data.length;
    session.audioStream.lastChunkAt = Date.now();
    session.audioLoopbackChunks.push(Buffer.from(data));
    session.audioStream.bufferedChunkCount = session.audioLoopbackChunks.length;
    session.audioStream.bufferedBytes += data.length;
    logger.info(
      `已缓存音频数据块，等待 audio_stop 后回发 -> ${session.remoteDeviceId}: seq=${String(meta.payload?.seq ?? "-")}, bytes=${data.length}`,
    );
  }

  session.pendingBinaryMeta = null;
}

function normalizeCapabilities(input: unknown): Capability[] {
  if (!Array.isArray(input)) {
    return [];
  }

  return input.filter((item): item is Capability => {
    return (
      typeof item === "string" && CAPABILITY_LIST.includes(item as Capability)
    );
  });
}

function intersectCapabilities(local: Capability[], remote: Capability[]) {
  const remoteSet = new Set(remote);
  return local.filter((item) => remoteSet.has(item));
}

function buildMessage(
  session: SessionState,
  localDeviceId: string,
  peerRole: PeerRole,
  type: MessageType,
  action: string,
  payload: Record<string, any>,
  replyTo?: string,
): UtsuhoMessage {
  const currentSessionId = session.sessionId || `srv-pre-${Date.now()}`;
  const id = `${type}-${++session.messageCounter}`;

  return {
    v: PROTOCOL_VERSION,
    type,
    action,
    id,
    session_id: currentSessionId,
    device_id: localDeviceId,
    peer_role: peerRole,
    ts: Date.now(),
    payload,
    ...(replyTo ? { reply_to: replyTo } : {}),
  };
}

function sendError(
  socket: any,
  session: SessionState,
  localDeviceId: string,
  code: string,
  message: string,
  replyTo?: string,
  action = "reject",
) {
  sendMessage(
    socket,
    buildMessage(
      session,
      localDeviceId,
      "server",
      "error",
      action,
      { code, message },
      replyTo,
    ),
  );
}

function sendMessage(socket: any, message: UtsuhoMessage) {
  socket.send(JSON.stringify(message));
}
