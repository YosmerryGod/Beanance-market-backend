import WebSocket from "ws";
import { config } from "./config.js";

export class ManagedWebSocket {
  constructor({ name, url, onMessage, onOpen, onClose, hasDemand = () => true }) {
    this.name = name;
    this.url = url;
    this.onMessage = onMessage;
    this.onOpen = onOpen;
    this.onClose = onClose;
    this.hasDemand = hasDemand;
    this.socket = null;
    this.reconnectTimer = null;
    this.heartbeatTimer = null;
    this.reconnectAttempts = 0;
    this.lastActivityAt = 0;
    this.stopped = false;
  }

  connect() {
    if (this.stopped || this.socket) return;
    const socket = new WebSocket(this.url, { handshakeTimeout: 12000, perMessageDeflate: false });
    this.socket = socket;

    socket.on("open", () => {
      if (this.socket !== socket) return;
      this.reconnectAttempts = 0;
      this.lastActivityAt = Date.now();
      console.info(`${this.name} WebSocket connected`);
      this.startHeartbeat(socket);
      this.onOpen?.(socket);
    });

    socket.on("message", (raw) => {
      this.lastActivityAt = Date.now();
      try {
        this.onMessage?.(JSON.parse(raw.toString()), socket);
      } catch (error) {
        console.error(`${this.name} WebSocket message processing failed:`, error.message);
      }
    });

    socket.on("pong", () => {
      this.lastActivityAt = Date.now();
    });

    socket.on("error", (error) => {
      console.error(`${this.name} WebSocket error:`, error.message);
    });

    socket.on("close", (code, reason) => {
      if (this.socket === socket) this.socket = null;
      this.stopHeartbeat();
      this.onClose?.(code, reason?.toString() || "");
      console.warn(`${this.name} WebSocket closed: ${code} ${reason?.toString() || ""}`);
      this.scheduleReconnect();
    });
  }

  startHeartbeat(socket) {
    this.stopHeartbeat();
    this.heartbeatTimer = setInterval(() => {
      if (this.socket !== socket || socket.readyState !== WebSocket.OPEN) return;
      if (Date.now() - this.lastActivityAt > config.websocket.staleTimeoutMs) {
        console.warn(`${this.name} WebSocket is stale; terminating connection`);
        socket.terminate();
        return;
      }
      try {
        socket.ping();
      } catch {
        socket.terminate();
      }
    }, config.websocket.heartbeatMs);
    this.heartbeatTimer.unref?.();
  }

  stopHeartbeat() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
  }

  scheduleReconnect() {
    if (this.stopped || !this.hasDemand() || this.reconnectTimer) return;
    const exponential = Math.min(
      config.reconnectBaseDelayMs * (2 ** this.reconnectAttempts),
      config.reconnectMaxDelayMs
    );
    const delay = Math.floor(exponential * (0.75 + Math.random() * 0.5));
    this.reconnectAttempts += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
    this.reconnectTimer.unref?.();
  }

  getSocket() {
    return this.socket;
  }

  get connected() {
    return this.socket?.readyState === WebSocket.OPEN;
  }

  stop() {
    this.stopped = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.stopHeartbeat();
    const socket = this.socket;
    this.socket = null;
    if (socket) {
      socket.removeAllListeners();
      if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
        socket.close(1000, "Service shutdown");
      }
    }
  }
}
