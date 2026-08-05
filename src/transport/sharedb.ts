import { EventEmitter } from "node:events";
import WebSocket from "ws";
import type { Config } from "../config.js";
import { WanderlogAuthError, WanderlogError } from "../errors.js";
import type { Json0Op } from "../ot/apply.js";
import type { TripPlan } from "../types.js";

export type { Json0Op };

type InitFrame = {
  a: "init";
  id: string;
  protocol: number;
  protocolMinor: number;
  type: string;
};

type HandshakeAckFrame = {
  a: "hs";
  id: string;
  protocol: number;
  protocolMinor: number;
  type: string;
};

type SubscribeAckFrame = {
  a: "s";
  c: string;
  d: string;
  data?: { v: number; data: TripPlan };
};

type OpFrame = {
  a: "op";
  c: string;
  d: string;
  v: number;
  seq?: number;
  src?: string;
  op?: Json0Op[];
};

type Frame = InitFrame | HandshakeAckFrame | SubscribeAckFrame | OpFrame;

type PendingSubscribe = {
  resolve: (ack: SubscribeAckFrame) => void;
  reject: (err: Error) => void;
  timer: NodeJS.Timeout;
};

const CONNECT_RETRY_DELAY_MS = 250;

export interface ShareDBClient {
  /** Fired when a remote op (not one we submitted) is received. */
  on(event: "remoteOp", listener: (ops: Json0Op[], version: number) => void): this;
  on(event: "reconnected", listener: () => void): this;
  on(event: "closed", listener: (code: number) => void): this;
  off(event: string, listener: (...args: any[]) => void): this;
}

/**
 * ShareDB JSONv0 client bound to a single trip key.
 * Exposes subscribe() for the initial snapshot, submit() for outgoing ops
 * (with version tracking and ack waiting), and a `remoteOp` event for ops
 * pushed by the server from other clients.
 */
export class ShareDBClient extends EventEmitter {
  private ws?: WebSocket;
  private sessionId?: string;
  private handshakeComplete = false;
  private closedByUser = false;
  private reconnectAttempts = 0;
  private seqCounter = 0;
  private snapshot?: TripPlan;
  private _version = 0;
  private subscribed = false;
  private subscribePending?: PendingSubscribe;
  private subscribePromise?: Promise<TripPlan>;
  private readonly pendingOps = new Map<
    number,
    { resolve: () => void; reject: (err: Error) => void; timer: NodeJS.Timeout }
  >();
  private connectPromise?: Promise<void>;
  private reconnectTimer?: NodeJS.Timeout;

  constructor(
    private readonly config: Config,
    private readonly tripKey: string,
  ) {
    super();
  }

  get version(): number {
    return this._version;
  }

  get currentSnapshot(): TripPlan | undefined {
    return this.snapshot;
  }

  get isSubscribed(): boolean {
    return this.subscribed;
  }

  private url(): string {
    return `${this.config.wsBaseUrl}/api/tripPlans/wsOverall/${encodeURIComponent(
      this.tripKey,
    )}?clientSchemaVersion=2`;
  }

  async connect(): Promise<void> {
    if (
      this.handshakeComplete &&
      this.ws?.readyState === WebSocket.OPEN
    ) {
      return;
    }
    if (this.connectPromise) return this.connectPromise;

    // An explicit caller retry should take over from a delayed background
    // reconnect instead of creating a second socket later.
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }

    this.connectPromise = this.connectWithRetry();
    try {
      await this.connectPromise;
    } finally {
      this.connectPromise = undefined;
    }
  }

  private async connectWithRetry(): Promise<void> {
    let attempt = 0;
    for (;;) {
      try {
        await this.doConnect();
        return;
      } catch (err) {
        if (
          this.closedByUser ||
          err instanceof WanderlogAuthError ||
          attempt >= 1
        ) {
          throw err;
        }
        // Keep one transient handshake failure from surfacing to the tool,
        // while leaving longer outages to the background reconnect policy.
        attempt += 1;
        await new Promise<void>((resolve) =>
          setTimeout(resolve, CONNECT_RETRY_DELAY_MS),
        );
      }
    }
  }

  private doConnect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(this.url(), {
        headers: {
          Cookie: this.config.cookieHeader,
          Origin: this.config.baseUrl,
          "User-Agent": this.config.userAgent,
        },
      });
      this.ws = ws;
      this.handshakeComplete = false;
      let settled = false;

      const resolveConnection = () => {
        if (settled) return;
        settled = true;
        resolve();
      };
      const rejectConnection = (err: Error) => {
        if (settled) return;
        settled = true;
        reject(err);
      };
      const isCurrentSocket = () => this.ws === ws;

      const handshakeTimeout = setTimeout(() => {
        rejectConnection(
          new WanderlogError("ShareDB handshake timeout", "ws_timeout"),
        );
        if (ws.readyState === WebSocket.OPEN) {
          ws.close(1011, "handshake timeout");
        } else if (ws.readyState === WebSocket.CONNECTING) {
          ws.terminate();
        }
      }, 10_000);

      ws.on("open", () => {
        if (!isCurrentSocket()) return;
        this.send({ a: "hs", id: null, protocol: 1, protocolMinor: 2 });
      });

      ws.on("message", (raw) => {
        if (!isCurrentSocket()) return;
        const text = raw.toString();
        let msg: unknown;
        try {
          msg = JSON.parse(text);
        } catch {
          return;
        }
        if (!msg || typeof msg !== "object") return;
        this.handleFrame(
          msg as Frame & { error?: unknown },
          handshakeTimeout,
          resolveConnection,
        );
      });

      ws.on("close", (code: number) => {
        clearTimeout(handshakeTimeout);
        if (!isCurrentSocket()) {
          rejectConnection(new WanderlogError("WebSocket closed", "ws_closed"));
          return;
        }
        const wasSubscribed = this.subscribed;
        this.handshakeComplete = false;
        this.subscribed = false;
        this.ws = undefined;
        this.failAllPending(
          new WanderlogError("WebSocket closed", "ws_closed"),
        );
        if (!settled) {
          rejectConnection(
            new WanderlogError(
              "WebSocket closed before ShareDB handshake completed",
              "ws_closed",
            ),
          );
        }
        this.emit("closed", code);
        if (!this.closedByUser && code !== 1000 && wasSubscribed) {
          this.scheduleReconnect(wasSubscribed);
        }
      });

      ws.on("unexpected-response", (_req, res) => {
        clearTimeout(handshakeTimeout);
        if (res.statusCode === 401 || res.statusCode === 403) {
          rejectConnection(new WanderlogAuthError());
        } else {
          rejectConnection(
            new WanderlogError(
              `WebSocket upgrade failed: ${res.statusCode}`,
              "ws_upgrade_failed",
            ),
          );
        }
      });

      ws.on("error", (err: Error) => {
        clearTimeout(handshakeTimeout);
        if (!isCurrentSocket() || !this.handshakeComplete) {
          rejectConnection(err);
        }
      });
    });
  }

  private handleFrame(
    frame: Frame & { error?: unknown; seq?: number; code?: number; message?: string },
    handshakeTimeout: NodeJS.Timeout,
    connectResolve: () => void,
  ): void {
    // Server rejections arrive as bare {code, message} frames with no `a` and
    // no `seq` (observed: {code: 4001, message: "Too many requests"}). Without
    // this branch they fall through silently and the submit dies as an opaque
    // 10s timeout. No seq means we can't attribute it — fail everything.
    const bare = frame as { a?: string; code?: number; message?: string };
    if (bare.a === undefined && typeof bare.code === "number") {
      const code = bare.code === 4001 ? "rate_limited" : "ws_rejected";
      this.failAllPending(
        new WanderlogError(
          `Wanderlog rejected the request (${bare.code}): ${bare.message ?? "unknown"}`,
          code,
        ),
      );
      return;
    }

    if (frame.error) {
      const err = frame.error as string | { message?: string };
      const errMsg = typeof err === "string" ? err : err.message ?? "unknown";

      // If the error frame carries a seq, it belongs to a specific submit.
      // Fail only that one pending op, so concurrent/queued submits are not
      // collateral damage.
      if (typeof frame.seq === "number" && this.pendingOps.has(frame.seq)) {
        const pending = this.pendingOps.get(frame.seq)!;
        this.pendingOps.delete(frame.seq);
        clearTimeout(pending.timer);
        pending.reject(new WanderlogError(errMsg, "ws_error"));
        return;
      }

      // No seq, or unknown seq — fall back to failing everything, since we
      // can't safely attribute the error.
      this.failAllPending(new WanderlogError(errMsg, "ws_error"));
      return;
    }

    if (frame.a === "init") {
      this.sessionId = (frame as InitFrame).id;
      return;
    }

    if (frame.a === "hs" && !this.handshakeComplete) {
      this.handshakeComplete = true;
      clearTimeout(handshakeTimeout);
      this.reconnectAttempts = 0;
      const hs = frame as HandshakeAckFrame;
      // A reconnect creates a new ShareDB session. Keeping the old id makes
      // later op acknowledgements look like remote ops and eventually time out.
      if (hs.id) this.sessionId = hs.id;
      connectResolve();
      return;
    }

    if (frame.a === "s") {
      const pending = this.subscribePending;
      if (pending) {
        this.subscribePending = undefined;
        clearTimeout(pending.timer);
        pending.resolve(frame as SubscribeAckFrame);
      }
      return;
    }

    if (frame.a === "op") {
      this.handleOpFrame(frame as OpFrame);
    }
  }

  private handleOpFrame(frame: OpFrame): void {
    const isOurAck =
      frame.src === this.sessionId &&
      frame.seq !== undefined &&
      this.pendingOps.has(frame.seq);

    if (isOurAck) {
      const pending = this.pendingOps.get(frame.seq!)!;
      this.pendingOps.delete(frame.seq!);
      clearTimeout(pending.timer);
      this._version = frame.v + 1;
      pending.resolve();
      return;
    }

    if (frame.op && frame.op.length > 0) {
      this._version = frame.v + 1;
      this.emit("remoteOp", frame.op, this._version);
    }
  }

  private failAllPending(err: Error): void {
    if (this.subscribePending) {
      const pending = this.subscribePending;
      this.subscribePending = undefined;
      clearTimeout(pending.timer);
      pending.reject(err);
    }
    for (const [seq, pending] of this.pendingOps) {
      clearTimeout(pending.timer);
      pending.reject(err);
      this.pendingOps.delete(seq);
    }
  }

  private scheduleReconnect(resubscribe: boolean): void {
    if (this.reconnectTimer || this.closedByUser) return;

    const delay = Math.min(1000 * 2 ** this.reconnectAttempts, 30_000);
    this.reconnectAttempts += 1;

    if (this.reconnectAttempts > 5) {
      console.warn(
        `[wanderdog] Reconnection has failed ${this.reconnectAttempts} times consecutively. Delaying next attempt by ${delay / 1000}s.`,
      );
    }

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      if (this.closedByUser) return;
      this.connect()
        .then(() => {
          this.reconnectAttempts = 0;
          if (resubscribe) {
            void this.subscribe()
              .then(() => this.emit("reconnected"))
              .catch((err) => this.handleReconnectFailure(err, resubscribe));
          } else {
            this.emit("reconnected");
          }
        })
        .catch((err) => {
          this.handleReconnectFailure(err, resubscribe);
        });
    }, delay);
  }

  private handleReconnectFailure(err: unknown, resubscribe: boolean): void {
    if (err instanceof WanderlogAuthError) {
      console.error(
        `[wanderdog] Permanent reconnection failure: Auth expired. Stopping reconnection.`,
      );
      this.failAllPending(err);
      return;
    }
    this.scheduleReconnect(resubscribe);
  }

  private send(obj: unknown): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new WanderlogError(
        "WebSocket is not open — cannot send frame",
        "ws_not_open",
      );
    }
    this.ws.send(JSON.stringify(obj));
  }

  async subscribe(): Promise<TripPlan> {
    if (this.subscribed && this.snapshot) return this.snapshot;
    if (this.subscribePromise) return this.subscribePromise;

    const promise = this.doSubscribe();
    this.subscribePromise = promise;
    try {
      return await promise;
    } finally {
      if (this.subscribePromise === promise) this.subscribePromise = undefined;
    }
  }

  private async doSubscribe(): Promise<TripPlan> {
    await this.connect();

    if (this.subscribed && this.snapshot) return this.snapshot;

    const ack = await new Promise<SubscribeAckFrame>((resolve, reject) => {
      const pending = {
        resolve,
        reject,
        timer: undefined as unknown as NodeJS.Timeout,
      } satisfies PendingSubscribe;
      this.subscribePending = pending;
      pending.timer = setTimeout(() => {
        if (this.subscribePending !== pending) return;
        this.subscribePending = undefined;
        reject(new WanderlogError("Subscribe timeout", "subscribe_timeout"));
      }, 10_000);
      try {
        this.send({ a: "s", c: "TripPlans", d: this.tripKey });
      } catch (err) {
        if (this.subscribePending === pending) {
          this.subscribePending = undefined;
          clearTimeout(pending.timer);
        }
        reject(err as Error);
      }
    });

    if (!ack.data) {
      throw new WanderlogError("Subscribe ack missing snapshot", "subscribe_failed");
    }

    this.snapshot = ack.data.data;
    this._version = ack.data.v;
    this.subscribed = true;
    return this.snapshot;
  }

  /**
   * Submit a JSON0 op array to the server. Resolves when the server acks.
   * Throws if not subscribed, if the WebSocket is closed, or on ack timeout.
   *
   * On successful ack, the local version is bumped to `frame.v + 1`.
   */
  async submit(ops: Json0Op[]): Promise<void> {
    if (!this.subscribed) {
      throw new WanderlogError(
        "Cannot submit op before subscribing to the trip",
        "not_subscribed",
      );
    }
    if (ops.length === 0) {
      throw new WanderlogError("Cannot submit an empty op array", "empty_op");
    }

    this.seqCounter += 1;
    const seq = this.seqCounter;
    const frame = {
      a: "op",
      c: "TripPlans",
      d: this.tripKey,
      v: this._version,
      seq,
      x: {},
      op: ops,
    };

    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pendingOps.has(seq)) {
          this.pendingOps.delete(seq);
          reject(new WanderlogError("Submit op timeout", "submit_timeout"));
        }
      }, 10_000);
      this.pendingOps.set(seq, { resolve, reject, timer });
      try {
        this.send(frame);
      } catch (err) {
        // Send failed (e.g. WS closed between the isSubscribed check and now).
        // Clean up the pending entry and propagate immediately rather than
        // waiting 10s for the timeout to fire.
        this.pendingOps.delete(seq);
        clearTimeout(timer);
        reject(err);
      }
    });
  }

  /** Drop the local snapshot so the next subscribe fetches a fresh document. */
  reset(): void {
    this.subscribed = false;
    this.handshakeComplete = false;
    this.snapshot = undefined;
    this.sessionId = undefined;
    this._version = 0;
    this.seqCounter = 0;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.failAllPending(new WanderlogError("Client reset", "ws_closed"));
    this.stopSocket();
  }

  private stopSocket(): void {
    const ws = this.ws;
    this.ws = undefined;
    if (ws?.readyState === WebSocket.OPEN) {
      ws.close();
    } else if (ws?.readyState === WebSocket.CONNECTING) {
      ws.terminate();
    }
  }

  close(): void {
    this.closedByUser = true;
    this.subscribed = false;
    this.snapshot = undefined;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.failAllPending(new WanderlogError("Client closed", "ws_closed"));
    this.stopSocket();
  }
}

/**
 * Pool of ShareDBClient instances keyed by trip key. A single MCP server
 * session may subscribe to multiple trips concurrently; each gets its own
 * WebSocket (required, since the URL embeds the trip key).
 */
export class ShareDBPool {
  private readonly clients = new Map<string, ShareDBClient>();

  constructor(private readonly config: Config) {}

  get(tripKey: string): ShareDBClient {
    let client = this.clients.get(tripKey);
    if (!client) {
      client = new ShareDBClient(this.config, tripKey);
      this.clients.set(tripKey, client);
    }
    return client;
  }

  has(tripKey: string): boolean {
    return this.clients.has(tripKey);
  }

  closeAll(): void {
    for (const client of this.clients.values()) {
      client.close();
    }
    this.clients.clear();
  }
}
