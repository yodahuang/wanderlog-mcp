import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ShareDBClient } from "../../src/transport/sharedb.ts";
import { WanderlogAuthError, WanderlogError } from "../../src/errors.ts";

const { FakeWebSocket } = vi.hoisted(() => {
  class FakeWebSocket {
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    static readonly CLOSING = 2;
    static readonly CLOSED = 3;

    readyState = FakeWebSocket.CONNECTING;
    private readonly listeners = new Map<
      string,
      Array<(...args: unknown[]) => void>
    >();

    on(event: string, listener: (...args: unknown[]) => void): this {
      const listeners = this.listeners.get(event) ?? [];
      listeners.push(listener);
      this.listeners.set(event, listeners);
      return this;
    }

    emit(event: string, ...args: unknown[]): void {
      for (const listener of this.listeners.get(event) ?? []) {
        listener(...args);
      }
    }

    send(_data: string): void {}

    terminate(): void {
      this.readyState = FakeWebSocket.CLOSED;
    }

    close(): void {
      this.readyState = FakeWebSocket.CLOSING;
      this.emit("close", 1000);
      this.readyState = FakeWebSocket.CLOSED;
    }
  }

  return { FakeWebSocket };
});

vi.mock("ws", () => ({ default: FakeWebSocket }));

describe("ShareDBClient reconnect logic", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("stops reconnecting immediately when encountering WanderlogAuthError", async () => {
    const config = {
      wsBaseUrl: "wss://test",
      baseUrl: "https://test",
      userAgent: "test",
    } as any;
    const client = new ShareDBClient(config, "tripA");

    const rejectErr = new WanderlogAuthError();
    const doConnectSpy = vi.spyOn(client as any, "doConnect").mockRejectedValue(rejectErr);
    const failAllPendingSpy = vi.spyOn(client as any, "failAllPending").mockImplementation(() => {});

    // Trigger scheduleReconnect
    (client as any).scheduleReconnect(false);

    // Initial reconnect is scheduled, wait for the timer to fire (delay is 1000 * 2^0 = 1000ms)
    expect((client as any).reconnectTimer).toBeDefined();
    
    await vi.advanceTimersByTimeAsync(1000);

    // doConnect should have been called
    expect(doConnectSpy).toHaveBeenCalledTimes(1);

    // The timer should be cleared/undefined, and no further reconnect scheduled
    expect((client as any).reconnectTimer).toBeUndefined();
    expect(failAllPendingSpy).toHaveBeenCalledWith(rejectErr);

    // Advance time further to make sure no more retries fire
    await vi.advanceTimersByTimeAsync(30000);
    expect(doConnectSpy).toHaveBeenCalledTimes(1);
  });

  it("deduplicates multiple scheduleReconnect calls using reconnectTimer", () => {
    const config = {
      wsBaseUrl: "wss://test",
      baseUrl: "https://test",
      userAgent: "test",
    } as any;
    const client = new ShareDBClient(config, "tripA");

    // Stub doConnect so it doesn't do real ws
    vi.spyOn(client as any, "doConnect").mockResolvedValue(undefined);

    // First call should schedule reconnect
    (client as any).scheduleReconnect(false);
    const initialTimer = (client as any).reconnectTimer;
    expect(initialTimer).toBeDefined();

    // Second call while timer is active should do nothing and keep the same timer
    (client as any).scheduleReconnect(false);
    expect((client as any).reconnectTimer).toBe(initialTimer);
  });

  it("resets reconnectAttempts back to 0 on a successful connection", async () => {
    const config = {
      wsBaseUrl: "wss://test",
      baseUrl: "https://test",
      userAgent: "test",
    } as any;
    const client = new ShareDBClient(config, "tripA");

    const doConnectSpy = vi.spyOn(client as any, "doConnect").mockResolvedValue(undefined);
    (client as any).reconnectAttempts = 3;

    (client as any).scheduleReconnect(false);
    await vi.advanceTimersByTimeAsync(8000); // 1000 * 2^3 = 8000ms

    expect(doConnectSpy).toHaveBeenCalledTimes(1);
    expect((client as any).reconnectAttempts).toBe(0);
  });

  it("rejects connect when the socket closes before the handshake", async () => {
    const config = {
      wsBaseUrl: "wss://test",
      baseUrl: "https://test",
      userAgent: "test",
    } as any;
    const client = new ShareDBClient(config, "tripA");
    const pending = (client as any).doConnect();
    const socket = (client as any).ws as InstanceType<typeof FakeWebSocket>;

    socket.emit("close", 1006);

    await expect(pending).rejects.toMatchObject({ code: "ws_closed" });
    expect((client as any).ws).toBeUndefined();
  });

  it("retries one transient connection failure for the active caller", async () => {
    const config = {
      wsBaseUrl: "wss://test",
      baseUrl: "https://test",
      userAgent: "test",
    } as any;
    const client = new ShareDBClient(config, "tripA");
    const doConnectSpy = vi
      .spyOn(client as any, "doConnect")
      .mockRejectedValueOnce(new Error("handshake failed"))
      .mockResolvedValueOnce(undefined);

    const promise = client.connect();
    await vi.advanceTimersByTimeAsync(250);
    await promise;

    expect(doConnectSpy).toHaveBeenCalledTimes(2);
  });

  it("does not retry an explicit rate-limit rejection", async () => {
    const config = {
      wsBaseUrl: "wss://test",
      baseUrl: "https://test",
      userAgent: "test",
    } as any;
    const client = new ShareDBClient(config, "tripA");
    const rejection = new WanderlogError(
      "Wanderlog rejected the request (4001): Too many requests",
      "rate_limited",
    );
    const doConnectSpy = vi
      .spyOn(client as any, "doConnect")
      .mockRejectedValue(rejection);

    await expect(client.connect()).rejects.toBe(rejection);
    expect(doConnectSpy).toHaveBeenCalledTimes(1);
  });

  it("surfaces a server rejection received during the handshake", () => {
    const config = {
      wsBaseUrl: "wss://test",
      baseUrl: "https://test",
      userAgent: "test",
    } as any;
    const client = new ShareDBClient(config, "tripA");
    const connectReject = vi.fn();

    (client as any).handleFrame(
      { code: 4001, message: "Too many requests" },
      undefined,
      vi.fn(),
      connectReject,
    );

    expect(connectReject).toHaveBeenCalledOnce();
    expect(connectReject.mock.calls[0]![0]).toMatchObject({
      code: "rate_limited",
      message: "Wanderlog rejected the request (4001): Too many requests",
    });
  });

  it("surfaces a protocol error received during the handshake", () => {
    const config = {
      wsBaseUrl: "wss://test",
      baseUrl: "https://test",
      userAgent: "test",
    } as any;
    const client = new ShareDBClient(config, "tripA");
    const connectReject = vi.fn();

    (client as any).handleFrame(
      { error: { message: "Handshake rejected" } },
      undefined,
      vi.fn(),
      connectReject,
    );

    expect(connectReject).toHaveBeenCalledOnce();
    expect(connectReject.mock.calls[0]![0]).toMatchObject({
      code: "ws_error",
      message: "Handshake rejected",
    });
  });

  it("logs a warning after N consecutive failures (reconnectAttempts > 5)", async () => {
    const config = {
      wsBaseUrl: "wss://test",
      baseUrl: "https://test",
      userAgent: "test",
    } as any;
    const client = new ShareDBClient(config, "tripA");

    // Mock failures
    vi.spyOn(client as any, "doConnect").mockRejectedValue(new Error("Transient connection error"));
    const consoleWarnSpy = vi.spyOn(console, "warn");

    // Set attempts to 5, so the next attempt increments it to 6 and warns
    (client as any).reconnectAttempts = 5;

    (client as any).scheduleReconnect(false);
    
    expect(consoleWarnSpy).toHaveBeenCalledTimes(1);
    expect(consoleWarnSpy.mock.calls[0]![0]).toContain("Reconnection has failed 6 times consecutively");
  });

  it("retries when reconnect resubscription rejects", async () => {
    const config = {
      wsBaseUrl: "wss://test",
      baseUrl: "https://test",
      userAgent: "test",
    } as any;
    const client = new ShareDBClient(config, "tripA");

    vi.spyOn(client, "connect").mockResolvedValue(undefined);
    const subscribeSpy = vi
      .spyOn(client, "subscribe")
      .mockRejectedValue(new Error("subscribe failed"));
    const scheduleSpy = vi.spyOn(client as any, "scheduleReconnect");

    (client as any).scheduleReconnect(true);
    await vi.advanceTimersByTimeAsync(1000);

    expect(subscribeSpy).toHaveBeenCalledTimes(1);
    expect(scheduleSpy).toHaveBeenCalledWith(true);
    client.close();
  });

  it("does not let an old subscribe timeout reject a newer subscription", async () => {
    const config = {
      wsBaseUrl: "wss://test",
      baseUrl: "https://test",
      userAgent: "test",
    } as any;
    const client = new ShareDBClient(config, "tripA");
    vi.spyOn(client, "connect").mockResolvedValue(undefined);
    vi.spyOn(client as any, "send").mockImplementation(() => {});

    const first = client.subscribe();
    await Promise.resolve();
    await Promise.resolve();
    (client as any).failAllPending(new Error("socket closed"));
    await expect(first).rejects.toThrow("socket closed");
    expect(vi.getTimerCount()).toBe(0);

    const second = client.subscribe();
    await Promise.resolve();
    await Promise.resolve();
    expect((client as any).subscribePending).toBeDefined();
    expect(vi.getTimerCount()).toBe(1);

    // The second request remains the only active timer after the first one
    // was rejected and cleaned up.
    await vi.advanceTimersByTimeAsync(9_999);
    const snapshot = { title: "Trip", itinerary: { sections: [] } } as any;
    (client as any).handleFrame(
      { a: "s", c: "TripPlans", d: "tripA", data: { v: 1, data: snapshot } },
      undefined as unknown as NodeJS.Timeout,
      () => {},
      () => {},
    );

    await expect(second).resolves.toBe(snapshot);
    client.close();
  });

  it("resets the client when a subscription acknowledgement times out", async () => {
    const config = {
      wsBaseUrl: "wss://test",
      baseUrl: "https://test",
      userAgent: "test",
    } as any;
    const client = new ShareDBClient(config, "tripA");
    vi.spyOn(client, "connect").mockResolvedValue(undefined);
    vi.spyOn(client as any, "send").mockImplementation(() => {});
    const resetSpy = vi.spyOn(client, "reset").mockImplementation(() => {});

    const pending = client.subscribe();
    const rejection = expect(pending).rejects.toThrow("Subscribe timeout");
    await Promise.resolve();
    await Promise.resolve();
    await vi.advanceTimersByTimeAsync(10_000);

    await rejection;
    expect(resetSpy).toHaveBeenCalledOnce();
  });

});
