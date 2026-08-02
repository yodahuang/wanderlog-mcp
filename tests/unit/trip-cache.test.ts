import { EventEmitter } from "node:events";
import { describe, expect, it } from "vitest";
import { TripCache } from "../../src/cache/trip-cache.ts";
import type { RestClient } from "../../src/transport/rest.ts";
import type { ShareDBPool } from "../../src/transport/sharedb.ts";

class FakeShareDBClient extends EventEmitter {
  public version = 1;
  public isSubscribed = true;
  public subscribeCalled = 0;
  public resetCalled = 0;

  async subscribe() {
    this.subscribeCalled++;
    this.isSubscribed = true;
    return { title: "Test Trip", sections: [] };
  }

  reset() {
    this.resetCalled++;
    this.isSubscribed = false;
  }
}

describe("TripCache event listener leak", () => {
  it("unregisters remoteOp listener on invalidate", async () => {
    const fakeClient = new FakeShareDBClient();
    const fakeRest = {
      getTripWithResources: async () => ({ geos: [] }),
    } as unknown as RestClient;

    const fakePool = {
      get: () => fakeClient,
    } as unknown as ShareDBPool;

    const cache = new TripCache(fakeRest, fakePool);

    // Initial get, should subscribe and register listener
    await cache.get("tripA");
    expect(fakeClient.listenerCount("remoteOp")).toBe(1);

    // Invalidate, should unregister listener
    cache.invalidate("tripA");
    expect(fakeClient.listenerCount("remoteOp")).toBe(0);
    expect(fakeClient.resetCalled).toBe(1);
  });

  it("handles multiple get/invalidate cycles without leaking listeners", async () => {
    const fakeClient = new FakeShareDBClient();
    const fakeRest = {
      getTripWithResources: async () => ({ geos: [] }),
    } as unknown as RestClient;

    const fakePool = {
      get: () => fakeClient,
    } as unknown as ShareDBPool;

    const cache = new TripCache(fakeRest, fakePool);

    for (let i = 0; i < 5; i++) {
      await cache.get("tripA");
      expect(fakeClient.listenerCount("remoteOp")).toBe(1);
      cache.invalidate("tripA");
      expect(fakeClient.listenerCount("remoteOp")).toBe(0);
    }
  });

  it("unregisters listener on clear", async () => {
    const fakeClient1 = new FakeShareDBClient();
    const fakeClient2 = new FakeShareDBClient();
    const fakeRest = {
      getTripWithResources: async () => ({ geos: [] }),
    } as unknown as RestClient;

    const fakePool = {
      get: (tripKey: string) => (tripKey === "tripA" ? fakeClient1 : fakeClient2),
    } as unknown as ShareDBPool;

    const cache = new TripCache(fakeRest, fakePool);

    await cache.get("tripA");
    await cache.get("tripB");

    expect(fakeClient1.listenerCount("remoteOp")).toBe(1);
    expect(fakeClient2.listenerCount("remoteOp")).toBe(1);

    cache.clear();

    expect(fakeClient1.listenerCount("remoteOp")).toBe(0);
    expect(fakeClient2.listenerCount("remoteOp")).toBe(0);
  });

  it("invalidates the cached snapshot when the live client closes", async () => {
    const fakeClient = new FakeShareDBClient();
    const fakeRest = {
      getTripWithResources: async () => ({ geos: [] }),
    } as unknown as RestClient;
    const fakePool = {
      get: () => fakeClient,
    } as unknown as ShareDBPool;
    const cache = new TripCache(fakeRest, fakePool);

    await cache.get("tripA");
    expect(fakeClient.listenerCount("closed")).toBe(1);

    fakeClient.isSubscribed = false;
    fakeClient.emit("closed", 1006);

    expect(fakeClient.listenerCount("remoteOp")).toBe(0);
    expect(fakeClient.listenerCount("closed")).toBe(0);

    await cache.get("tripA");
    expect(fakeClient.subscribeCalled).toBe(2);
  });

  it("resets the client so invalidation cannot reuse its old snapshot", async () => {
    const fakeClient = new FakeShareDBClient();
    const fakeRest = {
      getTripWithResources: async () => ({ geos: [] }),
    } as unknown as RestClient;
    const fakePool = {
      get: () => fakeClient,
    } as unknown as ShareDBPool;
    const cache = new TripCache(fakeRest, fakePool);

    await cache.get("tripA");
    cache.invalidate("tripA");

    expect(fakeClient.isSubscribed).toBe(false);
    await cache.get("tripA");
    expect(fakeClient.subscribeCalled).toBe(2);
  });
});
