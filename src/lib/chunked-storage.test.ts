import { describe, expect, it } from "vitest";
import { z } from "zod";
import {
	type ChunkedValueStorage,
	loadChunkedValue,
	saveChunkedValue,
} from "./chunked-storage";

class SizeLimitedMemoryStorage implements ChunkedValueStorage {
	readonly values = new Map<string, unknown>();
	writesBeforeFailure: number | undefined;

	constructor(private readonly maximumValueBytes: number) {}

	async get(key: string): Promise<unknown> {
		return this.values.get(key);
	}

	async getMany(keys: string[]): Promise<Map<string, unknown>> {
		const values = new Map<string, unknown>();
		for (const key of keys) {
			if (this.values.has(key)) {
				values.set(key, this.values.get(key));
			}
		}
		return values;
	}

	async putMany(entries: Record<string, unknown>): Promise<void> {
		if (this.writesBeforeFailure === 0) {
			this.writesBeforeFailure = undefined;
			throw new Error("simulated storage failure");
		}
		if (this.writesBeforeFailure !== undefined) {
			this.writesBeforeFailure--;
		}
		for (const value of Object.values(entries)) {
			const size =
				value instanceof Uint8Array
					? value.byteLength
					: new TextEncoder().encode(JSON.stringify(value)).byteLength;
			if (size > this.maximumValueBytes) {
				throw new RangeError("storage value is too large");
			}
		}
		for (const [key, value] of Object.entries(entries)) {
			this.values.set(key, value);
		}
	}

	async deleteMany(keys: string[]): Promise<void> {
		for (const key of keys) {
			this.values.delete(key);
		}
	}
}

describe("chunked storage", () => {
	it("round-trips state larger than one storage value", async () => {
		const storage = new SizeLimitedMemoryStorage(110_000);
		const state = {
			metrics: [{ name: "large", payload: "🔥".repeat(80_000) }],
			counters: { requests: 42 },
		};

		await saveChunkedValue(storage, "state", state);
		const restored = await loadChunkedValue(storage, "state", z.unknown());

		expect(restored).toEqual(state);
	});

	it("keeps the legacy base value as a rollback snapshot", async () => {
		const storage = new SizeLimitedMemoryStorage(110_000);
		const legacyState = { payload: "legacy" };
		storage.values.set("state", legacyState);

		await saveChunkedValue(storage, "state", { payload: "x".repeat(300_000) });

		expect(storage.values.get("state")).toEqual(legacyState);
		expect(await loadChunkedValue(storage, "state", z.unknown())).toEqual({
			payload: "x".repeat(300_000),
		});
	});

	it("reads state written by exporter versions before chunking", async () => {
		const storage = new SizeLimitedMemoryStorage(110_000);
		const legacyState = { counters: { requests: 42 } };
		storage.values.set("state", legacyState);

		expect(await loadChunkedValue(storage, "state", z.unknown())).toEqual(
			legacyState,
		);
	});

	it("rejects legacy state that does not match the caller's schema", async () => {
		const storage = new SizeLimitedMemoryStorage(110_000);
		storage.values.set("state", { counters: "corrupt" });
		const schema = z.object({
			counters: z.record(z.string(), z.number()),
		});

		await expect(loadChunkedValue(storage, "state", schema)).rejects.toThrow();
	});

	it("rejects a malformed manifest before allocating chunk keys", async () => {
		const storage = new SizeLimitedMemoryStorage(110_000);
		storage.values.set("state:manifest", {
			format: "chunked-json-v1",
			generation: 0,
			chunks: Number.MAX_SAFE_INTEGER,
		});

		await expect(
			loadChunkedValue(storage, "state", z.unknown()),
		).rejects.toThrow();
	});

	it("rejects a chunked value when one of its chunks is missing", async () => {
		const storage = new SizeLimitedMemoryStorage(110_000);
		await saveChunkedValue(storage, "state", { payload: "x".repeat(300_000) });
		const chunkKey = [...storage.values.keys()].find((key) =>
			key.startsWith("state:chunk:"),
		);
		expect(chunkKey).toBeDefined();
		if (chunkKey !== undefined) {
			storage.values.delete(chunkKey);
		}

		await expect(
			loadChunkedValue(storage, "state", z.unknown()),
		).rejects.toThrow("Missing state chunk");
	});

	it("rejects chunked state that does not match the caller's schema", async () => {
		const storage = new SizeLimitedMemoryStorage(110_000);
		await saveChunkedValue(storage, "state", { counters: "corrupt" });
		const schema = z.object({
			counters: z.record(z.string(), z.number()),
		});

		await expect(loadChunkedValue(storage, "state", schema)).rejects.toThrow();
	});

	it("replaces old chunks instead of accumulating storage generations", async () => {
		const storage = new SizeLimitedMemoryStorage(110_000);
		await saveChunkedValue(storage, "state", { payload: "x".repeat(300_000) });
		await saveChunkedValue(storage, "state", { payload: "small" });

		expect(await loadChunkedValue(storage, "state", z.unknown())).toEqual({
			payload: "small",
		});
		expect(storage.values.size).toBe(1);
	});

	it("keeps the previous generation readable when a replacement write fails", async () => {
		const storage = new SizeLimitedMemoryStorage(110_000);
		await saveChunkedValue(storage, "state", { payload: "previous" });
		storage.writesBeforeFailure = 0;

		await expect(
			saveChunkedValue(storage, "state", { payload: "replacement" }),
		).rejects.toThrow("simulated storage failure");
		expect(await loadChunkedValue(storage, "state", z.unknown())).toEqual({
			payload: "previous",
		});
	});

	it("cleans chunks from a failed multi-batch write when retried", async () => {
		const storage = new SizeLimitedMemoryStorage(110_000);
		await saveChunkedValue(storage, "state", { payload: "previous" });
		storage.writesBeforeFailure = 2;
		await expect(
			saveChunkedValue(storage, "state", {
				payload: "x".repeat(13_200_000),
			}),
		).rejects.toThrow("simulated storage failure");
		expect(
			[...storage.values.keys()].some((key) => key.startsWith("state:chunk:")),
		).toBe(true);

		await saveChunkedValue(storage, "state", { payload: "replacement" });

		expect(await loadChunkedValue(storage, "state", z.unknown())).toEqual({
			payload: "replacement",
		});
		expect(storage.values.size).toBe(1);
	});

	it("cleans pending state when a failed replacement is retried", async () => {
		const storage = new SizeLimitedMemoryStorage(110_000);
		await saveChunkedValue(storage, "state", { payload: "previous" });
		storage.writesBeforeFailure = 1;
		await expect(
			saveChunkedValue(storage, "state", { payload: "x".repeat(300_000) }),
		).rejects.toThrow("simulated storage failure");

		await saveChunkedValue(storage, "state", { payload: "replacement" });

		expect(await loadChunkedValue(storage, "state", z.unknown())).toEqual({
			payload: "replacement",
		});
		expect(storage.values.size).toBe(1);
	});

	it("compresses multi-chunk state with gzip", async () => {
		const storage = new SizeLimitedMemoryStorage(110_000);
		// Use varied content to produce realistic (not 1000:1) compression
		const payload = Array.from({ length: 5000 }, (_, i) => ({
			key: `metric_${i}`,
			labels: { zone: `zone-${i % 10}.example.com`, colo: `colo-${i % 300}` },
			value: (((i * 2654435761) >>> 0) % 100000) / 100,
		}));
		const state = { metrics: payload };

		await saveChunkedValue(storage, "state", state);
		const restored = await loadChunkedValue(storage, "state", z.unknown());

		expect(restored).toEqual(state);
		// Verify manifest indicates gzip format
		const manifest = storage.values.get("state:manifest");
		expect(manifest).toBeDefined();
		expect(manifest).toMatchObject({ format: "chunked-gzip-v1" });
	});

	it("reads old chunked-json-v1 state written before compression was added", async () => {
		const storage = new SizeLimitedMemoryStorage(110_000);
		// Manually construct a chunked-json-v1 manifest and raw UTF-8 chunks
		const state = {
			counters: { requests: 42 },
			metrics: ["a".repeat(200_000)],
		};
		const json = JSON.stringify(state);
		const encoded = new TextEncoder().encode(json);
		const chunkSize = 100 * 1024;
		const chunkCount = Math.ceil(encoded.byteLength / chunkSize);

		// Write raw (uncompressed) chunks with v1 manifest
		const manifest = {
			format: "chunked-json-v1",
			generation: 0,
			chunks: chunkCount,
			bytes: encoded.byteLength,
		};
		await storage.putMany({ "state:manifest": manifest });
		for (let i = 0; i < chunkCount; i++) {
			const chunk = encoded.slice(i * chunkSize, (i + 1) * chunkSize);
			await storage.putMany({ [`state:chunk:0:${i}`]: chunk });
		}

		const restored = await loadChunkedValue(storage, "state", z.unknown());
		expect(restored).toEqual(state);
	});

	it("migrates chunked-json-v1 state to chunked-gzip-v1 on re-save", async () => {
		const storage = new SizeLimitedMemoryStorage(110_000);
		// Seed chunked-json-v1 data manually (simulating pre-gzip exporter)
		const state = {
			counters: { requests: 42 },
			metrics: ["b".repeat(200_000)],
		};
		const json = JSON.stringify(state);
		const encoded = new TextEncoder().encode(json);
		const chunkSize = 100 * 1024;
		const chunkCount = Math.ceil(encoded.byteLength / chunkSize);

		const manifest = {
			format: "chunked-json-v1",
			generation: 0,
			chunks: chunkCount,
			bytes: encoded.byteLength,
		};
		await storage.putMany({ "state:manifest": manifest });
		for (let i = 0; i < chunkCount; i++) {
			const chunk = encoded.slice(i * chunkSize, (i + 1) * chunkSize);
			await storage.putMany({ [`state:chunk:0:${i}`]: chunk });
		}

		// Load v1 data and re-save it (production migration path)
		const loaded = await loadChunkedValue(storage, "state", z.unknown());
		expect(loaded).toEqual(state);

		await saveChunkedValue(storage, "state", loaded);

		// Verify manifest upgraded to gzip format
		const updatedManifest = storage.values.get("state:manifest");
		expect(updatedManifest).toBeDefined();
		expect(updatedManifest).toMatchObject({ format: "chunked-gzip-v1" });

		// Verify data is intact after migration
		const reloaded = await loadChunkedValue(storage, "state", z.unknown());
		expect(reloaded).toEqual(state);
	});
});
