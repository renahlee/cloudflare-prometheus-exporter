import { z } from "zod";

const CHUNK_BYTES = 100 * 1024;
const MAX_KEYS_PER_OPERATION = 128;
const MAX_SERIALIZED_BYTES = 16 * 1024 * 1024;
const MAX_CHUNKS = Math.ceil(MAX_SERIALIZED_BYTES / CHUNK_BYTES);
const FORMAT_JSON = "chunked-json-v1";
const FORMAT_GZIP = "chunked-gzip-v1";

const ChunkManifestSchema = z.object({
	format: z.union([z.literal(FORMAT_JSON), z.literal(FORMAT_GZIP)]),
	generation: z.union([z.literal(0), z.literal(1)]),
	chunks: z.number().int().positive().max(MAX_CHUNKS),
	bytes: z.number().int().positive().max(MAX_SERIALIZED_BYTES).optional(),
});

type ChunkManifest = z.infer<typeof ChunkManifestSchema>;

export interface ChunkedValueStorage {
	get(key: string): Promise<unknown>;
	getMany(keys: string[]): Promise<Map<string, unknown>>;
	putMany(entries: Record<string, unknown>): Promise<void>;
	deleteMany(keys: string[]): Promise<void>;
}

/** Adapts Durable Object storage to the minimal chunked-value interface. */
export function chunkedDurableObjectStorage(
	storage: DurableObjectStorage,
): ChunkedValueStorage {
	return {
		get: (key) => storage.get(key, { noCache: true }),
		getMany: (keys) => storage.get(keys, { noCache: true }),
		putMany: (entries) => storage.put(entries, { noCache: true }),
		deleteMany: async (keys) => {
			await storage.delete(keys);
		},
	};
}

function manifestKey(baseKey: string): string {
	return `${baseKey}:manifest`;
}

function isChunkFormat(value: unknown): boolean {
	return (
		typeof value === "object" &&
		value !== null &&
		"format" in value &&
		(value.format === FORMAT_JSON || value.format === FORMAT_GZIP)
	);
}

function parseManifest(value: unknown): ChunkManifest | undefined {
	if (!isChunkFormat(value)) return undefined;
	return ChunkManifestSchema.parse(value);
}

function chunkKey(baseKey: string, generation: number, index: number): string {
	return `${baseKey}:chunk:${generation}:${index}`;
}

function pendingKey(baseKey: string, generation: number): string {
	return `${baseKey}:pending:${generation}`;
}

function chunkKeys(baseKey: string, manifest: ChunkManifest): string[] {
	return Array.from({ length: manifest.chunks }, (_, index) =>
		chunkKey(baseKey, manifest.generation, index),
	);
}

function batches<T>(items: T[]): T[][] {
	const result: T[][] = [];
	for (let index = 0; index < items.length; index += MAX_KEYS_PER_OPERATION) {
		result.push(items.slice(index, index + MAX_KEYS_PER_OPERATION));
	}
	return result;
}

async function cleanupPendingGeneration(
	storage: ChunkedValueStorage,
	baseKey: string,
	generation: number,
): Promise<void> {
	const key = pendingKey(baseKey, generation);
	const stored = await storage.get(key);
	if (stored === undefined) return;
	const pending = ChunkManifestSchema.parse(stored);
	for (const keyBatch of batches(chunkKeys(baseKey, pending))) {
		await storage.deleteMany(keyBatch);
	}
	await storage.deleteMany([key]);
}

async function readCurrentValues(
	storage: ChunkedValueStorage,
	baseKey: string,
): Promise<{ base: unknown; manifest: ChunkManifest | undefined }> {
	const pointerKey = manifestKey(baseKey);
	const values = await storage.getMany([baseKey, pointerKey]);
	const pointer = values.get(pointerKey);
	if (pointer !== undefined) {
		return {
			base: values.get(baseKey),
			manifest: ChunkManifestSchema.parse(pointer),
		};
	}
	const base = values.get(baseKey);
	return { base, manifest: parseManifest(base) };
}

/** Compress a Uint8Array using gzip via the Web Streams API. */
async function gzipCompress(data: Uint8Array): Promise<Uint8Array> {
	const stream = new Blob([data])
		.stream()
		.pipeThrough(new CompressionStream("gzip"));
	return new Uint8Array(await new Response(stream).arrayBuffer());
}

/** Decompress a gzip-compressed Uint8Array via the Web Streams API. */
async function gzipDecompress(data: Uint8Array): Promise<Uint8Array> {
	try {
		const stream = new Blob([data])
			.stream()
			.pipeThrough(new DecompressionStream("gzip"));
		return new Uint8Array(await new Response(stream).arrayBuffer());
	} catch (cause) {
		throw new Error("Failed to decompress gzip chunk data", { cause });
	}
}

/** Loads a chunked value, or state written by an older unchunked exporter. */
export async function loadChunkedValue<T>(
	storage: ChunkedValueStorage,
	baseKey: string,
	schema: z.ZodType<T>,
): Promise<T | undefined> {
	const current = await readCurrentValues(storage, baseKey);
	if (current.manifest === undefined) {
		return current.base === undefined ? undefined : schema.parse(current.base);
	}

	const keys = chunkKeys(baseKey, current.manifest);
	const chunkArrays: Uint8Array[] = [];
	let byteLength = 0;
	for (const keyBatch of batches(keys)) {
		const values = await storage.getMany(keyBatch);
		for (const key of keyBatch) {
			const value = values.get(key);
			if (!(value instanceof Uint8Array)) {
				throw new Error(`Missing state chunk: ${key}`);
			}
			byteLength += value.byteLength;
			if (byteLength > MAX_SERIALIZED_BYTES) {
				throw new RangeError(
					"Chunked storage value exceeds the safe size limit",
				);
			}
			chunkArrays.push(value);
		}
	}
	if (
		current.manifest.bytes !== undefined &&
		byteLength !== current.manifest.bytes
	) {
		throw new Error("Chunked storage value has an invalid byte length");
	}

	// Reassemble chunks into a single buffer
	const assembled = new Uint8Array(byteLength);
	let offset = 0;
	for (const chunk of chunkArrays) {
		assembled.set(chunk, offset);
		offset += chunk.byteLength;
	}

	// Decompress if stored with gzip format, otherwise decode directly
	const jsonBytes =
		current.manifest.format === FORMAT_GZIP
			? await gzipDecompress(assembled)
			: assembled;
	if (jsonBytes.byteLength > MAX_SERIALIZED_BYTES) {
		throw new RangeError(
			"Decompressed chunked storage value exceeds the safe size limit",
		);
	}

	const serialized = new TextDecoder().decode(jsonBytes);
	const parsed: unknown = JSON.parse(serialized);
	return schema.parse(parsed);
}

/**
 * Persists a value in bounded chunks and atomically switches a manifest pointer.
 * Values larger than a single chunk are gzip-compressed before chunking to
 * reduce storage footprint for high-cardinality metric state.
 *
 * The legacy base value is retained while state is large so that a rollback to
 * the *previous* exporter version can load the last small valid snapshot.
 * Note: rolling back past the gzip migration is not supported — an older exporter
 * that only accepts `chunked-json-v1` will fail to parse the `chunked-gzip-v1`
 * manifest. In that scenario the `state:manifest` key must be deleted manually
 * from DO storage to fall back to the legacy base value.
 */
export async function saveChunkedValue(
	storage: ChunkedValueStorage,
	baseKey: string,
	value: unknown,
): Promise<void> {
	const current = await readCurrentValues(storage, baseKey);
	const previousManifest = current.manifest;
	const generation = previousManifest?.generation === 0 ? 1 : 0;
	const json = JSON.stringify(value);
	if (json === undefined) {
		throw new TypeError("Chunked storage value must be JSON serializable");
	}
	const rawBytes = new TextEncoder().encode(json);

	// Small values are stored directly without chunking or compression
	if (rawBytes.byteLength <= CHUNK_BYTES) {
		if (previousManifest === undefined) {
			await cleanupPendingGeneration(storage, baseKey, 0);
			await cleanupPendingGeneration(storage, baseKey, 1);
		} else {
			await storage.putMany({
				[pendingKey(baseKey, previousManifest.generation)]: previousManifest,
			});
		}
		await storage.putMany({ [baseKey]: value });
		await storage.deleteMany([manifestKey(baseKey)]);
		if (previousManifest !== undefined) {
			await cleanupPendingGeneration(
				storage,
				baseKey,
				previousManifest.generation,
			);
		}
		return;
	}

	// Compress before chunking to maximize storage headroom
	const compressed = await gzipCompress(rawBytes);
	if (compressed.byteLength > MAX_SERIALIZED_BYTES) {
		throw new RangeError("Chunked storage value exceeds the safe size limit");
	}

	const nextManifest: ChunkManifest = {
		format: FORMAT_GZIP,
		generation,
		chunks: Math.ceil(compressed.byteLength / CHUNK_BYTES),
		bytes: compressed.byteLength,
	};
	const nextPendingKey = pendingKey(baseKey, generation);
	if (previousManifest === undefined) {
		await cleanupPendingGeneration(storage, baseKey, 0);
		await cleanupPendingGeneration(storage, baseKey, 1);
	} else {
		await cleanupPendingGeneration(storage, baseKey, generation);
	}

	await storage.putMany({
		[nextPendingKey]: nextManifest,
		...(previousManifest === undefined
			? {}
			: {
					[pendingKey(baseKey, previousManifest.generation)]: previousManifest,
				}),
	});

	for (
		let firstChunk = 0;
		firstChunk < nextManifest.chunks;
		firstChunk += MAX_KEYS_PER_OPERATION
	) {
		const entries: Record<string, unknown> = {};
		const lastChunk = Math.min(
			firstChunk + MAX_KEYS_PER_OPERATION,
			nextManifest.chunks,
		);
		for (let index = firstChunk; index < lastChunk; index++) {
			entries[chunkKey(baseKey, generation, index)] = compressed.slice(
				index * CHUNK_BYTES,
				(index + 1) * CHUNK_BYTES,
			);
		}
		await storage.putMany(entries);
	}

	await storage.putMany({ [manifestKey(baseKey)]: nextManifest });

	if (previousManifest !== undefined) {
		await cleanupPendingGeneration(
			storage,
			baseKey,
			previousManifest.generation,
		);
	}
	await storage.deleteMany([nextPendingKey]);
}
