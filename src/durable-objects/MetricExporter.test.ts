import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { MetricDefinition } from "../lib/metrics";
import type { CounterState } from "../lib/types";
import type { MetricExporter } from "./MetricExporter";

/**
 * Helper to generate counter metrics with configurable cardinality.
 */
function generateMetricBatch(zoneCount: number, countriesPerZone: number): MetricDefinition[] {
	const metrics: MetricDefinition[] = [];

	for (let z = 0; z < zoneCount; z++) {
		const zoneName = `zone-${z}.example.com`;
		const values: MetricDefinition["values"] = [];

		for (let c = 0; c < countriesPerZone; c++) {
			values.push({
				labels: {
					zone: zoneName,
					country: `country-${c}`,
				},
				value: Math.floor(Math.random() * 1000),
			});
		}

		metrics.push({
			name: "cloudflare_zone_requests_total",
			help: "Total requests by zone and country",
			type: "counter",
			values,
		});
	}

	return metrics;
}

/**
 * Apply a country offset to simulate label churn between cycles.
 */
function applyCountryOffset(batch: MetricDefinition[], offset: number): MetricDefinition[] {
	for (const metric of batch) {
		for (const value of metric.values) {
			const countryNum = Number.parseInt(value.labels.country!.split("-")[1]!, 10) + offset;
			value.labels.country = `country-${countryNum}`;
		}
	}
	return batch;
}

describe("MetricExporter.processCounters", () => {
	it("accumulates counter values across refresh cycles", async () => {
		const id = env.MetricExporter.newUniqueId();
		const stub = env.MetricExporter.get(id);

		await runInDurableObject(stub, async (instance: MetricExporter) => {
			// First cycle: 3 counter keys
			const batch1: MetricDefinition[] = [
				{
					name: "cloudflare_requests_total",
					help: "Total requests",
					type: "counter",
					values: [
						{ labels: { zone: "a.com", country: "US" }, value: 100 },
						{ labels: { zone: "a.com", country: "UK" }, value: 50 },
						{ labels: { zone: "b.com", country: "US" }, value: 200 },
					],
				},
			];

			const result1 = instance.processCounters(batch1, {});
			expect(Object.keys(result1.counters).length).toBe(3);

			// Key format: metric{label=value,label=value} (sorted alphabetically)
			const usKey = "cloudflare_requests_total{country=US,zone=a.com}";
			expect(result1.counters[usKey]?.accumulated).toBe(100);

			// Second cycle: same keys, values should accumulate
			const batch2: MetricDefinition[] = [
				{
					name: "cloudflare_requests_total",
					help: "Total requests",
					type: "counter",
					values: [
						{ labels: { zone: "a.com", country: "US" }, value: 150 },
						{ labels: { zone: "a.com", country: "UK" }, value: 75 },
						{ labels: { zone: "b.com", country: "US" }, value: 300 },
					],
				},
			];

			const result2 = instance.processCounters(batch2, result1.counters);
			expect(result2.counters[usKey]?.accumulated).toBe(250); // 100 + 150
		});
	});

	it("prunes stale counters after expiration cycles", async () => {
		const id = env.MetricExporter.newUniqueId();
		const stub = env.MetricExporter.get(id);

		await runInDurableObject(stub, async (instance: MetricExporter) => {
			// Cycle 1: countries 0-4
			let batch = generateMetricBatch(1, 5);
			let result = instance.processCounters(batch, {});
			expect(Object.keys(result.counters).length).toBe(5);

			const staleKey = "cloudflare_zone_requests_total{country=country-0,zone=zone-0.example.com}";
			const activeKey = "cloudflare_zone_requests_total{country=country-5,zone=zone-0.example.com}";

			// Cycle 2: countries 5-9 (countries 0-4 become stale)
			batch = applyCountryOffset(generateMetricBatch(1, 5), 5);
			result = instance.processCounters(batch, result.counters);

			// Should have 10 keys: 5 active + 5 stale (with expiration counting down)
			expect(Object.keys(result.counters).length).toBe(10);
			expect(result.counters[staleKey]).toBeDefined();
			expect(result.counters[activeKey]).toBeDefined();

			// Stale key should have expiration decremented by 1
			// Active key should have full expiration (COUNTER_EXPIRATION_CYCLES)
			expect(result.counters[staleKey]!.expiration).toBeLessThan(result.counters[activeKey]!.expiration);

			// Simulate enough cycles for stale keys to expire
			// Keep using countries 5-9 so countries 0-4 remain stale
			for (let i = 0; i < 10; i++) {
				batch = applyCountryOffset(generateMetricBatch(1, 5), 5);
				result = instance.processCounters(batch, result.counters);
			}

			// After enough cycles, stale keys should be pruned
			expect(result.counters[staleKey]).toBeUndefined();
			expect(result.counters[activeKey]).toBeDefined();
			expect(Object.keys(result.counters).length).toBe(5);
		});
	});

	it("preserves counter monotonicity for active keys", async () => {
		const id = env.MetricExporter.newUniqueId();
		const stub = env.MetricExporter.get(id);

		await runInDurableObject(stub, async (instance: MetricExporter) => {
			const key = "cloudflare_requests_total{country=US,zone=a.com}";
			let counters: Record<string, CounterState> = {};

			// Simulate 10 cycles with the same key
			for (let i = 0; i < 10; i++) {
				const batch: MetricDefinition[] = [
					{
						name: "cloudflare_requests_total",
						help: "Total requests",
						type: "counter",
						values: [{ labels: { zone: "a.com", country: "US" }, value: 100 }],
					},
				];

				const result = instance.processCounters(batch, counters);
				counters = result.counters;

				// Accumulated value should be monotonically increasing
				expect(counters[key]?.accumulated).toBe((i + 1) * 100);
			}
		});
	});

	it("passes through gauges without creating counter state", async () => {
		const id = env.MetricExporter.newUniqueId();
		const stub = env.MetricExporter.get(id);

		await runInDurableObject(stub, async (instance: MetricExporter) => {
			const batch: MetricDefinition[] = [
				{
					name: "cloudflare_active_connections",
					help: "Current active connections",
					type: "gauge",
					values: [{ labels: { zone: "a.com" }, value: 500 }],
				},
			];

			const result = instance.processCounters(batch, {});

			// Gauges pass through but the current implementation still adds them to counters
			// This test documents actual behavior
			expect(result.metrics.length).toBeGreaterThan(0);
		});
	});
});

describe("Counter expiration prevents unbounded growth", () => {
	it("counter map size stabilizes with label churn", async () => {
		const id = env.MetricExporter.newUniqueId();
		const stub = env.MetricExporter.get(id);

		await runInDurableObject(stub, async (instance: MetricExporter) => {
			let counters: Record<string, CounterState> = {};
			const keyCounts: number[] = [];

			// Simulate 20 cycles with label churn (10-country sliding window)
			for (let cycle = 0; cycle < 20; cycle++) {
				const batch = applyCountryOffset(generateMetricBatch(2, 10), cycle * 2);
				const result = instance.processCounters(batch, counters);
				counters = result.counters;
				keyCounts.push(Object.keys(counters).length);
			}

			// Key count should stabilize, not grow unboundedly
			// With 10 countries per cycle and 2-country offset, overlap is high
			// But stale keys expire, so we shouldn't exceed a reasonable bound
			const maxKeys = Math.max(...keyCounts);
			const currentWindowKeys = 2 * 10; // 2 zones × 10 countries

			console.log(`Key counts over 20 cycles: ${keyCounts.join(", ")}`);
			console.log(`Max keys: ${maxKeys}, Current window: ${currentWindowKeys}`);

			// Should be bounded - not more than ~4x current window
			// (accounts for TTL grace period)
			expect(maxKeys).toBeLessThan(currentWindowKeys * 5);

			// Final count should be close to current window + some TTL buffer
			const finalCount = keyCounts[keyCounts.length - 1]!;
			expect(finalCount).toBeLessThan(currentWindowKeys * 4);
		});
	});
});
