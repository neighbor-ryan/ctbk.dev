import type { MinuteRecord } from './avail-monoid';

/** Minute aliases serve the normalized series; immutable generations retain
 * every observed LU, including two LUs that fall in the same minute. */
export async function statusContentHash(record: Pick<MinuteRecord, 'ts' | 'stations'>): Promise<string> {
	const bytes = new TextEncoder().encode(JSON.stringify({ ts: record.ts, stations: record.stations }));
	const digest = await crypto.subtle.digest('SHA-256', bytes);
	return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

export async function writeStatusSnapshot(
	bucket: R2Bucket,
	record: MinuteRecord,
	contentHash?: string,
): Promise<string> {
	const hash = contentHash ?? await statusContentHash(record);
	const date = new Date(record.ts * 1000).toISOString().slice(0, 10);
	const minute = new Date(record.ts * 1000).toISOString().slice(11, 16).replace(':', '-');
	let generationKey = `gbfs/status-generation/${date}/${record.ts}.json`;
	const options = {
		onlyIf: { etagDoesNotMatch: '*' },
		httpMetadata: { contentType: 'application/json' },
		customMetadata: { ts: String(record.ts), content_hash: hash },
	};
	const stored = await bucket.put(generationKey, JSON.stringify(record), options);
	if (!stored) {
		const prior = await bucket.head(generationKey);
		if (!prior) throw new Error(`generation missing after conditional put: ${generationKey}`);
		if (prior.customMetadata?.content_hash !== hash) {
			// Same upstream LU with different contents must not silently erase either
			// observation. Preserve the variant; diagnostics can inspect both hashes.
			generationKey = `gbfs/status-generation/${date}/${record.ts}-${hash}.json`;
			await bucket.put(generationKey, JSON.stringify(record), options);
			console.warn(`GBFS content changed for LU ${record.ts}; retained ${generationKey}`);
		}
	}

	const alias = `gbfs/status/${date}/${minute}.json`;
	// Multiple cron isolates may finish out of order. CAS makes the alias
	// latest-LU-per-minute, rather than latest network completion per minute.
	for (let attempt = 0; attempt < 5; attempt++) {
		const prior = await bucket.head(alias);
		if (prior) {
			const ts = prior.customMetadata?.ts;
			const previous = ts === undefined ? await bucket.get(alias) : null;
			const priorTs = ts === undefined ? (await previous?.json<MinuteRecord>())?.ts : Number(ts);
			if (priorTs !== undefined && priorTs >= record.ts) return alias;
		}
		const written = await bucket.put(alias, JSON.stringify({ ...record, generation_key: generationKey }), {
			onlyIf: prior ? { etagMatches: prior.etag } : { etagDoesNotMatch: '*' },
			httpMetadata: { contentType: 'application/json' },
			customMetadata: { ts: String(record.ts), generation_key: generationKey },
		});
		if (written) return alias;
	}
	throw new Error(`minute alias contention after 5 attempts: ${alias}`);
}
