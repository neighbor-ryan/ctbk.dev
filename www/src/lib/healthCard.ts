import type { FeedHealth } from './feedHealth';

const escape = (text: string) => text.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&apos;' }[c]!));
const text = (x: number, y: number, body: string, size = 20, fill = '#d8e2ed') =>
	`<text x="${x}" y="${y}" font-size="${size}" fill="${fill}">${escape(body)}</text>`;
const colors = ['#8fafff', '#ba9dff', '#54d6a1'];

/** Shared by the health page preview and PNG renderer. All inputs are bounded
 * health metadata; this image never runs historical R2 scans. */
export function healthCardSvg(snapshot: { generatedAt: number; feed: FeedHealth }): string {
	const history = new Map((snapshot.feed.coverageHistory ?? []).map((day) => [day.date, day]));
	const days = Array.from({ length: 7 }, (_, i) => new Date((snapshot.generatedAt - (6 - i) * 86400) * 1000).toISOString().slice(0, 10));
	const c = snapshot.feed.coverage;
	const today = c?.today;
	const updated = new Date(snapshot.generatedAt * 1000).toISOString().slice(0, 16).replace('T', ' ');
	const stale = Date.now() / 1000 - snapshot.generatedAt > 900;
	const heading = stale ? 'Health snapshot stale' : c?.complete ? today!.settled - today!.combined > 0 ? 'Combined coverage gaps' : today!.recovered > 0 ? 'Backup protecting minute coverage' : 'Combined minute coverage intact' : 'Combined coverage unknown';
	const pieces = [
		'<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630">',
		'<rect width="1200" height="630" rx="18" fill="#111a25"/><g font-family="Inter, sans-serif">',
		text(40, 52, 'GBFS · FEED HEALTH', 23, '#91a5ba'), text(40, 100, heading, 34, '#ffffff'),
		text(40, 137, `Updated ${updated} UTC · latest primary lag ${snapshot.feed.drift?.latestS ?? 'unknown'}s`, 19, '#91a5ba'),
	];
	if (today && c?.complete) pieces.push(
		text(40, 180, `${today.combined}/${today.settled} slots observed · ${today.recovered} recovered by backup · ${today.settled - today.combined} combined gaps`, 24),
	);
	pieces.push(text(40, 222, 'Recent UTC days · rows: primary / backup / combined · gray: unmeasured', 18, '#91a5ba'));
	const barX = 220, hourW = 37;
	for (let i = 0; i < days.length; i++) {
		const date = days[i], day = history.get(date), y = 242 + i * 43;
		pieces.push(text(40, y + 19, date, 18, day ? '#d8e2ed' : '#718297'));
		for (let h = 0; h < 24; h++) {
			const hour = day?.hours[h] ?? [0, 0, 0, 0];
			for (let row = 0; row < 3; row++) {
				const expected = hour[0], observed = hour[row + 1];
				const fill = expected === 0 ? '#283342' : observed < expected ? '#f06f72' : colors[row];
				const opacity = expected > 0 ? 0.3 + 0.7 * observed / expected : 1;
				pieces.push(`<rect x="${barX + h * hourW}" y="${y + row * 10}" width="${hourW - 2}" height="8" fill="${fill}" opacity="${opacity.toFixed(2)}"/>`);
			}
		}
	}
	pieces.push(text(220, 563, '00', 16, '#91a5ba'), text(220 + 12 * hourW, 563, '12', 16, '#91a5ba'), text(220 + 23 * hourW, 563, '23 UTC', 16, '#91a5ba'));
	pieces.push(text(40, 603, 'Minute slots are observations, not every upstream generation. Empty slots can reflect timestamp phase shifts.', 17, '#91a5ba'), '</g></svg>');
	return pieces.join('');
}
