/** One editable monthly OP, weekly rollups, and one editable reply per UTC day.
 * Routine flaps stay in the daily reply; critical incidents alone broadcast. */
import { SlackClient } from '@rdub/thrds';
import type { FeedCoverageDay, HealthSnapshot } from './health';
import type { FiringEntry, Transition } from './alerts';

export interface DayCondition {
	starts: number;
	active: boolean;
	changedAt: string;
	text: string;
}
export interface HealthReportDay {
	coverage: FeedCoverageDay | null;
	conditions: Record<string, DayCondition>;
	messageTs?: string;
	body?: string;
}
export interface HealthReportState {
	month: string;
	threadTs?: string;
	updatedAt?: string;
	days: Record<string, HealthReportDay>;
}
export interface HealthSlack {
	post(text: string, threadTs?: string): Promise<{ id: string }>;
	edit(ts: string, text: string): Promise<unknown>;
	postReport(text: string, imageUrl: string): Promise<{ id: string }>;
	editReport(ts: string, text: string, imageUrl: string): Promise<unknown>;
	postUrgent(text: string, threadTs: string): Promise<{ id: string }>;
}

export function reportBlocks(text: string, imageUrl: string) {
	return [
		{ type: 'section', text: { type: 'mrkdwn', text } },
		{ type: 'image', image_url: imageUrl, alt_text: 'Recent GBFS minute coverage: primary, backup, combined. Gaps and backup recovery are separate.' },
	];
}

/** The TS thrds transport has plain post/edit; Python thrds lifts Markdown
 * image lines into these same editable image blocks. Reuse its request/retry path. */
export class HealthSlackClient extends SlackClient implements HealthSlack {
	async postReport(text: string, imageUrl: string): Promise<{ id: string }> {
		const result = await this.request('chat.postMessage', {
			channel: this.channel, text, blocks: reportBlocks(text, imageUrl),
			username: this.username, icon_emoji: this.iconEmoji,
			unfurl_links: false, unfurl_media: false,
		});
		return { id: result.ts as string };
	}
	async editReport(ts: string, text: string, imageUrl: string): Promise<void> {
		await this.request('chat.update', {
			channel: this.channel, ts, text, blocks: reportBlocks(text, imageUrl),
			unfurl_links: false, unfurl_media: false,
		});
	}
	async postUrgent(text: string, threadTs: string): Promise<{ id: string }> {
		const result = await this.request('chat.postMessage', {
			channel: this.channel, text, thread_ts: threadTs, reply_broadcast: true,
			username: this.username, icon_emoji: this.iconEmoji,
			unfurl_links: false, unfurl_media: false,
		});
		return { id: result.ts as string };
	}
}

const DATE = (iso: string) => iso.slice(0, 10);
const stamp = (iso: string) => `${iso.slice(0, 10)} ${iso.slice(11, 16)} UTC`;
const freshDay = (): HealthReportDay => ({ coverage: null, conditions: {} });
const coverageText = (c: FeedCoverageDay) =>
	`${c.combined}/${c.settled} minute slots observed · primary ${c.primary} · backup ${c.backup} · backup recovered ${c.recovered} · combined gaps ${c.settled - c.combined}`;

export function dailyReportText(date: string, day: HealthReportDay, closed = false): string {
	const lines = [`*${date} UTC — GBFS health${closed ? ' · status at day end' : ''}*`, day.coverage ? coverageText(day.coverage) : 'Combined minute coverage unknown'];
	for (const [id, c] of Object.entries(day.conditions).sort(([a], [b]) => a.localeCompare(b))) {
		lines.push(`${c.active ? ':warning: *Active*' : ':white_check_mark: *Resolved*'} · ${id} · ${c.starts} episode(s) · ${stamp(c.changedAt)}\n${c.text}`);
	}
	if (Object.keys(day.conditions).length === 0) lines.push('No alert conditions observed by this reporter today.');
	lines.push('<https://ctbk.dev/health/feed|Feed coverage> · <https://ctbk.dev/health|Pipeline health>');
	return lines.join('\n');
}

function weekStart(date: string): string {
	const d = new Date(`${date}T00:00:00Z`);
	d.setUTCDate(d.getUTCDate() - (d.getUTCDay() + 6) % 7);
	return DATE(d.toISOString());
}

export function monthlyReportText(report: HealthReportState, firing: Record<string, FiringEntry>, now: string, closed = false): string {
	const active = Object.keys(firing).sort();
	const lines = [`*GBFS health · ${report.month}*`, closed ? 'Month complete · daily conditions show status at day end' : active.length ? `*Active:* ${active.join(', ')}` : ':white_check_mark: No active alert conditions', `Updated ${stamp(now)} · UTC weeks below`];
	const weeks = new Map<string, { days: number; settled: number; combined: number; recovered: number; episodes: number }>();
	for (const [date, day] of Object.entries(report.days).sort(([a], [b]) => a.localeCompare(b))) {
		const w = weekStart(date), v = weeks.get(w) ?? { days: 0, settled: 0, combined: 0, recovered: 0, episodes: 0 };
		if (day.coverage) { v.days++; v.settled += day.coverage.settled; v.combined += day.coverage.combined; v.recovered += day.coverage.recovered; }
		v.episodes += Object.values(day.conditions).reduce((n, c) => n + c.starts, 0);
		weeks.set(w, v);
	}
	for (const [week, v] of weeks) lines.push(`• Week ${week}: ${v.combined}/${v.settled} minute slots across ${v.days} observed day(s); ${v.recovered} recovered by backup; ${v.episodes} alert episode(s)`);
	lines.push('Minute slots measure observations, not every upstream generation. Daily replies are edited in place; only critical incidents broadcast.', '<https://ctbk.dev/health|Pipeline health> · <https://ctbk.dev/health/feed|Coverage details>');
	return lines.join('\n');
}

export function reportDue(report: HealthReportState | undefined, now: string): boolean {
	return !report?.threadTs || report.month !== now.slice(0, 7) || DATE(report.updatedAt ?? '') !== DATE(now)
		|| Date.parse(now) - Date.parse(report.updatedAt ?? '') >= 15 * 60_000;
}

/** Checkpoint every successful write before trying the next one: a failed daily
 * reply must not cause a second monthly OP on retry. No thread-history scans,
 * deletions, or edits of human replies. */
export async function updateHealthReport(
	slack: HealthSlack,
	previous: HealthReportState | undefined,
	firing: Record<string, FiringEntry>,
	transitions: Transition[],
	snapshot: Pick<HealthSnapshot, 'generatedAt' | 'feed'>,
	now: string,
	checkpoint: (report: HealthReportState) => Promise<void>,
): Promise<HealthReportState> {
	const month = now.slice(0, 7), date = DATE(now);
	const imageUrl = `https://ctbk-gbfs-api.hccs-ctbk.workers.dev/api/health/card.png?v=${snapshot.generatedAt}`;
	if (previous?.threadTs && previous.month !== month) {
		const closed = structuredClone(previous);
		for (const coverage of snapshot.feed.coverageHistory ?? []) {
			if (coverage.date.startsWith(closed.month)) (closed.days[coverage.date] ??= freshDay()).coverage = coverage;
		}
		await slack.editReport(closed.threadTs!, monthlyReportText(closed, {}, now, true), imageUrl);
		for (const [dayDate, row] of Object.entries(closed.days)) {
			if (!row.messageTs) continue;
			const text = dailyReportText(dayDate, row, true);
			if (text !== row.body) await slack.edit(row.messageTs, text);
		}
	}
	const report: HealthReportState = previous?.month === month ? structuredClone(previous) : { month, days: {} };
	for (const coverage of snapshot.feed.coverageHistory ?? []) {
		if (!coverage.date.startsWith(month)) continue;
		(report.days[coverage.date] ??= freshDay()).coverage = coverage;
	}
	const day = report.days[date] ??= freshDay();
	if (snapshot.feed.coverage?.complete && snapshot.feed.coverage.today.date === date) day.coverage = snapshot.feed.coverage.today;
	for (const [id, c] of Object.entries(day.conditions)) c.active = firing[id] !== undefined;
	for (const t of transitions) {
		const old = day.conditions[t.rule.id];
		if (old?.changedAt === now && old.active === (t.kind === 'firing')) continue;
		day.conditions[t.rule.id] = {
			starts: (old?.starts ?? 0) + +(t.kind === 'firing'), active: t.kind === 'firing', changedAt: now,
			text: t.firingText ?? old?.text ?? t.priorEntry!.firingText,
		};
	}
	for (const [id, entry] of Object.entries(firing)) {
		day.conditions[id] ??= { starts: 0, active: true, changedAt: entry.firingSince, text: entry.firingText };
	}
	const body = monthlyReportText(report, firing, now);
	if (!report.threadTs) {
		report.threadTs = (await slack.postReport(body, imageUrl)).id;
		await checkpoint(report);
	} else await slack.editReport(report.threadTs, body, imageUrl);
	for (const [dayDate, row] of Object.entries(report.days).sort(([a], [b]) => a.localeCompare(b))) {
		if (dayDate !== date && !row.messageTs) continue;
		const text = dailyReportText(dayDate, row, dayDate !== date);
		if (!row.messageTs) {
			row.messageTs = (await slack.post(text, report.threadTs)).id;
			row.body = text;
			await checkpoint(report);
		} else if (text !== row.body) {
			await slack.edit(row.messageTs, text);
			row.body = text;
		}
	}
	report.updatedAt = now;
	return report;
}
