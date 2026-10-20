import { describe, expect, it } from 'vitest';
import { HealthSlackClient, dailyReportText, monthlyReportText, reportBlocks, updateHealthReport } from './healthReport';
import type { HealthReportState } from './healthReport';
import type { FeedHealth } from './health';

describe('health report content', () => {
	it('finalizes a previous month from settled history and never assigns another day’s coverage to a retry', async () => {
		const partial = { date: '2026-05-31', settled: 1438, primary: 1430, backup: 1438, combined: 1438, recovered: 8, hours: [] };
		const complete = { ...partial, settled: 1440, backup: 1440, combined: 1440, recovered: 10 };
		const previous: HealthReportState = { month: '2026-05', threadTs: 'old-month', days: {
			'2026-05-31': { coverage: partial, conditions: {}, messageTs: 'old-day' },
		} };
		const edits: Array<[string, string]> = [];
		const slack = {
			editReport: async (id: string, text: string) => { edits.push([id, text]); },
			edit: async (id: string, text: string) => { edits.push([id, text]); },
			postReport: async () => ({ id: 'new-month' }), post: async () => ({ id: 'new-day' }),
			postUrgent: async () => ({ id: 'urgent' }),
		};
		const snapshot = { generatedAt: Date.parse('2026-06-01T00:01:00Z') / 1000, feed: {
			latestPoll: null, drift: null, todayCount: 0, todayExpected: 0, last7Days: [],
			coverageHistory: [complete], coverage: { complete: true, today: complete, missing: [], unexplained: [], recent: { primary: [], backup: [], combined: [] } },
		} satisfies FeedHealth };
		const report = await updateHealthReport(slack, previous, {}, [], snapshot, '2026-06-01T00:01:00Z', async () => {});
		expect(edits).toEqual([
			['old-month', monthlyReportText({ ...previous, days: { '2026-05-31': { ...previous.days['2026-05-31'], coverage: complete } } }, {}, '2026-06-01T00:01:00Z', true)],
			['old-day', dailyReportText('2026-05-31', { ...previous.days['2026-05-31'], coverage: complete }, true)],
		]);
		expect(report.days['2026-06-01'].coverage).toBe(null);
		expect(previous.days['2026-05-31'].coverage).toEqual(partial);
	});

	it('shows recovery, episode count, UTC week rollups and measured coverage literally', () => {
		const day = {
			coverage: { date: '2026-10-20', settled: 10, primary: 7, backup: 10, combined: 10, recovered: 3, hours: [] },
			conditions: { 'feed-redundancy': { starts: 4, active: false, changedAt: '2026-10-20T08:00:00.000Z', text: 'Primary missing 3/10; backup covers all three.' } },
		};
		expect(dailyReportText('2026-10-20', day).split('\n')).toEqual([
			'*2026-10-20 UTC — GBFS health*',
			'10/10 minute slots observed · primary 7 · backup 10 · backup recovered 3 · combined gaps 0',
			':white_check_mark: *Resolved* · feed-redundancy · 4 episode(s) · 2026-10-20 08:00 UTC',
			'Primary missing 3/10; backup covers all three.',
			'<https://ctbk.dev/health/feed|Feed coverage> · <https://ctbk.dev/health|Pipeline health>',
		]);
		const report: HealthReportState = { month: '2026-10', days: { '2026-10-20': day } };
		expect(monthlyReportText(report, {}, '2026-10-20T08:01:00.000Z').split('\n')).toEqual([
			'*GBFS health · 2026-10*', ':white_check_mark: No active alert conditions',
			'Updated 2026-10-20 08:01 UTC · UTC weeks below',
			'• Week 2026-10-19: 10/10 minute slots across 1 observed day(s); 3 recovered by backup; 4 alert episode(s)',
			'Minute slots measure observations, not every upstream generation. Daily replies are edited in place; only critical incidents broadcast.',
			'<https://ctbk.dev/health|Pipeline health> · <https://ctbk.dev/health/feed|Coverage details>',
		]);
	});
});

describe('Slack health image transport', () => {
	it('edits replace image blocks with a new URL, retain accessible text, and broadcast only critical replies', async () => {
		const calls: Array<{ method: string; payload: unknown }> = [];
		const slack = new HealthSlackClient({ token: 'test', channel: 'channel', username: 'ctbk-bot', iconEmoji: ':bike:', fetch: async (input, init) => {
			calls.push({ method: String(input).split('/').at(-1)!, payload: JSON.parse(init!.body as string) });
			return new Response(JSON.stringify({ ok: true, ts: 'message' }));
		} });
		await slack.postReport('First', 'https://ctbk.dev/card.png?v=1');
		await slack.editReport('message', 'Updated', 'https://ctbk.dev/card.png?v=2');
		await slack.postUrgent('Combined loss', 'month');
		expect(calls).toEqual([
			{ method: 'chat.postMessage', payload: { channel: 'channel', text: 'First', blocks: reportBlocks('First', 'https://ctbk.dev/card.png?v=1'), username: 'ctbk-bot', icon_emoji: ':bike:', unfurl_links: false, unfurl_media: false } },
			{ method: 'chat.update', payload: { channel: 'channel', ts: 'message', text: 'Updated', blocks: reportBlocks('Updated', 'https://ctbk.dev/card.png?v=2'), unfurl_links: false, unfurl_media: false } },
			{ method: 'chat.postMessage', payload: { channel: 'channel', text: 'Combined loss', thread_ts: 'month', reply_broadcast: true, username: 'ctbk-bot', icon_emoji: ':bike:', unfurl_links: false, unfurl_media: false } },
		]);
		expect(reportBlocks('Updated', 'https://ctbk.dev/card.png?v=2')).toEqual([
			{ type: 'section', text: { type: 'mrkdwn', text: 'Updated' } },
			{ type: 'image', image_url: 'https://ctbk.dev/card.png?v=2', alt_text: 'Recent GBFS minute coverage: primary, backup, combined. Gaps and backup recovery are separate.' },
		]);
	});
});
