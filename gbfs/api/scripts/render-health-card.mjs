#!/usr/bin/env node
/** Render a saved /api/health snapshot with the same SVG used by the page and Worker. */
import { readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { initWasm, Resvg } from '@resvg/resvg-wasm';
import { healthCardSvg } from '../src/healthCard.ts';

const [input, output] = process.argv.slice(2);
if (!input || !output) throw new Error('Usage: node scripts/render-health-card.mjs HEALTH.json OUTPUT.png');
const snapshot = JSON.parse(await readFile(input, 'utf8'));
const svg = healthCardSvg(snapshot);
await initWasm(await readFile(fileURLToPath(new URL('../src/assets/resvg.wasm', import.meta.url))));
const font = await readFile(fileURLToPath(new URL('../src/assets/Inter-SemiBold.ttf', import.meta.url)));
const png = new Resvg(svg, { font: { fontBuffers: [font], defaultFontFamily: 'Inter' } }).render().asPng();
await writeFile(output, png);
console.error(`Wrote ${output} (${png.byteLength} bytes)`);
