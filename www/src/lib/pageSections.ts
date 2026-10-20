export interface PageSection { id: string; label: string }
const sections = (items: Array<[string, string]>): PageSection[] => items.map(([id, label]) => ({ id, label }))

const HOME = sections([['plot', 'Rides'], ['map', 'Stations and destinations'], ['states', 'Station states'], ['pipeline', 'Data pipeline']])
const PIPELINE = sections([
  ['overview', 'Data pipeline'], ['source-data', 'Source data'], ['normalization', 'Normalization'],
  ['consolidation', 'Consolidation'], ['legacy-data', 'Legacy data preservation'], ['aggregation', 'Aggregation'],
  ['station-metadata', 'Station metadata'], ['station-meta-histograms', 'Station meta histograms'],
  ['station-modes', 'Station modes'], ['station-pair-jsons', 'Station pair JSONs'],
  ['per-station-pages', 'Per-station pages'], ['serving', 'Serving'],
])
const HEALTH = sections([
  ['overview', 'Pipeline health'], ['tripdata', 'Tripdata'], ['feed', 'Feed coverage'],
  ['compactions', 'Compactions'], ['builds', 'Pyramid builds'], ['pyramids', 'Pyramid coverage'],
  ['cascade', 'Legacy cascade'], ['browse', 'Browse'],
])
const FEED = sections([['overview', 'Feed metrics'], ['summary', 'Coverage summary'], ['intervals', 'Update intervals'], ['days', 'Daily coverage']])
const STATION = sections([
  ['overview', 'Station overview'], ['availability', 'Availability'], ['states', 'Station state'],
  ['by-day', 'By day and time'], ['by-hour', 'By hour of day'], ['map', 'Station map'],
  ['trips', 'Monthly trips'], ['rides', 'Rides'],
])

export function pageSections(path: string): PageSection[] {
  if (path === '/' || path === '/v2') return HOME
  if (path === '/pipeline') return PIPELINE
  if (path === '/health') return HEALTH
  if (path === '/health/feed') return FEED
  if (/^\/(?:s|stations)\/[^/]+\/?$/.test(path)) return STATION
  return []
}

export function pageSectionLabel(view: URL): string | undefined {
  return pageSections(view.pathname).find(section => section.id === view.searchParams.get('section'))?.label
}
