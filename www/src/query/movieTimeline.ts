const MINUTE = 60_000
const DAY = 1440 * MINUTE
const STEPS = [1, 5, 15, 30, 60, 120, 180, 360, 720, 1440, 2880, 10080]
const DAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

/** These timestamps encode simulated New York wall time, not UTC instants. */
export function simulationClock(ms: number) {
  const dayMs = ((ms % DAY) + DAY) % DAY
  const minutes = dayMs / MINUTE
  const label = new Date(ms).toISOString().slice(11, 16)
  const period = minutes < 720 ? 'AM' : 'PM'
  return { label, dialLabel: `${Math.floor(minutes / 60) % 12 || 12}:${label.slice(3)} ${period}`, hourDegrees: (minutes % 720) / 2, minuteDegrees: (minutes % 60) * 6, period }
}

/** Calendar-aligned ticks in simulation time; preserve both exact clip edges. */
export function movieTimeTicks(start: number, end: number, width: number) {
  if (!(Number.isFinite(start) && Number.isFinite(end) && end > start && Number.isFinite(width) && width > 0)) throw new Error('Movie timeline needs an ordered clip and positive width')
  const span = end - start
  const intervals = Math.max(2, Math.floor(width / 120))
  const step = (STEPS.find((n) => n * MINUTE >= span / intervals) ?? Math.ceil(span / intervals / DAY) * 1440) * MINUTE
  const times = [start]
  for (let ms = Math.ceil(start / step) * step; ms < end; ms += step) {
    if ((ms - times[times.length - 1]) / span * width >= 80 && (end - ms) / span * width >= 80) times.push(ms)
  }
  times.push(end)
  return times.map((ms, i) => {
    const date = new Date(ms)
    const midnight = ms % DAY === 0
    return {
      ms, position: (ms - start) / span, label: simulationClock(ms).label, midnight,
      date: i === 0 || i === times.length - 1 || midnight ? `${DAYS[date.getUTCDay()]} ${MONTHS[date.getUTCMonth()]} ${date.getUTCDate()}` : null,
    }
  })
}
