import { simulationClock } from '../../query/movieTimeline'

/** `ms` is the simulation's local-as-UTC clock, never movie elapsed time;
 *  direct SVG rotation keeps interactive playback and exported frames exact. */
export function DayClock({ ms }: { ms: number }) {
  const { hourDegrees, minuteDegrees, dialLabel, period } = simulationClock(ms)
  return (
    <svg viewBox="0 0 88 88" width={88} height={88} role="img" aria-label={`12-hour clock, ${dialLabel}`} data-testid="ride-clock">
      <circle cx={44} cy={44} r={43} fill="rgba(255,255,255,0.04)" stroke="#555" />
      {Array.from({ length: 60 }, (_, m) => (
        <line key={m} x1={44} y1={m % 5 === 0 ? 7 : 4} x2={44} y2={2}
          transform={`rotate(${m * 6} 44 44)`} stroke={m % 5 === 0 ? '#aaa' : '#555'} strokeWidth={m % 5 === 0 ? 1.4 : 1} />
      ))}
      {[['12', 44, 18], ['3', 70, 44], ['6', 44, 70], ['9', 18, 44]].map(([label, x, y]) => (
        <text key={label} x={x} y={y} textAnchor="middle" dominantBaseline="central" fill="#aaa" fontSize={9}>{label}</text>
      ))}
      <text x={44} y={59} textAnchor="middle" fill="#aaa" fontSize={9}>{period}</text>
      <g transform={`rotate(${hourDegrees} 44 44)`} data-testid="ride-clock-hand">
        <line x1={44} y1={44} x2={44} y2={25} stroke="#fff" strokeWidth={3} strokeLinecap="round" />
      </g>
      <g transform={`rotate(${minuteDegrees} 44 44)`} data-testid="ride-clock-minute-hand">
        <line x1={44} y1={44} x2={44} y2={9} stroke="#fff" strokeWidth={1.5} strokeLinecap="round" />
      </g>
      <circle cx={44} cy={44} r={3} fill="#fff" />
    </svg>
  )
}
