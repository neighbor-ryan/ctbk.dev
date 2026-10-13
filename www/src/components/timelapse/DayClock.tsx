/** PATH-inspired 24-hour dial. `ms` is the simulation's local-as-UTC clock;
 *  direct SVG rotation keeps interactive playback and exported frames exact. */
export function DayClock({ ms }: { ms: number }) {
  const hour = ((ms % 86_400_000) + 86_400_000) % 86_400_000 / 3_600_000
  const time = new Date(ms).toISOString().slice(11, 16)
  return (
    <svg viewBox="0 0 88 88" width={88} height={88} role="img" aria-label={`24-hour clock, ${time}`} data-testid="ride-clock">
      <circle cx={44} cy={44} r={43} fill="rgba(255,255,255,0.04)" stroke="#555" />
      {Array.from({ length: 24 }, (_, h) => (
        <line key={h} x1={44} y1={h % 6 === 0 ? 7 : 5} x2={44} y2={2}
          transform={`rotate(${h * 15} 44 44)`} stroke={h % 6 === 0 ? '#aaa' : '#666'} strokeWidth={h % 6 === 0 ? 1.4 : 1} />
      ))}
      {[['00', 44, 18], ['06', 70, 44], ['12', 44, 70], ['18', 18, 44]].map(([label, x, y]) => (
        <text key={label} x={x} y={y} textAnchor="middle" dominantBaseline="central" fill="#aaa" fontSize={9}>{label}</text>
      ))}
      <g transform={`rotate(${hour * 15} 44 44)`} data-testid="ride-clock-hand">
        <line x1={44} y1={44} x2={44} y2={11} stroke="#fff" strokeWidth={1.5} strokeLinecap="round" />
        <circle cx={44} cy={11} r={3} fill="#fff" />
      </g>
      <circle cx={44} cy={44} r={3} fill="#fff" />
    </svg>
  )
}
