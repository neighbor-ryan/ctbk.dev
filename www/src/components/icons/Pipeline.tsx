export function PipelineIcon({ className, style }: { className?: string; style?: React.CSSProperties }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      className={className}
      style={style}
      aria-hidden="true"
    >
      <path d="M5 11 10 6m-5 7 5 5m4-12 5 5m-5 7 5-5" stroke="currentColor" strokeWidth="1.5" />
      <path d="m7.5 6.5 2.5-.5-.5 2.5M7.5 17.5l2.5.5-.5-2.5m7-7 2.5 2.5-3-.5m.5 4.5 2.5-2.5-3 .5" stroke="currentColor" strokeWidth="1.2" strokeLinejoin="round" />
      <circle cx="3" cy="12" r="2.5" fill="#16845b" />
      <circle cx="12" cy="4" r="2.5" fill="#1f77b4" />
      <circle cx="12" cy="20" r="2.5" fill="#f38020" />
      <circle cx="21" cy="12" r="2.5" fill="#a175df" />
    </svg>
  )
}
