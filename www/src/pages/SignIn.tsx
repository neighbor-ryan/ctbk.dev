import { SignInPanel } from '@open-athena/auth/react'
import { useQuery } from '@tanstack/react-query'
import { Link, useSearchParams } from 'react-router-dom'
import css from './MovieStudio.module.css'

export default function SignIn() {
  const [params] = useSearchParams()
  const next = params.get('next') ?? '/movie'
  const safeNext = next.startsWith('/') && !next.startsWith('//') && !/[\\\x00-\x1f]/.test(next) ? next : '/movie'
  const providers = useQuery({ queryKey: ['signin-providers'], queryFn: async () => {
    const response = await fetch('/api/auth/providers', { cache: 'no-store' })
    if (!response.ok) throw new Error('Hosted movie sign-in is being set up. Localhost exports are available now.')
    return response.json() as Promise<{ google: boolean; email: boolean }>
  }, retry: false })
  return <main className={css.page}>
    <div style={{ maxWidth: 420, margin: '10vh auto' }}>
      {providers.data && <SignInPanel title="Save your Citi Bike movie" hint="Sign in to render and download an MP4. Any email is welcome." googleUrl={providers.data.google ? '/auth/google?next=' + encodeURIComponent(safeNext) : undefined} withNext={false} emailAuth={providers.data.email} onSignedIn={() => window.location.assign(safeNext)} />}
      {providers.isPending && <p>Loading sign-in…</p>}
      {providers.error && <p role="alert">{providers.error.message}</p>}
      {providers.data && !providers.data.google && !providers.data.email && <p>Hosted movie sign-in is being set up.</p>}
      <p><Link to={safeNext}>Back to movie preview</Link></p>
    </div>
  </main>
}
