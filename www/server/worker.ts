import { anyEmailPolicy, authRoutes, createGate, emailCodeAuth } from '@open-athena/auth'
import { d1AuditQuery, d1AuditSink, d1GrantStore, d1PendingAuthStore } from '@open-athena/auth/d1'
import { oidcCallback, oidcStart } from '@open-athena/auth/oidc'
import { resendEmail } from '@open-athena/auth/resend'
import { batchRenderer } from './batch-renderer'
import { movieJobs } from './movie-jobs'
import { movieStore, type MovieDatabase } from './movie-store'

type Env = {
  ASSETS: { fetch(request: Request): Promise<Response> }
  DB?: Parameters<typeof d1GrantStore>[0] & MovieDatabase
  SESSION_SECRET?: string
  ADMIN_EMAILS?: string
  GOOGLE_CLIENT_ID?: string
  GOOGLE_CLIENT_SECRET?: string
  RESEND_API_KEY?: string
  MAIL_FROM?: string
  MOVIE_GATEWAY_URL?: string
  MOVIE_GATEWAY_SECRET?: string
}
const unavailable = () => Response.json({ error: 'Hosted movie exports are being set up.' }, { status: 503, headers: { 'cache-control': 'no-store' } })

/** Static maps stay public; only verified users can enqueue/access private movies. */
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    const next = url.searchParams.get('next')
    if (next && (!next.startsWith('/') || next.startsWith('//') || /[\\\x00-\x1f]/.test(next))) {
      url.searchParams.set('next', '/movie')
      request = new Request(url, request)
    }
    const isAuth = url.pathname.startsWith('/api/auth/') || url.pathname.startsWith('/auth/')
    const isMovie = url.pathname.startsWith('/__movies/')
    if (!isAuth && !isMovie) return env.ASSETS.fetch(request)
    if (!env.DB || !env.SESSION_SECRET) return unavailable()
    const gate = createGate({
      store: d1GrantStore(env.DB), secret: env.SESSION_SECRET, cookieName: 'ctbk_session',
      adminEmails: (env.ADMIN_EMAILS ?? '').split(/[\s,]+/).filter(Boolean), policy: anyEmailPolicy(['movie:export']),
      audit: d1AuditSink(env.DB),
    })
    if (isAuth) {
      if (!['GET', 'HEAD'].includes(request.method) && request.headers.get('origin') !== url.origin) return new Response('Use a same-origin request.', { status: 403 })
      if (url.pathname === '/api/auth/providers') return Response.json({ google: !!(env.GOOGLE_CLIENT_ID && env.GOOGLE_CLIENT_SECRET), email: !!(env.RESEND_API_KEY && env.MAIL_FROM) }, { headers: { 'cache-control': 'no-store' } })
      if (url.pathname === '/auth/google' || url.pathname === '/auth/google/callback') {
        if (!env.GOOGLE_CLIENT_ID || !env.GOOGLE_CLIENT_SECRET) return unavailable()
        const options = { gate, clientId: env.GOOGLE_CLIENT_ID, clientSecret: env.GOOGLE_CLIENT_SECRET, redirectUri: url.origin + '/auth/google/callback', seedProfile: false }
        return (url.pathname.endsWith('/callback') ? oidcCallback(options) : oidcStart(options))({ request })
      }
      if (url.pathname.startsWith('/api/auth/email/')) {
        if (!env.RESEND_API_KEY || !env.MAIL_FROM) return unavailable()
        const email = emailCodeAuth({ gate, store: d1PendingAuthStore(env.DB), send: resendEmail({ apiKey: env.RESEND_API_KEY }), from: env.MAIL_FROM, appName: 'ctbk.dev', linkFor: (token) => url.origin + '/api/auth/email/link?token=' + encodeURIComponent(token), defaultNext: '/movie' })
        if (url.pathname === '/api/auth/email/start' && request.method === 'POST') return email.start({ request })
        if (url.pathname === '/api/auth/email/code' && request.method === 'POST') return email.verifyCode({ request })
        if (url.pathname === '/api/auth/email/link' && request.method === 'GET') return email.verifyLink({ request })
        return new Response('Not found.', { status: 404 })
      }
      return await authRoutes(gate, { audit: d1AuditQuery(env.DB) })(request) ?? new Response('Not found.', { status: 404 })
    }
    if (!env.MOVIE_GATEWAY_URL || !env.MOVIE_GATEWAY_SECRET) return unavailable()
    const route = movieJobs({
      identify: async (req) => {
        const auth = await gate.authenticate(req)
        if (!auth || auth.kind !== 'sso') return null
        return { email: auth.email, admin: auth.admin }
      },
      store: movieStore(env.DB), renderer: batchRenderer(env.MOVIE_GATEWAY_URL, env.MOVIE_GATEWAY_SECRET), signInUrl: '/signin',
      audit: async (event) => {
        await d1AuditSink(env.DB!).log({ event: 'view', sessionSub: 'e:' + event.email.toLowerCase(), path: '/__movies/jobs/' + event.job + '/' + event.kind.slice('movie.'.length), reason: event.kind, status: 200, ts: Math.floor(Date.now() / 1000) })
      },
    })
    return await route(request) ?? new Response('Not found.', { status: 404 })
  },
}
