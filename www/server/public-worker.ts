/// <reference path="./preview-env.d.ts" />
/// <reference types="@cloudflare/workers-types" />
import { servePublicPage } from './page-previews'

export default {
  fetch(request: Request, env: PreviewBindings, ctx: { waitUntil(promise: Promise<unknown>): void }): Promise<Response> {
    return servePublicPage(request, env, ctx)
  },
}
