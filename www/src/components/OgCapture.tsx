import { useEffect } from 'react'
import { useIsFetching } from '@tanstack/react-query'
import { useLocation } from 'react-router-dom'

/** A screenshot uses the normal page and data hooks. Readiness excludes lazy
 * route placeholders and in-flight queries; no substitute chart is rendered. */
export function OgCapture() {
  const location = useLocation()
  const fetching = useIsFetching()
  const enabled = new URLSearchParams(location.search).get('__og') === '1'
  useEffect(() => {
    if (!enabled) return
    document.documentElement.dataset.ogCapture = 'true'
    delete document.documentElement.dataset.ogReady
    const timer = window.setInterval(() => {
      const rendered = document.querySelector('[data-og-page-ready="true"]')
        ?? document.querySelector('main:not(:empty), h1')
      if (!fetching && rendered && !document.querySelector('[data-og-page-ready="false"]')) document.documentElement.dataset.ogReady = 'true'
    }, 250)
    return () => {
      clearInterval(timer)
      delete document.documentElement.dataset.ogCapture
      delete document.documentElement.dataset.ogReady
    }
  }, [enabled, fetching, location.pathname, location.search])
  return null
}
