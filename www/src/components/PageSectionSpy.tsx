import { useLocation, useNavigationType } from 'react-router-dom'
import { useSectionSpy } from 'use-prms/sections'
import { pageSections } from '../lib/pageSections'

export function PageSectionSpy() {
  const location = useLocation()
  const navType = useNavigationType()
  useSectionSpy({
    sections: pageSections(location.pathname).map(section => section.id),
    mode: { kind: 'param', key: 'section' },
    nav: { location, navType },
  })
  return null
}
