import { Link } from 'react-router-dom'
import { GitHubIcon, HealthIcon, R2Icon, BlueskyIcon, PipelineIcon, HomeIcon } from './icons'
import css from './Footer.module.css'

interface FooterProps {
  showPipeline?: boolean
  showHome?: boolean
}

export function Footer({ showPipeline = true, showHome = false }: FooterProps) {
  return (
    <nav className={css.footer} aria-label="Project links">
      {showHome && (
        <Link to="/" className={css.item}>
          Home: <HomeIcon className={`${css.icon} ${css.home}`} />
        </Link>
      )}
      <a className={css.item} href="https://github.com/hudcostreets/ctbk.dev" target="_blank" rel="noopener noreferrer">
        Code: <GitHubIcon className={`${css.icon} ${css.code}`} />
      </a>
      <Link to="/files/" className={css.item}>
        Data: <R2Icon className={`${css.icon} ${css.data}`} />
      </Link>
      {showPipeline && (
        <Link to="/pipeline" className={css.item}>
          Pipeline: <PipelineIcon className={`${css.icon} ${css.pipeline}`} />
        </Link>
      )}
      <Link to="/health" className={css.item}>
        Health: <HealthIcon className={`${css.icon} ${css.health}`} />
      </Link>
      <a className={css.item} href="https://bsky.app/profile/runsascoded.com" target="_blank" rel="noopener noreferrer">
        Author: <BlueskyIcon className={`${css.icon} ${css.author}`} />
      </a>
    </nav>
  )
}
