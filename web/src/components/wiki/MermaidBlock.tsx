'use client'

import * as React from 'react'

export function MermaidBlock({ chart }: { chart: string }) {
  const containerRef = React.useRef<HTMLDivElement>(null)
  const idRef = React.useRef(`mermaid-${Math.random().toString(36).slice(2, 9)}`)
  const [isDark, setIsDark] = React.useState(false)

  React.useEffect(() => {
    if (typeof document === 'undefined') return

    const root = document.documentElement
    const syncTheme = () => setIsDark(root.classList.contains('dark'))
    syncTheme()

    const observer = new MutationObserver(syncTheme)
    observer.observe(root, { attributes: true, attributeFilter: ['class'] })

    return () => observer.disconnect()
  }, [])

  React.useEffect(() => {
    let cancelled = false

    if (typeof document === 'undefined') return

    const css = getComputedStyle(document.documentElement)
    const asHsl = (token: string) => `hsl(${css.getPropertyValue(token).trim()})`

    import('mermaid').then(({ default: mermaid }) => {
      mermaid.initialize({
        startOnLoad: false,
        theme: 'base',
        fontFamily: getComputedStyle(document.body).fontFamily,
        flowchart: {
          htmlLabels: false,
          useMaxWidth: true,
          nodeSpacing: 36,
          rankSpacing: 56,
          padding: 20,
        },
        themeVariables: {
          background: asHsl('--background'),
          primaryColor: isDark ? asHsl('--secondary') : asHsl('--card'),
          primaryTextColor: asHsl('--foreground'),
          primaryBorderColor: asHsl('--border'),
          secondaryColor: isDark ? asHsl('--muted') : asHsl('--accent'),
          secondaryTextColor: asHsl('--foreground'),
          secondaryBorderColor: asHsl('--border'),
          tertiaryColor: asHsl('--muted'),
          tertiaryTextColor: asHsl('--foreground'),
          tertiaryBorderColor: asHsl('--border'),
          mainBkg: isDark ? asHsl('--secondary') : asHsl('--card'),
          nodeBorder: asHsl('--border'),
          clusterBkg: isDark ? asHsl('--muted') : asHsl('--accent'),
          clusterBorder: asHsl('--border'),
          defaultLinkColor: asHsl('--muted-foreground'),
          lineColor: asHsl('--muted-foreground'),
          edgeLabelBackground: asHsl('--background'),
          textColor: asHsl('--foreground'),
        },
      })
      mermaid
        .render(idRef.current, chart)
        .then(({ svg }) => {
          if (!cancelled && containerRef.current) {
            containerRef.current.innerHTML = svg
          }
        })
        .catch(() => {
          if (!cancelled && containerRef.current) {
            containerRef.current.textContent = chart
          }
        })
    })
    return () => {
      cancelled = true
    }
  }, [chart, isDark])

  return (
    <div
      ref={containerRef}
      className="mermaid-block"
    />
  )
}
