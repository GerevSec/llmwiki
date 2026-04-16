'use client'

import * as React from 'react'

/**
 * Quote bracketed labels that start with `/` so mermaid doesn't try to parse them
 * as a parallelogram shape (`[/text/]`). `ARCH[/wiki/foo.md]` breaks the parser
 * and was the root cause of the home-page nav rendering as raw text.
 */
export function preprocessMermaidChart(chart: string): string {
  return chart.replace(
    /(\b\w+)\[(\/[^\]"\n]+?)(?<!\/)\]/g,
    (_match, id: string, body: string) => `${id}["${body}"]`,
  )
}

export function MermaidBlock({ chart }: { chart: string }) {
  const containerRef = React.useRef<HTMLDivElement>(null)
  const idRef = React.useRef(`mermaid-${Math.random().toString(36).slice(2, 9)}`)
  const [isDark, setIsDark] = React.useState(false)
  const [failed, setFailed] = React.useState(false)

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

    setFailed(false)
    const sanitized = preprocessMermaidChart(chart)

    import('mermaid').then(({ default: mermaid }) => {
      mermaid.initialize({
        startOnLoad: false,
        theme: 'base',
        fontFamily: getComputedStyle(document.body).fontFamily,
        flowchart: {
          htmlLabels: false,
          // Render at natural size; the outer container (.mermaid-block) uses
          // overflow-x-auto so wide diagrams scroll horizontally and stay legible
          // instead of being scaled down into an unreadable blob.
          useMaxWidth: false,
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
        .render(idRef.current, sanitized)
        .then(({ svg }) => {
          if (!cancelled && containerRef.current) {
            containerRef.current.innerHTML = svg
          }
        })
        .catch(() => {
          if (!cancelled) setFailed(true)
        })
    })
    return () => {
      cancelled = true
    }
  }, [chart, isDark])

  if (failed) {
    return (
      <div className="mermaid-block mermaid-block--failed">
        <p className="mermaid-block__error">Diagram failed to render — showing source:</p>
        <pre className="mermaid-block__source">{chart}</pre>
      </div>
    )
  }

  return (
    <div
      ref={containerRef}
      className="mermaid-block"
    />
  )
}
