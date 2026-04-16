const BULLET_RE = /^[-*]\s+/

export function splitMarkdownIntoGuidelines(text: string): string[] {
  const lines = text.split('\n')
  const chunks: string[] = []
  let buffer: string[] = []

  const flush = () => {
    if (buffer.length === 0) return
    const [first, ...rest] = buffer
    const cleaned = [first.replace(BULLET_RE, ''), ...rest].join('\n').trimEnd()
    const trimmedFront = cleaned.replace(/^\s*\n/, '')
    if (trimmedFront.trim()) chunks.push(trimmedFront)
    buffer = []
  }

  for (const line of lines) {
    if (BULLET_RE.test(line)) {
      flush()
      buffer.push(line)
    } else if (buffer.length > 0 || line.trim()) {
      buffer.push(line)
    }
  }
  flush()

  return chunks
}
