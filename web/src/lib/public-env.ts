declare global {
  interface Window {
    __PUBLIC_ENV__?: Record<string, string | undefined>
  }
}

export function getPublicEnv(name: string): string | undefined {
  if (typeof window !== 'undefined') {
    return window.__PUBLIC_ENV__?.[name] ?? process.env[name]
  }

  return process.env[name]
}

export function getRequiredPublicEnv(name: string): string {
  const value = getPublicEnv(name)

  if (!value) {
    throw new Error(`Missing ${name} environment variable`)
  }

  return value
}
