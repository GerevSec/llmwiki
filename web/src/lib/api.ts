import { getPublicEnv } from '@/lib/public-env'

function getApiUrl() {
  return getPublicEnv('NEXT_PUBLIC_API_URL') || 'http://localhost:8000'
}

export async function apiFetch<T>(
  path: string,
  token: string,
  options?: RequestInit,
): Promise<T> {
  let res: Response
  try {
    res = await fetch(`${getApiUrl()}${path}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
        ...options?.headers,
      },
    })
  } catch (error) {
    throw new Error('Could not reach the API. If you are using a custom app domain, check the API origin/CORS configuration.')
  }
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `API error: ${res.status}`)
  }
  if (res.status === 204) return undefined as T
  return res.json()
}
