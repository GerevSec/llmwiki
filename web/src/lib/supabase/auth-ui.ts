import { getPublicEnv } from "@/lib/public-env"

type AuthSettingsResponse = {
  external?: {
    google?: boolean
  }
}

type SupabaseLikeError = {
  message?: string
  code?: string | number
  error_code?: string
}

export async function getAuthSettings() {
  const supabaseUrl = getPublicEnv("NEXT_PUBLIC_SUPABASE_URL")
  const supabasePublishableKey = getPublicEnv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

  if (!supabaseUrl || !supabasePublishableKey) {
    return null
  }

  const response = await fetch(`${supabaseUrl}/auth/v1/settings`, {
    headers: {
      apikey: supabasePublishableKey,
      Authorization: `Bearer ${supabasePublishableKey}`,
    },
  })

  if (!response.ok) {
    throw new Error("Unable to load authentication settings")
  }

  return (await response.json()) as AuthSettingsResponse
}

export function getAuthErrorMessage(error: unknown) {
  const authError = error as SupabaseLikeError | null | undefined

  if (authError?.error_code === "over_email_send_rate_limit") {
    return "Email signups are temporarily rate-limited. Please wait a few minutes and try again."
  }

  if (authError?.message) {
    return authError.message
  }

  if (error instanceof Error && error.message) {
    return error.message
  }

  return "Something went wrong. Please try again."
}
