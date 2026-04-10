import { createBrowserClient } from "@supabase/ssr";
import { getRequiredPublicEnv } from "@/lib/public-env";

export function createClient() {
  const supabaseUrl = getRequiredPublicEnv("NEXT_PUBLIC_SUPABASE_URL");
  const supabaseAnonKey = getRequiredPublicEnv("NEXT_PUBLIC_SUPABASE_ANON_KEY");

  return createBrowserClient(supabaseUrl, supabaseAnonKey);
}
