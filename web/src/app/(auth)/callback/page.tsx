"use client";

import { createClient } from "@/lib/supabase/client";
import { useRouter } from "next/navigation";
import { useEffect } from "react";

export default function CallbackPage() {
  const router = useRouter();

  useEffect(() => {
    const supabase = createClient();
    const { data: { subscription } } = supabase.auth.onAuthStateChange((event) => {
      if (event === "SIGNED_IN") {
        router.push("/kb");
        router.refresh();
      }
    });

    return () => subscription.unsubscribe();
  }, [router]);

  return (
    <div className="flex min-h-screen items-center justify-center">
      <p className="text-muted-foreground">Completing sign in...</p>
    </div>
  );
}
