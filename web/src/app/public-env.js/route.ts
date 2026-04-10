import { NextResponse } from "next/server"

export const dynamic = "force-dynamic"

export async function GET() {
  const body = `window.__PUBLIC_ENV__ = ${JSON.stringify({
    NEXT_PUBLIC_SUPABASE_URL: process.env.NEXT_PUBLIC_SUPABASE_URL ?? "",
    NEXT_PUBLIC_SUPABASE_ANON_KEY: process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "",
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL ?? "",
    NEXT_PUBLIC_MCP_URL: process.env.NEXT_PUBLIC_MCP_URL ?? "",
  })};`

  return new NextResponse(body, {
    headers: {
      "Content-Type": "application/javascript; charset=utf-8",
      "Cache-Control": "no-store",
    },
  })
}
