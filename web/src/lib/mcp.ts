import { getPublicEnv } from '@/lib/public-env'

const DEFAULT_MCP_URL = 'http://localhost:8080/mcp'

function getApiUrl() {
  return getPublicEnv('NEXT_PUBLIC_API_URL') || 'http://localhost:8000'
}

function getMcpUrl() {
  return (
    getPublicEnv('NEXT_PUBLIC_MCP_URL') ||
    (getPublicEnv('NEXT_PUBLIC_API_URL') ? `${getApiUrl()}/mcp` : DEFAULT_MCP_URL)
  )
}

export const MCP_URL = getMcpUrl()

export function buildOAuthMcpConfig(): string {
  return JSON.stringify(
    {
      mcpServers: {
        llmwiki: {
          url: getMcpUrl(),
        },
      },
    },
    null,
    2,
  )
}

