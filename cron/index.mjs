const apiUrl = process.env.API_URL || process.env.RAILWAY_SERVICE_API_URL || '';
const automationSecret = process.env.LLMWIKI_AUTOMATION_SECRET || '';

if (!apiUrl) throw new Error('API_URL is required');
if (!automationSecret) throw new Error('LLMWIKI_AUTOMATION_SECRET is required');

const normalizedApiUrl = apiUrl.startsWith('http') ? apiUrl : `https://${apiUrl}`;
const baseUrl = normalizedApiUrl.replace(/\/$/, '');

async function invoke(path) {
  const res = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: {
      'x-llmwiki-automation-secret': automationSecret,
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${path} failed: ${res.status} ${text}`);
  }

  return await res.json();
}

const compile = await invoke('/internal/compile-due');
const streamline = await invoke('/internal/streamline-due');

console.log(JSON.stringify({ compile, streamline }, null, 2));
