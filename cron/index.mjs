const apiUrl = process.env.API_URL || process.env.RAILWAY_SERVICE_API_URL || '';
const automationSecret = process.env.LLMWIKI_AUTOMATION_SECRET || '';

if (!apiUrl) throw new Error('API_URL is required');
if (!automationSecret) throw new Error('LLMWIKI_AUTOMATION_SECRET is required');

const normalizedApiUrl = apiUrl.startsWith('http') ? apiUrl : `https://${apiUrl}`;
const res = await fetch(`${normalizedApiUrl.replace(/\/$/, '')}/internal/compile-due`, {
  method: 'POST',
  headers: {
    'x-llmwiki-automation-secret': automationSecret,
  },
});

if (!res.ok) {
  const text = await res.text();
  throw new Error(`compile-due failed: ${res.status} ${text}`);
}

console.log(JSON.stringify(await res.json(), null, 2));
