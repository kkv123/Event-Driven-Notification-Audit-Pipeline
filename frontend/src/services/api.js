/**
 * API service for Event-Driven Notification & Audit Pipeline
 * Supports configurable API base URL via environment variable (VITE_API_BASE_URL)
 * or runtime override stored in localStorage.
 */

const STORAGE_KEY = 'PIPELINE_API_BASE_URL';

export function getApiBaseUrl() {
  const custom = localStorage.getItem(STORAGE_KEY);
  if (custom !== null && custom !== undefined && custom.trim() !== '') {
    return custom.trim().replace(/\/+$/, '');
  }
  const envUrl = import.meta.env.VITE_API_BASE_URL;
  if (envUrl && envUrl.trim() !== '') {
    return envUrl.trim().replace(/\/+$/, '');
  }
  // Default to relative path (uses Vite dev proxy or same-domain deployment)
  return '';
}

export function setApiBaseUrl(url) {
  if (!url || url.trim() === '') {
    localStorage.removeItem(STORAGE_KEY);
  } else {
    localStorage.setItem(STORAGE_KEY, url.trim().replace(/\/+$/, ''));
  }
}

function resolveUrl(path) {
  const base = getApiBaseUrl();
  return base ? `${base}${path}` : path;
}

export async function fetchStats() {
  const res = await fetch(resolveUrl('/api/events/stats'));
  if (!res.ok) {
    throw new Error(`Failed to fetch stats: ${res.statusText}`);
  }
  return res.json();
}

export async function fetchAuditLogs({ page = 0, size = 10, query = '', status = '' } = {}) {
  const params = new URLSearchParams();
  params.set('page', page);
  params.set('size', size);
  if (query && query.trim()) params.set('query', query.trim());
  if (status && status.trim()) params.set('status', status.trim());

  const res = await fetch(resolveUrl(`/api/events/audit?${params.toString()}`));
  if (!res.ok) {
    throw new Error(`Failed to fetch audit logs: ${res.statusText}`);
  }
  return res.json();
}

export async function publishEvent(payload) {
  const res = await fetch(resolveUrl('/api/events/publish'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const data = await res.json();
  return { status: res.status, ok: res.ok, data };
}

export async function triggerBurst({ count = 35, includeDuplicates = true } = {}) {
  const params = new URLSearchParams();
  params.set('count', count);
  params.set('includeDuplicates', includeDuplicates);

  const res = await fetch(resolveUrl(`/api/events/burst?${params.toString()}`), {
    method: 'POST'
  });
  if (!res.ok) {
    throw new Error(`Burst simulation failed: ${res.statusText}`);
  }
  return res.json();
}

export async function checkHealth() {
  const res = await fetch(resolveUrl('/api/events/health'));
  if (!res.ok) {
    throw new Error(`Health check failed: ${res.statusText}`);
  }
  return res.json();
}

export async function testCloudConnections() {
  const res = await fetch(resolveUrl('/api/events/test-connections'));
  if (!res.ok) {
    throw new Error(`Cloud connection diagnostics failed: ${res.statusText}`);
  }
  return res.json();
}

/**
 * Subscribes to the live SSE stream.
 * @returns {() => void} unsubscribe function to close the stream.
 */
export function subscribeToEventStream(onEvent, onError, onOpen) {
  const streamUrl = resolveUrl('/api/events/stream');
  let eventSource = null;

  try {
    eventSource = new EventSource(streamUrl);

    eventSource.onopen = (e) => {
      if (onOpen) onOpen(e);
    };

    eventSource.addEventListener('event', (e) => {
      try {
        const parsed = JSON.parse(e.data);
        if (onEvent) onEvent(parsed);
      } catch (err) {
        console.error('SSE JSON parse error:', err);
      }
    });

    eventSource.onerror = (e) => {
      if (onError) onError(e);
    };
  } catch (err) {
    if (onError) onError(err);
  }

  return () => {
    if (eventSource) {
      eventSource.close();
    }
  };
}
