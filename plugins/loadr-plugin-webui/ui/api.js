/* loadr api.js — fetch wrapper with auth, JSON helpers and SSE-over-fetch. */
'use strict';

window.API = (function () {
  const AUTH_KEY = 'loadr.auth';
  let authHeader = sessionStorage.getItem(AUTH_KEY) || null;
  const listeners = { unauthorized: [] };

  function on(event, fn) {
    (listeners[event] = listeners[event] || []).push(fn);
  }

  function emit(event) {
    (listeners[event] || []).forEach((fn) => {
      try {
        fn();
      } catch (e) {
        /* listener errors must not break the app */
      }
    });
  }

  function setAuth(header) {
    authHeader = header;
    if (header) sessionStorage.setItem(AUTH_KEY, header);
    else sessionStorage.removeItem(AUTH_KEY);
  }

  function headers(extra) {
    const h = Object.assign({}, extra || {});
    if (authHeader) h['Authorization'] = authHeader;
    return h;
  }

  async function request(path, opts = {}) {
    const res = await fetch(path, Object.assign({}, opts, { headers: headers(opts.headers) }));
    if (res.status === 401) {
      emit('unauthorized');
      const err = new Error('unauthorized');
      err.status = 401;
      throw err;
    }
    const text = await res.text();
    let body = null;
    try {
      body = text ? JSON.parse(text) : null;
    } catch (e) {
      body = text;
    }
    if (!res.ok) {
      const err = new Error((body && body.error) || 'HTTP ' + res.status);
      err.status = res.status;
      err.body = body;
      throw err;
    }
    return body;
  }

  const json = (method) => (path, body) =>
    request(path, {
      method,
      headers: body !== undefined ? { 'Content-Type': 'application/json' } : {},
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });

  /**
   * Consume a server-sent-event stream via fetch so the Authorization header
   * applies (EventSource cannot set headers). Reconnects on network errors.
   * @returns {{close: () => void}}
   */
  function sse(path, handlers) {
    let closed = false;
    let ctrl = new AbortController();

    (async function pump() {
      while (!closed) {
        try {
          const res = await fetch(path, {
            headers: headers({ Accept: 'text/event-stream' }),
            signal: ctrl.signal,
          });
          if (res.status === 401) {
            emit('unauthorized');
            return;
          }
          if (!res.ok || !res.body) throw new Error('stream failed: ' + res.status);
          if (handlers.open) handlers.open();
          const reader = res.body.getReader();
          const decoder = new TextDecoder();
          let buf = '';
          for (;;) {
            const { done, value } = await reader.read();
            if (done) break;
            buf += decoder.decode(value, { stream: true });
            let idx;
            while ((idx = buf.indexOf('\n\n')) >= 0) {
              const raw = buf.slice(0, idx);
              buf = buf.slice(idx + 2);
              let event = 'message';
              const data = [];
              for (const line of raw.split('\n')) {
                if (line.startsWith('event:')) event = line.slice(6).trim();
                else if (line.startsWith('data:')) data.push(line.slice(5).replace(/^ /, ''));
                // lines starting with ':' are keep-alive comments
              }
              if (data.length && handlers[event]) {
                try {
                  handlers[event](JSON.parse(data.join('\n')));
                } catch (e) {
                  /* malformed frame — skip */
                }
              }
            }
          }
          // Stream ended cleanly (e.g. run finished).
          if (!closed && handlers.end) handlers.end();
          return;
        } catch (e) {
          if (closed) return;
          if (handlers.error) handlers.error(e);
          if (handlers.reconnecting) handlers.reconnecting();
          await new Promise((r) => setTimeout(r, 2000));
          ctrl = new AbortController();
        }
      }
    })();

    return {
      close() {
        closed = true;
        ctrl.abort();
      },
    };
  }

  return {
    get: (path) => request(path),
    post: json('POST'),
    put: json('PUT'),
    del: (path) => request(path, { method: 'DELETE' }),
    request,
    sse,
    on,
    setAuth,
    hasAuth: () => !!authHeader,
  };
})();
