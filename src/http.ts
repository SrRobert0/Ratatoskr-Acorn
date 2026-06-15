import { RatatoskrError } from './errors.js';
import type { ClientOptions } from './types.js';

/** Combina dois AbortSignals sem depender de AbortSignal.any() (Node 20+). */
function combineSignals(a: AbortSignal, b: AbortSignal): AbortSignal {
  const controller = new AbortController();
  if (a.aborted || b.aborted) {
    controller.abort();
    return controller.signal;
  }
  const abort = () => controller.abort();
  a.addEventListener('abort', abort, { once: true });
  b.addEventListener('abort', abort, { once: true });
  return controller.signal;
}

export class HttpClient {
  private readonly baseUrl: string;
  private readonly apiKey: string;
  private readonly timeout: number;

  constructor(options: ClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/$/, '');
    this.apiKey = options.apiKey;
    // Default de 35s: comporta waitTimeSeconds=20 + margem de rede
    this.timeout = options.timeout ?? 35_000;
  }

  async request<T>(
    method: string,
    path: string,
    body?: unknown,
    signal?: AbortSignal,
  ): Promise<T> {
    const timeoutController = new AbortController();
    const timeoutId = setTimeout(() => timeoutController.abort(), this.timeout);

    // Combina o signal externo (ex: stop()) com o timeout interno.
    // Não usamos AbortSignal.any() pois ele só existe no Node >= 20.
    const combined = signal
      ? combineSignals(timeoutController.signal, signal)
      : timeoutController.signal;

    try {
      const res = await fetch(`${this.baseUrl}${path}`, {
        method,
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': this.apiKey,
        },
        body: body !== undefined ? JSON.stringify(body) : null,
        signal: combined,
      });

      // 204 No Content — sem body
      if (res.status === 204) return undefined as T;

      const data: unknown = await res.json();

      if (!res.ok) {
        const err = (data as { error?: { message?: string; code?: string; details?: unknown } })
          .error ?? {};
        throw new RatatoskrError(
          err.message ?? `HTTP ${res.status}`,
          res.status,
          err.code ?? 'UNKNOWN',
          err.details,
        );
      }

      return data as T;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  get<T>(path: string, signal?: AbortSignal): Promise<T> {
    return this.request<T>('GET', path, undefined, signal);
  }

  post<T>(path: string, body?: unknown): Promise<T> {
    return this.request<T>('POST', path, body);
  }

  delete<T>(path: string): Promise<T> {
    return this.request<T>('DELETE', path);
  }

  patch<T>(path: string, body?: unknown): Promise<T> {
    return this.request<T>('PATCH', path, body);
  }
}
