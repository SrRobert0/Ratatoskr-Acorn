'use strict';

// src/errors.ts
var RatatoskrError = class extends Error {
  statusCode;
  code;
  details;
  constructor(message, statusCode, code, details) {
    super(message);
    this.name = "RatatoskrError";
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
    Object.setPrototypeOf(this, new.target.prototype);
  }
};

// src/http.ts
function combineSignals(a, b) {
  const controller = new AbortController();
  if (a.aborted || b.aborted) {
    controller.abort();
    return [controller.signal, () => {
    }];
  }
  const abort = () => controller.abort();
  a.addEventListener("abort", abort, { once: true });
  b.addEventListener("abort", abort, { once: true });
  return [controller.signal, () => b.removeEventListener("abort", abort)];
}
var HttpClient = class {
  baseUrl;
  apiKey;
  timeout;
  constructor(options) {
    this.baseUrl = options.baseUrl.replace(/\/$/, "");
    this.apiKey = options.apiKey;
    this.timeout = options.timeout ?? 35e3;
  }
  async request(method, path, body, signal) {
    const timeoutController = new AbortController();
    const timeoutId = setTimeout(() => timeoutController.abort(), this.timeout);
    const [combined, cleanupSignal] = signal ? combineSignals(timeoutController.signal, signal) : [timeoutController.signal, () => {
    }];
    try {
      const res = await fetch(`${this.baseUrl}${path}`, {
        method,
        headers: {
          "Content-Type": "application/json",
          "x-api-key": this.apiKey
        },
        body: body !== void 0 ? JSON.stringify(body) : null,
        signal: combined
      });
      if (res.status === 204) return void 0;
      const data = await res.json();
      if (!res.ok) {
        const err = data.error ?? {};
        throw new RatatoskrError(
          err.message ?? `HTTP ${res.status}`,
          res.status,
          err.code ?? "UNKNOWN",
          err.details
        );
      }
      return data;
    } finally {
      clearTimeout(timeoutId);
      cleanupSignal();
    }
  }
  get(path, signal) {
    return this.request("GET", path, void 0, signal);
  }
  post(path, body) {
    return this.request("POST", path, body);
  }
  delete(path) {
    return this.request("DELETE", path);
  }
  patch(path, body) {
    return this.request("PATCH", path, body);
  }
};

// src/worker.ts
function toError(value) {
  return value instanceof Error ? value : new Error(String(value));
}
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
var Worker = class {
  constructor(http, queueId, handler, options = {}) {
    this.http = http;
    this.queueId = queueId;
    this.handler = handler;
    this.concurrency = options.concurrency ?? 1;
    this.waitTimeSeconds = options.waitTimeSeconds ?? 10;
    this.maxNumberOfMessages = options.maxNumberOfMessages ?? this.concurrency;
    this.onError = options.onError ?? ((err) => console.error("[ratatoskr-acorn]", err));
  }
  http;
  queueId;
  handler;
  running = false;
  activeCount = 0;
  abortController = null;
  /**
   * Mensagens recebidas no último poll mas ainda não despachadas por falta de slot.
   * Drenadas antes de qualquer novo round trip de rede.
   */
  messageBuffer = [];
  /**
   * Fila de resolvers pendentes de `waitForSlot`.
   * Cada entry é resolvida quando um slot de concorrência libera (ou em stop()).
   */
  slotWaiters = [];
  /** Resolvidos quando activeCount chega a 0 após stop(). */
  idleWaiters = [];
  concurrency;
  waitTimeSeconds;
  maxNumberOfMessages;
  onError;
  /**
   * Inicia o loop de polling. Idempotente — chamar mais de uma vez não cria loops duplicados.
   */
  start() {
    if (this.running) return this;
    this.running = true;
    this.abortController = new AbortController();
    void this.loop();
    return this;
  }
  /**
   * Para o worker de forma graceful:
   * 1. Aborta o fetch em andamento (long polling encerra imediatamente).
   * 2. Acorda qualquer loop parado esperando slot.
   * 3. Aguarda todas as mensagens em processamento terminarem.
   */
  async stop() {
    if (!this.running) return;
    this.running = false;
    this.messageBuffer.length = 0;
    this.abortController?.abort();
    for (const resolve of this.slotWaiters) resolve();
    this.slotWaiters = [];
    if (this.activeCount > 0) {
      await new Promise((resolve) => this.idleWaiters.push(resolve));
    }
  }
  // ─── Loop principal ──────────────────────────────────────────────────────────
  async loop() {
    const signal = this.abortController.signal;
    while (this.running) {
      while (this.messageBuffer.length > 0 && this.activeCount < this.concurrency) {
        if (!this.running) return;
        this.activeCount++;
        void this.dispatch(this.messageBuffer.shift());
      }
      if (this.activeCount >= this.concurrency) {
        await this.waitForSlot().catch(() => {
        });
        continue;
      }
      const qs = new URLSearchParams({
        maxNumberOfMessages: String(this.maxNumberOfMessages),
        waitTimeSeconds: String(this.waitTimeSeconds)
      });
      try {
        const { messages } = await this.http.get(
          `/queues/${this.queueId}/messages?${qs}`,
          signal
        );
        for (const msg of messages) {
          if (!this.running) break;
          if (this.activeCount < this.concurrency) {
            this.activeCount++;
            void this.dispatch(msg);
          } else {
            this.messageBuffer.push(msg);
          }
        }
      } catch (err) {
        if (!this.running) break;
        this.onError(toError(err), null);
        await sleep(1e3);
      }
    }
  }
  // ─── Dispatch de uma mensagem ─────────────────────────────────────────────────
  async dispatch(msg) {
    try {
      await this.handler(msg);
      await this.http.delete(
        `/queues/${this.queueId}/messages/${encodeURIComponent(msg.receiptHandle)}`
      );
    } catch (err) {
      this.onError(toError(err), msg);
    } finally {
      this.activeCount--;
      this.notifySlot();
    }
  }
  // ─── Coordenação de slots ─────────────────────────────────────────────────────
  /** Suspende até que um slot fique disponível. Resolvida por notifySlot() ou stop(). */
  waitForSlot() {
    return new Promise((resolve) => {
      this.slotWaiters.push(resolve);
    });
  }
  /** Libera o próximo waiter de slot e, se idle após stop(), resolve os idle waiters. */
  notifySlot() {
    const next = this.slotWaiters.shift();
    next?.();
    if (this.activeCount === 0 && !this.running && this.idleWaiters.length > 0) {
      for (const resolve of this.idleWaiters) resolve();
      this.idleWaiters = [];
    }
  }
};

// src/client.ts
var RatatoskrAcorn = class {
  http;
  constructor(options) {
    this.http = new HttpClient(options);
  }
  // ─── High-level: subscribe ──────────────────────────────────────────────────
  /**
   * Observa uma fila com long polling.
   *
   * O `handler` é chamado para cada mensagem recebida. Em caso de sucesso,
   * a mensagem é deletada automaticamente (ack). Em caso de erro, a mensagem
   * volta a ficar disponível após o `visibilityTimeout` da fila (nack implícito).
   *
   * @example
   * const worker = acorn.subscribe('queue-id', async (msg) => {
   *   await process(JSON.parse(msg.body));
   * }, { concurrency: 5, waitTimeSeconds: 10 });
   *
   * worker.start();
   * // ...
   * await worker.stop();
   */
  subscribe(queueId, handler, options) {
    return new Worker(this.http, queueId, handler, options);
  }
  // ─── Messages: low-level ────────────────────────────────────────────────────
  /**
   * Envia uma mensagem para a fila.
   */
  async send(queueId, input) {
    return this.http.post(`/queues/${queueId}/messages`, input);
  }
  /**
   * Recebe mensagens da fila manualmente (com suporte a long polling).
   * Prefira `subscribe()` para consumo contínuo.
   */
  async receive(queueId, options = {}) {
    const qs = new URLSearchParams();
    if (options.maxNumberOfMessages !== void 0)
      qs.set("maxNumberOfMessages", String(options.maxNumberOfMessages));
    if (options.waitTimeSeconds !== void 0)
      qs.set("waitTimeSeconds", String(options.waitTimeSeconds));
    if (options.visibilityTimeout !== void 0)
      qs.set("visibilityTimeout", String(options.visibilityTimeout));
    const query = qs.toString();
    return this.http.get(
      `/queues/${queueId}/messages${query ? `?${query}` : ""}`
    );
  }
  /**
   * Confirma o processamento de uma mensagem (ack / DeleteMessage).
   */
  async ack(queueId, receiptHandle) {
    await this.http.delete(
      `/queues/${queueId}/messages/${encodeURIComponent(receiptHandle)}`
    );
  }
  /**
   * Inspeciona mensagens da fila **sem** consumi-las (PeekMessages).
   * Não altera status, receiptHandle nem receiveCount.
   */
  async peek(queueId, options = {}) {
    const qs = options.limit !== void 0 ? `?limit=${options.limit}` : "";
    return this.http.get(`/queues/${queueId}/messages/peek${qs}`);
  }
  /**
   * Altera o visibility timeout de uma mensagem em voo.
   * Útil para estender o prazo de processamento de mensagens pesadas.
   */
  async changeVisibility(queueId, receiptHandle, visibilityTimeout) {
    await this.http.patch(
      `/queues/${queueId}/messages/${encodeURIComponent(receiptHandle)}/visibility`,
      { visibilityTimeout }
    );
  }
  // ─── Health ─────────────────────────────────────────────────────────────────
  /**
   * Verifica se o servidor está acessível.
   */
  async health() {
    return this.http.get("/health");
  }
};

exports.RatatoskrAcorn = RatatoskrAcorn;
exports.RatatoskrError = RatatoskrError;
exports.Worker = Worker;
//# sourceMappingURL=index.cjs.map
//# sourceMappingURL=index.cjs.map