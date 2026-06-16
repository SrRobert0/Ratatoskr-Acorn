import type { HttpClient } from './http.js';
import type { Message, MessageHandler, ReceiveResult, SubscribeOptions } from './types.js';

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class Worker {
  private running = false;
  private activeCount = 0;
  private abortController: AbortController | null = null;

  /**
   * Mensagens recebidas no último poll mas ainda não despachadas por falta de slot.
   * Drenadas antes de qualquer novo round trip de rede.
   */
  private messageBuffer: Message[] = [];

  /**
   * Fila de resolvers pendentes de `waitForSlot`.
   * Cada entry é resolvida quando um slot de concorrência libera (ou em stop()).
   */
  private slotWaiters: Array<() => void> = [];

  /** Resolvidos quando activeCount chega a 0 após stop(). */
  private idleWaiters: Array<() => void> = [];

  private readonly concurrency: number;
  private readonly waitTimeSeconds: number;
  private readonly maxNumberOfMessages: number;
  private readonly onError: (err: Error, msg: Message | null) => void;

  constructor(
    private readonly http: HttpClient,
    private readonly queueId: string,
    private readonly handler: MessageHandler,
    options: SubscribeOptions = {},
  ) {
    this.concurrency = options.concurrency ?? 1;
    this.waitTimeSeconds = options.waitTimeSeconds ?? 10;
    this.maxNumberOfMessages = options.maxNumberOfMessages ?? this.concurrency;
    this.onError = options.onError ?? ((err) => console.error('[ratatoskr-acorn]', err));
  }

  /**
   * Inicia o loop de polling. Idempotente — chamar mais de uma vez não cria loops duplicados.
   */
  start(): this {
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
  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;

    // Descarta mensagens bufferizadas (voltam à fila após visibilityTimeout)
    this.messageBuffer.length = 0;

    // Aborta o fetch de long polling em andamento
    this.abortController?.abort();

    // Acorda o loop caso esteja suspenso em waitForSlot()
    for (const resolve of this.slotWaiters) resolve();
    this.slotWaiters = [];

    // Espera handlers em voo concluírem
    if (this.activeCount > 0) {
      await new Promise<void>((resolve) => this.idleWaiters.push(resolve));
    }
  }

  // ─── Loop principal ──────────────────────────────────────────────────────────

  private async loop(): Promise<void> {
    const signal = this.abortController!.signal;

    while (this.running) {
      // Drena o buffer antes de qualquer round trip de rede.
      // Evita poll desnecessário quando o poll anterior já trouxe mensagens extras.
      while (this.messageBuffer.length > 0 && this.activeCount < this.concurrency) {
        if (!this.running) return;
        this.activeCount++;
        void this.dispatch(this.messageBuffer.shift()!);
      }

      if (this.activeCount >= this.concurrency) {
        await this.waitForSlot().catch(() => {});
        continue;
      }

      // Buffer vazio e slots livres: busca mais mensagens na API.
      const qs = new URLSearchParams({
        maxNumberOfMessages: String(this.maxNumberOfMessages),
        waitTimeSeconds: String(this.waitTimeSeconds),
      });

      try {
        const { messages } = await this.http.get<ReceiveResult>(
          `/queues/${this.queueId}/messages?${qs}`,
          signal,
        );

        for (const msg of messages) {
          if (!this.running) break;
          if (this.activeCount < this.concurrency) {
            this.activeCount++;
            void this.dispatch(msg);
          } else {
            // Slot ocupado: guarda para despachar quando um slot abrir.
            this.messageBuffer.push(msg);
          }
        }
      } catch (err: unknown) {
        // AbortError esperado quando stop() ou timeout acionam o signal
        if (!this.running) break;

        // Erro real de poll (rede, 4xx, 5xx): notifica e faz backoff
        this.onError(toError(err), null);
        await sleep(1_000);
      }
    }
  }

  // ─── Dispatch de uma mensagem ─────────────────────────────────────────────────

  private async dispatch(msg: Message): Promise<void> {
    try {
      await this.handler(msg);
      // Sucesso → ack automático (DeleteMessage)
      await this.http.delete(
        `/queues/${this.queueId}/messages/${encodeURIComponent(msg.receiptHandle!)}`,
      );
    } catch (err: unknown) {
      // Erro no handler → notifica, NÃO faz ack.
      // A mensagem volta a ficar disponível após o visibilityTimeout expirar.
      this.onError(toError(err), msg);
    } finally {
      this.activeCount--;
      this.notifySlot();
    }
  }

  // ─── Coordenação de slots ─────────────────────────────────────────────────────

  /** Suspende até que um slot fique disponível. Resolvida por notifySlot() ou stop(). */
  private waitForSlot(): Promise<void> {
    return new Promise<void>((resolve) => {
      this.slotWaiters.push(resolve);
    });
  }

  /** Libera o próximo waiter de slot e, se idle após stop(), resolve os idle waiters. */
  private notifySlot(): void {
    const next = this.slotWaiters.shift();
    next?.();

    if (this.activeCount === 0 && !this.running && this.idleWaiters.length > 0) {
      for (const resolve of this.idleWaiters) resolve();
      this.idleWaiters = [];
    }
  }
}
