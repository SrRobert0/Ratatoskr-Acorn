import { HttpClient } from './http.js';
import { Worker } from './worker.js';
import type {
  ClientOptions,
  HealthResult,
  Message,
  MessageHandler,
  PeekOptions,
  ReceiveOptions,
  ReceiveResult,
  SendMessageInput,
  SubscribeOptions,
} from './types.js';

export class RatatoskrAcorn {
  private readonly http: HttpClient;

  constructor(options: ClientOptions) {
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
  subscribe(
    queueId: string,
    handler: MessageHandler,
    options?: SubscribeOptions,
  ): Worker {
    return new Worker(this.http, queueId, handler, options);
  }

  // ─── Messages: low-level ────────────────────────────────────────────────────

  /**
   * Envia uma mensagem para a fila.
   */
  async send(queueId: string, input: SendMessageInput): Promise<Message> {
    return this.http.post<Message>(`/queues/${queueId}/messages`, input);
  }

  /**
   * Recebe mensagens da fila manualmente (com suporte a long polling).
   * Prefira `subscribe()` para consumo contínuo.
   */
  async receive(queueId: string, options: ReceiveOptions = {}): Promise<ReceiveResult> {
    const qs = new URLSearchParams();
    if (options.maxNumberOfMessages !== undefined)
      qs.set('maxNumberOfMessages', String(options.maxNumberOfMessages));
    if (options.waitTimeSeconds !== undefined)
      qs.set('waitTimeSeconds', String(options.waitTimeSeconds));
    if (options.visibilityTimeout !== undefined)
      qs.set('visibilityTimeout', String(options.visibilityTimeout));

    const query = qs.toString();
    return this.http.get<ReceiveResult>(
      `/queues/${queueId}/messages${query ? `?${query}` : ''}`,
    );
  }

  /**
   * Confirma o processamento de uma mensagem (ack / DeleteMessage).
   */
  async ack(queueId: string, receiptHandle: string): Promise<void> {
    await this.http.delete(
      `/queues/${queueId}/messages/${encodeURIComponent(receiptHandle)}`,
    );
  }

  /**
   * Inspeciona mensagens da fila **sem** consumi-las (PeekMessages).
   * Não altera status, receiptHandle nem receiveCount.
   */
  async peek(queueId: string, options: PeekOptions = {}): Promise<ReceiveResult> {
    const qs = options.limit !== undefined
      ? `?limit=${options.limit}`
      : '';
    return this.http.get<ReceiveResult>(`/queues/${queueId}/messages/peek${qs}`);
  }

  /**
   * Altera o visibility timeout de uma mensagem em voo.
   * Útil para estender o prazo de processamento de mensagens pesadas.
   */
  async changeVisibility(
    queueId: string,
    receiptHandle: string,
    visibilityTimeout: number,
  ): Promise<void> {
    await this.http.patch(
      `/queues/${queueId}/messages/${encodeURIComponent(receiptHandle)}/visibility`,
      { visibilityTimeout },
    );
  }

  // ─── Health ─────────────────────────────────────────────────────────────────

  /**
   * Verifica se o servidor está acessível.
   */
  async health(): Promise<HealthResult> {
    return this.http.get<HealthResult>('/health');
  }
}
