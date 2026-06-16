type MessageStatus = 'delayed' | 'available' | 'in_flight' | 'archived';
interface Message {
    id: string;
    body: string;
    attributes: Record<string, string>;
    messageGroupId: string | null;
    messageDeduplicationId: string | null;
    receiptHandle: string | null;
    receiveCount: number;
    status: MessageStatus;
    createdAt: string;
    availableAt: string;
    archivedAt: string | null;
}
interface ClientOptions {
    /** Base URL do servidor Ratatoskr (sem barra final). */
    baseUrl: string;
    /** API key enviada no header `x-api-key`. */
    apiKey: string;
    /** Timeout por request em ms. Default: 35_000 (>= waitTimeSeconds máximo de 20s + margem). */
    timeout?: number;
}
interface SendMessageInput {
    body: string;
    attributes?: Record<string, string>;
    /** Apenas filas FIFO. */
    messageGroupId?: string;
    /** Apenas filas FIFO com contentBasedDeduplication=false. */
    messageDeduplicationId?: string;
    /** Sobrescreve o delaySeconds da fila para esta mensagem (0–900). */
    delaySeconds?: number;
}
interface ReceiveOptions {
    /** Máximo de mensagens retornadas por poll (1–10). Default: 1. */
    maxNumberOfMessages?: number;
    /** Segundos de long polling (0–20). Default: 0. */
    waitTimeSeconds?: number;
    /** Sobrescreve o visibilityTimeout da fila para as mensagens recebidas (0–43200). */
    visibilityTimeout?: number;
}
interface ReceiveResult {
    messages: Message[];
}
interface SubscribeOptions {
    /** Máximo de mensagens processadas em paralelo. Default: 1. */
    concurrency?: number;
    /** Segundos de long polling por ciclo (0–20). Default: 10. */
    waitTimeSeconds?: number;
    /**
     * Quantas mensagens buscar por poll (1–10).
     * Mensagens que excedam os slots livres são bufferizadas e despachadas
     * sem round trip adicional conforme os slots abrem. Default: igual ao `concurrency`.
     */
    maxNumberOfMessages?: number;
    /**
     * Chamado quando o handler lança um erro (a mensagem volta após visibilityTimeout)
     * ou quando um ciclo de poll falha (msg será null).
     */
    onError?: (error: Error, message: Message | null) => void;
}
type MessageHandler = (message: Message) => Promise<void>;
interface PeekOptions {
    /** Máximo de mensagens inspecionadas (1–50). Default: 20. */
    limit?: number;
}
interface HealthResult {
    status: 'ok';
    timestamp: string;
}

declare class HttpClient {
    private readonly baseUrl;
    private readonly apiKey;
    private readonly timeout;
    constructor(options: ClientOptions);
    request<T>(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<T>;
    get<T>(path: string, signal?: AbortSignal): Promise<T>;
    post<T>(path: string, body?: unknown): Promise<T>;
    delete<T>(path: string): Promise<T>;
    patch<T>(path: string, body?: unknown): Promise<T>;
}

declare class Worker {
    private readonly http;
    private readonly queueId;
    private readonly handler;
    private running;
    private activeCount;
    private abortController;
    /**
     * Mensagens recebidas no último poll mas ainda não despachadas por falta de slot.
     * Drenadas antes de qualquer novo round trip de rede.
     */
    private messageBuffer;
    /**
     * Fila de resolvers pendentes de `waitForSlot`.
     * Cada entry é resolvida quando um slot de concorrência libera (ou em stop()).
     */
    private slotWaiters;
    /** Resolvidos quando activeCount chega a 0 após stop(). */
    private idleWaiters;
    private readonly concurrency;
    private readonly waitTimeSeconds;
    private readonly maxNumberOfMessages;
    private readonly onError;
    constructor(http: HttpClient, queueId: string, handler: MessageHandler, options?: SubscribeOptions);
    /**
     * Inicia o loop de polling. Idempotente — chamar mais de uma vez não cria loops duplicados.
     */
    start(): this;
    /**
     * Para o worker de forma graceful:
     * 1. Aborta o fetch em andamento (long polling encerra imediatamente).
     * 2. Acorda qualquer loop parado esperando slot.
     * 3. Aguarda todas as mensagens em processamento terminarem.
     */
    stop(): Promise<void>;
    private loop;
    private dispatch;
    /** Suspende até que um slot fique disponível. Resolvida por notifySlot() ou stop(). */
    private waitForSlot;
    /** Libera o próximo waiter de slot e, se idle após stop(), resolve os idle waiters. */
    private notifySlot;
}

declare class RatatoskrAcorn {
    private readonly http;
    constructor(options: ClientOptions);
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
    subscribe(queueId: string, handler: MessageHandler, options?: SubscribeOptions): Worker;
    /**
     * Envia uma mensagem para a fila.
     */
    send(queueId: string, input: SendMessageInput): Promise<Message>;
    /**
     * Recebe mensagens da fila manualmente (com suporte a long polling).
     * Prefira `subscribe()` para consumo contínuo.
     */
    receive(queueId: string, options?: ReceiveOptions): Promise<ReceiveResult>;
    /**
     * Confirma o processamento de uma mensagem (ack / DeleteMessage).
     */
    ack(queueId: string, receiptHandle: string): Promise<void>;
    /**
     * Inspeciona mensagens da fila **sem** consumi-las (PeekMessages).
     * Não altera status, receiptHandle nem receiveCount.
     */
    peek(queueId: string, options?: PeekOptions): Promise<ReceiveResult>;
    /**
     * Altera o visibility timeout de uma mensagem em voo.
     * Útil para estender o prazo de processamento de mensagens pesadas.
     */
    changeVisibility(queueId: string, receiptHandle: string, visibilityTimeout: number): Promise<void>;
    /**
     * Verifica se o servidor está acessível.
     */
    health(): Promise<HealthResult>;
}

declare class RatatoskrError extends Error {
    readonly statusCode: number;
    readonly code: string;
    readonly details: unknown;
    constructor(message: string, statusCode: number, code: string, details?: unknown);
}

export { type ClientOptions, type HealthResult, type Message, type MessageHandler, type MessageStatus, type PeekOptions, RatatoskrAcorn, RatatoskrError, type ReceiveOptions, type ReceiveResult, type SendMessageInput, type SubscribeOptions, Worker };
