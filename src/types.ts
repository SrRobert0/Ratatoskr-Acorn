// ─── Core entities ────────────────────────────────────────────────────────────

export type MessageStatus = 'delayed' | 'available' | 'in_flight' | 'archived';

export interface Message {
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

// ─── Client config ─────────────────────────────────────────────────────────────

export interface ClientOptions {
  /** Base URL do servidor Ratatoskr (sem barra final). */
  baseUrl: string;
  /** API key enviada no header `x-api-key`. */
  apiKey: string;
  /** Timeout por request em ms. Default: 35_000 (>= waitTimeSeconds máximo de 20s + margem). */
  timeout?: number;
}

// ─── send() ───────────────────────────────────────────────────────────────────

export interface SendMessageInput {
  body: string;
  attributes?: Record<string, string>;
  /** Apenas filas FIFO. */
  messageGroupId?: string;
  /** Apenas filas FIFO com contentBasedDeduplication=false. */
  messageDeduplicationId?: string;
  /** Sobrescreve o delaySeconds da fila para esta mensagem (0–900). */
  delaySeconds?: number;
}

// ─── receive() ────────────────────────────────────────────────────────────────

export interface ReceiveOptions {
  /** Máximo de mensagens retornadas por poll (1–10). Default: 1. */
  maxNumberOfMessages?: number;
  /** Segundos de long polling (0–20). Default: 0. */
  waitTimeSeconds?: number;
  /** Sobrescreve o visibilityTimeout da fila para as mensagens recebidas (0–43200). */
  visibilityTimeout?: number;
}

export interface ReceiveResult {
  messages: Message[];
}

// ─── subscribe() ──────────────────────────────────────────────────────────────

export interface SubscribeOptions {
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

export type MessageHandler = (message: Message) => Promise<void>;

// ─── peek() ───────────────────────────────────────────────────────────────────

export interface PeekOptions {
  /** Máximo de mensagens inspecionadas (1–50). Default: 20. */
  limit?: number;
}

// ─── health ───────────────────────────────────────────────────────────────────

export interface HealthResult {
  status: 'ok';
  timestamp: string;
}
