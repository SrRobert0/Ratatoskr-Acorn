export class RatatoskrError extends Error {
  readonly statusCode: number;
  readonly code: string;
  readonly details: unknown;

  constructor(message: string, statusCode: number, code: string, details?: unknown) {
    super(message);
    this.name = 'RatatoskrError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;

    // Mantém o prototype correto ao transpilar para CommonJS
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
