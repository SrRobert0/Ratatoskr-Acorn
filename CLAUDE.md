# ratatoskr-acorn

SDK TypeScript para consumir a API de filas [Ratatoskr](https://github.com/SrRobert0/Ratatoskr).

- **API docs**: https://ratatoskr-3uik.onrender.com/docs
- **Pacote npm**: `ratatoskr-acorn`
- **Node mínimo**: 18 (usa `fetch` nativo)
- **Output**: dual ESM + CJS via tsup

---

## Comandos

```bash
npm install          # instala devDependencies (tsup, typescript)
npm run build        # compila para dist/ (ESM + CJS + .d.ts)
npm run dev          # watch mode
npm run typecheck    # tsc sem emitir arquivos
```

---

## Estrutura

```
src/
├── index.ts       # exports públicos
├── client.ts      # RatatoskrAcorn — classe principal
├── worker.ts      # Worker — loop de polling + concorrência
├── http.ts        # HttpClient — wrapper de fetch com auth e timeout
├── types.ts       # Interfaces e tipos TypeScript
└── errors.ts      # RatatoskrError
```

---

## Decisões de design

### Autenticação
Todas as requests injetam o header `x-api-key` automaticamente via `HttpClient`. O usuário só configura a key uma vez no construtor.

### Long polling
O método `receive()` e o loop interno do `Worker` passam `waitTimeSeconds` como query param. O `HttpClient` tem timeout padrão de 35s para acomodar o máximo de 20s de wait + margem de rede.

### Combinação de AbortSignal
`AbortSignal.any()` só existe no Node >= 20. Usamos um helper `combineSignals()` manual em `http.ts` para combinar o timeout interno com o signal externo (usado pelo `Worker` para cancelar o long polling no `stop()`).

### Concorrência no Worker
O `Worker` mantém `activeCount` (handlers em execução) e uma fila `slotWaiters` de resolvers.

**Loop flow:**
1. Calcula `available = concurrency - activeCount`
2. Se `available <= 0`: suspende em `waitForSlot()` até algum dispatch terminar
3. Faz poll com `maxNumberOfMessages = min(available, maxNumberOfMessages)`
4. Despacha cada mensagem em paralelo via `void dispatch(msg)`

**Ack/nack:**
- Sucesso no handler → `DeleteMessage` automático (ack)
- Erro no handler → sem ack; mensagem volta após `visibilityTimeout` expirar (nack implícito)

### Graceful stop
`stop()` faz três coisas em ordem:
1. `this.running = false`
2. `abortController.abort()` → cancela o fetch de long polling imediatamente
3. Resolve todos os `slotWaiters` pendentes → desbloqueia o loop
4. Aguarda `activeCount === 0` antes de resolver a Promise retornada

### Escape hatch
Os métodos `receive()`, `ack()`, `peek()` e `changeVisibility()` são expostos no cliente para quem precisar de controle manual fora do ciclo automático do `subscribe()`.

---

## Rotas usadas

| Método  | Rota                                                          | Uso                        |
|---------|---------------------------------------------------------------|----------------------------|
| GET     | `/queues/{id}/messages`                                       | poll (subscribe + receive) |
| DELETE  | `/queues/{id}/messages/{receiptHandle}`                       | ack automático             |
| PATCH   | `/queues/{id}/messages/{receiptHandle}/visibility`            | changeVisibility()         |
| POST    | `/queues/{id}/messages`                                       | send()                     |
| GET     | `/queues/{id}/messages/peek`                                  | peek()                     |
| GET     | `/health`                                                     | health()                   |

Gerenciamento de filas (CRUD), API keys e redrive/arquivamento ficam fora do escopo da lib — o consumidor já tem as filas criadas.

---

## Exemplo de uso

```ts
import { RatatoskrAcorn } from 'ratatoskr-acorn';

const acorn = new RatatoskrAcorn({
  baseUrl: 'https://ratatoskr.example.com',
  apiKey: process.env.RATATOSKR_API_KEY!,
});

// Consumo contínuo com 5 workers paralelos
const worker = acorn.subscribe('queue-id', async (msg) => {
  await processOrder(JSON.parse(msg.body));
}, {
  concurrency: 5,
  waitTimeSeconds: 10,
  onError: (err, msg) => console.error('Falha:', err, msg?.id),
});

worker.start();

// Shutdown graceful
process.on('SIGTERM', async () => {
  await worker.stop();
  process.exit(0);
});
```
