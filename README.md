# POC Activación de Servicios con Kafka (Fases 1 a 3)

Monorepo pnpm: 6 servicios NestJS (loyalty opcional), UI React y dos librerías compartidas.

```
apps/activation-api          REST + WebSocket + agregador de la saga
apps/billing-service         crea/anula cuentas (compensación)
apps/provisioning-service    aprovisionamiento simulado (1-3 s)
apps/notification-service    emails a Mailhog
apps/crm-analytics-service   registra todos los eventos; replay (puerto 3001)
apps/loyalty-service         escenario 5: consumidor nuevo (perfil "loyalty", puerto 3002)
apps/demo-ui                 React + Vite + socket.io-client
libs/contracts               tipos de eventos, sobre común, nombres de topics
libs/kafka-toolkit           cliente Kafka y Mongo aislados (reemplazables)
docker/create-topics.sh      4 topics + DLQ, 3 particiones, retención 7 días
```

## Levantar todo
```bash
docker compose up --build
```
| Qué | URL |
|---|---|
| Demo UI | http://localhost:5173 |
| activation-api | http://localhost:3000 |
| Kafka UI | http://localhost:8080 |
| Mailhog | http://localhost:8025 |

## Alcance
**Fases 1 y 2:** RF-01 a RF-09, RNF-01, RNF-02 (clave = customerId), RNF-06, RNF-07.

**Fase 3 (robustez):**
- RNF-03 Idempotencia: cada consumidor registra el `eventId` en `processed_events` (índice único, TTL de 7 días)
  **en la misma transacción de Mongo que su efecto**.
- RNF-04 Reintentos y DLQ: ante un error se reintenta 3 veces (1 s, 2 s, 4 s); si sigue fallando, el mensaje va a
  `<topic>.dlq` con el motivo en los headers y se continúa con el siguiente.
- RNF-05: commit de offset manual, siempre después de procesar; un servicio caído retoma sin perder eventos.
- RF-10: `crm-analytics` registra todos los eventos y puede reprocesar el historial (`POST :3001/replay`).
- Servicio `loyalty` y escenarios 3 a 5 (ver `docs/demo-script.md`).

Toda la lógica de validación, idempotencia, reintentos, DLQ y commit vive en `libs/kafka-toolkit`
(`EventConsumer.handle`), así los servicios solo escriben su lógica de negocio.

**Pendiente (Fase 4, opcional):** outbox transaccional, Schema Registry, OpenTelemetry. Hoy se publica directo:
si un servicio se cae entre guardar en Mongo y publicar, el evento se pierde (riesgo documentado). Si la
transacción falla después de publicar, se reprocesa y puede publicarse dos veces; por eso los consumidores
son idempotentes.

Ver `docs/demo-script.md`.
