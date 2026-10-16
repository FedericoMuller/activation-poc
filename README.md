# POC Activación de Servicios con Kafka (Fase 2)

Monorepo pnpm: 4 servicios NestJS, UI React y dos librerías compartidas.

```
apps/activation-api          REST + WebSocket + agregador de la saga
apps/billing-service         crea/anula cuentas (compensación)
apps/provisioning-service    aprovisionamiento simulado (1-3 s)
apps/notification-service    emails a Mailhog
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

## Alcance de la Fase 2
Incluido: RF-01 a RF-09 (UI, 202 Accepted, saga con billing/provisioning en paralelo, compensación,
email, timeline por WebSocket, GET con historial), RNF-01, RNF-02 (clave = customerId), RNF-06, RNF-07.

Pendiente para la Fase 3: crm-analytics y replay (RF-10), idempotencia con `processed_events` (RNF-03),
reintentos + DLQ (RNF-04), commit manual de offsets, servicio `loyalty` y escenarios 3 a 5.
Pendiente para la Fase 4: outbox transaccional. Hoy se publica directo después de guardar en Mongo
(si la API se cae entre ambos pasos, el evento se pierde: riesgo documentado).

Ver `docs/demo-script.md`.
