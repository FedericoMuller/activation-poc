# Guión de demo (escenarios 1 a 5)

Pantalla dividida: Demo UI (http://localhost:5173) a la izquierda, Kafka UI (http://localhost:8080) a la derecha.
Mailhog: http://localhost:8025 · crm-analytics: http://localhost:3001 · loyalty: http://localhost:3002

## 1 · Camino feliz (concepto: fan-out)
Contratar `PLAN DIGITAL` sin fallo. Timeline: ActivationRequested → BillingAccountCreated →
ProvisioningCompleted → ActivationCompleted. Email en Mailhog. Un solo mensaje en
`activation.requested` lo consumen billing, provisioning y crm-analytics.

## 2 · Fallo y compensación (concepto: saga sin llamadas directas)
Contratar con fallo **En aprovisionamiento**. Timeline: ProvisioningFailed → ActivationFailed →
BillingAccountCancelled. Email de error. Nadie llamó a billing: reaccionó a `ActivationFailed`.

## 3 · Servicio caído (concepto: offsets, nadie pierde mensajes)
```bash
docker compose stop notification-service
```
Crear 3 activaciones desde la UI (sin fallo). En Kafka UI → Consumers → `notification-svc`: lag de 3.
```bash
docker compose start notification-service
```
Llegan los 3 emails a Mailhog y el lag vuelve a 0.

## 4 · Escalar (concepto: consumer group y particiones)
```bash
docker compose up -d --scale billing-service=2
```
En Kafka UI → Consumers → `billing-svc`: las particiones se reparten entre 2 miembros
(el rebalanceo tarda unos segundos: es parte de la demo). Crear activaciones: siguen funcionando.
Para volver: `docker compose up -d --scale billing-service=1`.

## 5 · Nuevo consumidor (concepto: replay del log)
```bash
docker compose --profile loyalty up -d --build loyalty-service
curl localhost:3002/points
```
El servicio `loyalty` (grupo nuevo, lee desde el inicio) procesa todo el historial de
`activation.events` sin tocar a los demás: devuelve los puntos de las activaciones pasadas.

## Extras de la Fase 3 (criterios de aceptación)

**Idempotencia — reenviar un evento ya procesado no duplica nada.**
En Kafka UI abrí un mensaje de `activation.events` (ActivationCompleted), copiá su JSON y su key,
y producilo de nuevo en el mismo topic (*Produce Message*). En los logs de notification aparece
"ya procesado; se descarta" y Mailhog no recibe un segundo email.

**DLQ — un mensaje inválido no frena el flujo.**
Produci en `activation.requested` el mensaje `{"foo":"bar"}`. Los consumidores reintentan (1 s, 2 s, 4 s) y
a los ~7 s aparece en `activation.requested.dlq` (uno por consumidor que falló) con el motivo en los headers
(`x-error`, `x-failed-consumer`, ...). Mientras tanto, contratar un plan sigue funcionando.

**Trazabilidad y replay (RF-10 / RNF-06).**
```bash
curl localhost:3001/stats
curl "localhost:3001/events?correlationId=act-xxxxxxxx"
curl -X POST localhost:3001/replay      # vacía event_log y lo reconstruye releyendo los topics
```
