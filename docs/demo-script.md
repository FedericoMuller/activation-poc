# Guión de demo (Fase 2: escenarios 1 y 2)

Pantalla dividida: Demo UI (http://localhost:5173) a la izquierda, Kafka UI (http://localhost:8080) a la derecha.
Mailhog en http://localhost:8025.

## Escenario 1 · Camino feliz (concepto: fan-out)
1. En la UI, contratar `PLAN DIGITAL` con fallo **Ninguno**.
2. Se ve la timeline: ActivationRequested → BillingAccountCreated → ProvisioningCompleted → ActivationCompleted.
3. Mailhog muestra el email de bienvenida.
4. En Kafka UI: un único mensaje en `activation.requested` que consumen billing y provisioning.

## Escenario 2 · Fallo y compensación (concepto: saga sin llamadas directas)
1. Contratar con fallo **En aprovisionamiento**.
2. Timeline: ProvisioningFailed → ActivationFailed → BillingAccountCancelled.
3. Mailhog muestra el email de error.
4. Explicar: nadie llamó a billing; reaccionó a `ActivationFailed` en `activation.events`.

Variante: fallo **En facturación** (BillingFailed → ActivationFailed; no hay cuenta que anular).

## Comandos útiles
```bash
docker compose up -d --build
curl -X POST localhost:3000/activations -H 'Content-Type: application/json' \
  -d '{"customerId":"C-1234","planId":"PLAN DIGITAL","simulateFailure":"none"}'
curl localhost:3000/activations/<activationId>
docker compose logs -f billing-service provisioning-service
```
