import { randomUUID } from 'crypto';

/** Versión del contrato de eventos (RNF-07). Cambios incompatibles => subir versión. */
export const CONTRACT_VERSION = 1;

export const TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested',
  BILLING_EVENTS: 'billing.events',
  PROVISIONING_EVENTS: 'provisioning.events',
  ACTIVATION_EVENTS: 'activation.events',
} as const;

export type Topic = (typeof TOPICS)[keyof typeof TOPICS];
export const dlqOf = (topic: string) => `${topic}.dlq`;

export type SimulateFailure = 'none' | 'billing' | 'provisioning';
export type ActivationStatus = 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';

export type EventType =
  | 'ActivationRequested'
  | 'BillingAccountCreated'
  | 'BillingFailed'
  | 'BillingAccountCancelled'
  | 'ProvisioningCompleted'
  | 'ProvisioningFailed'
  | 'ActivationCompleted'
  | 'ActivationFailed';

/** Sobre común a todos los eventos. El payload cambia según eventType. */
export interface EventEnvelope<T = Record<string, unknown>> {
  eventId: string;
  eventType: EventType;
  version: number;
  occurredAt: string;
  /** = activationId; agrupa todos los eventos de una activación (RNF-06). */
  correlationId: string;
  /** También es la clave del mensaje en Kafka (RNF-02). */
  customerId: string;
  source: string;
  payload: T;
}

export interface ActivationRequestedPayload {
  planId: string;
  channel: string;
  simulateFailure: SimulateFailure;
}
export interface BillingAccountCreatedPayload { accountId: string }
export interface BillingFailedPayload { reason: string }
export interface BillingAccountCancelledPayload { accountId: string; reason: string }
export interface ProvisioningCompletedPayload { orderId: string }
export interface ProvisioningFailedPayload { reason: string }
export interface ActivationCompletedPayload { planId: string }
export interface ActivationFailedPayload {
  planId: string;
  failedStep: 'billing' | 'provisioning';
  reason: string;
}

export function buildEvent<T>(args: {
  eventType: EventType;
  correlationId: string;
  customerId: string;
  source: string;
  payload: T;
}): EventEnvelope<T> {
  return {
    eventId: randomUUID(),
    eventType: args.eventType,
    version: CONTRACT_VERSION,
    occurredAt: new Date().toISOString(),
    correlationId: args.correlationId,
    customerId: args.customerId,
    source: args.source,
    payload: args.payload,
  };
}

/** Mensaje que la API empuja a la UI por WebSocket (RF-08). */
export interface TimelineMessage {
  activationId: string;
  customerId: string;
  planId: string;
  eventType: EventType;
  source: string;
  at: string;
  status: ActivationStatus;
}
