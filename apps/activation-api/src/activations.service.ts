import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { Collection, Db, Document } from 'mongodb';
import {
  ActivationCompletedPayload,
  ActivationFailedPayload,
  ActivationRequestedPayload,
  ActivationStatus,
  buildEvent,
  EventEnvelope,
  EventType,
  SimulateFailure,
  TOPICS,
} from '@poc/contracts';
import { EventPublisher, MONGO_DB } from '@poc/kafka-toolkit';
import { ActivationsGateway } from './activations.gateway';

type Step = 'billing' | 'provisioning';

/** Qué paso de la saga actualiza cada evento de resultado. */
const RESULT_MAP: Partial<Record<EventType, { step?: Step; status?: 'OK' | 'FAILED' }>> = {
  BillingAccountCreated: { step: 'billing', status: 'OK' },
  BillingFailed: { step: 'billing', status: 'FAILED' },
  BillingAccountCancelled: {}, // compensación: solo queda en el historial
  ProvisioningCompleted: { step: 'provisioning', status: 'OK' },
  ProvisioningFailed: { step: 'provisioning', status: 'FAILED' },
};

const OPEN: ActivationStatus[] = ['PENDING', 'IN_PROGRESS'];

@Injectable()
export class ActivationsService implements OnModuleInit {
  private readonly log = new Logger(ActivationsService.name);
  private readonly col: Collection<Document>;

  constructor(
    @Inject(MONGO_DB) db: Db,
    private readonly publisher: EventPublisher,
    private readonly gateway: ActivationsGateway,
  ) {
    this.col = db.collection('activations');
  }

  async onModuleInit() {
    await this.col.createIndex({ customerId: 1 });
  }

  async create(customerId: string, planId: string, simulateFailure: SimulateFailure) {
    const activationId = `act-${randomUUID().slice(0, 8)}`;
    const event = buildEvent<ActivationRequestedPayload>({
      eventType: 'ActivationRequested',
      correlationId: activationId,
      customerId,
      source: 'activation-api',
      payload: { planId, channel: 'web', simulateFailure },
    });

    await this.col.insertOne({
      _id: activationId as any,
      customerId,
      planId,
      status: 'PENDING',
      simulateFailure,
      steps: {},
      history: [{ eventType: event.eventType, source: event.source, at: event.occurredAt }],
      createdAt: event.occurredAt,
      updatedAt: event.occurredAt,
    });
    this.notify(activationId, customerId, planId, event.eventType, event.source, event.occurredAt, 'PENDING');

    // Fase 2: se publica directo después de guardar (riesgo documentado; el outbox llega en la fase 4).
    await this.publisher.publish(TOPICS.ACTIVATION_REQUESTED, event);
    return { activationId, status: 'PENDING' as ActivationStatus };
  }

  list() {
    return this.col.find().sort({ createdAt: -1 }).limit(20).toArray();
  }

  get(id: string) {
    return this.col.findOne({ _id: id as any });
  }

  /** Agregador de la saga: guarda el resultado de cada servicio y decide el estado final. */
  async handleResult(event: EventEnvelope<any>) {
    const map = RESULT_MAP[event.eventType];
    if (!map) return;
    const id = event.correlationId;
    const at = event.occurredAt;

    if (map.step) {
      await this.col.updateOne({ _id: id as any, status: 'PENDING' }, { $set: { status: 'IN_PROGRESS' } });
    }

    const $set: Record<string, unknown> = { updatedAt: at };
    if (map.step) {
      $set[`steps.${map.step}`] = { status: map.status, at, ...(event.payload?.reason ? { reason: event.payload.reason } : {}) };
    }
    const doc = await this.col.findOneAndUpdate(
      { _id: id as any },
      { $set, $push: { history: { eventType: event.eventType, source: event.source, at } } as any },
      { returnDocument: 'after' },
    );
    if (!doc) {
      this.log.warn(`Evento ${event.eventType} para activación desconocida ${id}`);
      return;
    }
    this.notify(id, doc.customerId, doc.planId, event.eventType, event.source, at, doc.status);

    if (!map.step) return;
    if (map.status === 'FAILED') {
      await this.finish(id, 'FAILED', { step: map.step, reason: event.payload?.reason ?? 'Error' });
    } else if (doc.steps?.billing?.status === 'OK' && doc.steps?.provisioning?.status === 'OK') {
      await this.finish(id, 'ACTIVE');
    }
  }

  /**
   * Transición final. La actualización condicionada (status abierto) la hace atómica:
   * si dos resultados llegan a la vez, solo uno publica el evento final, y los
   * resultados posteriores a un FAILED solo se registran en el historial.
   */
  private async finish(id: string, status: 'ACTIVE' | 'FAILED', failure?: { step: Step; reason: string }) {
    const eventType: EventType = status === 'ACTIVE' ? 'ActivationCompleted' : 'ActivationFailed';
    const now = new Date().toISOString();
    const doc = await this.col.findOneAndUpdate(
      { _id: id as any, status: { $in: OPEN } },
     { $set: { status, updatedAt: now }, $push: { history: { eventType, source: 'activation-api', at: now } } as any },
      { returnDocument: 'after' },
    );
    if (!doc) return;

    const event =
      status === 'ACTIVE'
        ? buildEvent<ActivationCompletedPayload>({
            eventType,
            correlationId: id,
            customerId: doc.customerId,
            source: 'activation-api',
            payload: { planId: doc.planId },
          })
        : buildEvent<ActivationFailedPayload>({
            eventType,
            correlationId: id,
            customerId: doc.customerId,
            source: 'activation-api',
            payload: { planId: doc.planId, failedStep: failure!.step, reason: failure!.reason },
          });

    this.notify(id, doc.customerId, doc.planId, eventType, 'activation-api', now, status);
    await this.publisher.publish(TOPICS.ACTIVATION_EVENTS, event);
  }

  private notify(
    activationId: string,
    customerId: string,
    planId: string,
    eventType: EventType,
    source: string,
    at: string,
    status: ActivationStatus,
  ) {
    this.gateway.broadcast({ activationId, customerId, planId, eventType, source, at, status });
  }
}
