import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientSession, Collection, Db, Document } from 'mongodb';
import {
  ActivationRequestedPayload,
  BillingAccountCancelledPayload,
  BillingAccountCreatedPayload,
  BillingFailedPayload,
  buildEvent,
  EventEnvelope,
  EventType,
  TOPICS,
} from '@poc/contracts';
import { EventPublisher, MONGO_DB } from '@poc/kafka-toolkit';

/**
 * billing_accounts.status: CREATED | CANCELLED | FAILED | FAILURE_SEEN
 * FAILURE_SEEN = llegó ActivationFailed antes de crear la cuenta; si la cuenta
 * se crea después, se anula igual (caso borde del documento, sección 06).
 *
 * Los métodos reciben la sesión de la transacción abierta por EventConsumer
 * (misma transacción que el registro en processed_events).
 */
@Injectable()
export class BillingService implements OnModuleInit {
  private readonly log = new Logger(BillingService.name);
  private readonly accounts: Collection<Document>;

  constructor(@Inject(MONGO_DB) db: Db, private readonly publisher: EventPublisher) {
    this.accounts = db.collection('billing_accounts');
  }

  async onModuleInit() {
    await this.accounts.createIndex({ activationId: 1 }, { unique: true });
  }

  async handleRequested(e: EventEnvelope<ActivationRequestedPayload>, session: ClientSession) {
    const id = e.correlationId;
    const now = new Date().toISOString();

    if (e.payload.simulateFailure === 'billing') {
      await this.accounts.updateOne(
        { activationId: id },
        { $set: { status: 'FAILED', updatedAt: now }, $setOnInsert: { activationId: id, createdAt: now } },
        { upsert: true, session },
      );
      // Error de negocio simulado: se publica como evento, no va a la DLQ.
      await this.emit<BillingFailedPayload>('BillingFailed', e, { reason: 'Fallo de facturación simulado' });
      return;
    }

    const accountId = `BA-${id}`;
    const before = await this.accounts.findOneAndUpdate(
      { activationId: id },
      { $setOnInsert: { activationId: id, accountId, status: 'CREATED', createdAt: now } },
      { upsert: true, returnDocument: 'before', session },
    );

    if (before === null) {
      await this.emit<BillingAccountCreatedPayload>('BillingAccountCreated', e, { accountId });
    } else if (before.status === 'FAILURE_SEEN') {
      // La activación ya había fallado: se crea y se anula de inmediato.
      await this.accounts.updateOne(
        { activationId: id },
        { $set: { accountId, status: 'CANCELLED', updatedAt: now } },
        { session },
      );
      await this.emit<BillingAccountCreatedPayload>('BillingAccountCreated', e, { accountId });
      await this.emit<BillingAccountCancelledPayload>('BillingAccountCancelled', e, {
        accountId,
        reason: 'Compensación: la activación falló antes de crear la cuenta',
      });
    } else {
      this.log.warn(`Cuenta de ${id} ya existe (${before.status}); no se crea otra`);
    }
  }

  /** Compensación (RF-06): anula la cuenta si existe. */
  async handleFailed(e: EventEnvelope<any>, session: ClientSession) {
    const id = e.correlationId;
    const doc = await this.accounts.findOneAndUpdate(
      { activationId: id, status: 'CREATED' },
      { $set: { status: 'CANCELLED', updatedAt: new Date().toISOString() } },
      { returnDocument: 'after', session },
    );
    if (doc) {
      await this.emit<BillingAccountCancelledPayload>('BillingAccountCancelled', e, {
        accountId: doc.accountId,
        reason: 'Compensación: la activación falló',
      });
      return;
    }
    // No hay cuenta todavía: dejar constancia del fallo para anularla si se crea tarde.
    await this.accounts.updateOne(
      { activationId: id },
      { $setOnInsert: { activationId: id, status: 'FAILURE_SEEN', createdAt: new Date().toISOString() } },
      { upsert: true, session },
    );
  }

  private emit<T>(eventType: EventType, cause: EventEnvelope<any>, payload: T) {
    return this.publisher.publish(
      TOPICS.BILLING_EVENTS,
      buildEvent<T>({ eventType, correlationId: cause.correlationId, customerId: cause.customerId, source: 'billing', payload }),
    );
  }
}
