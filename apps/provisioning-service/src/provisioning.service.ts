import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Collection, Db, Document } from 'mongodb';
import {
  ActivationRequestedPayload,
  buildEvent,
  EventEnvelope,
  EventType,
  ProvisioningCompletedPayload,
  ProvisioningFailedPayload,
  TOPICS,
} from '@poc/contracts';
import { EventPublisher, isDuplicateKey, MONGO_DB, sleep } from '@poc/kafka-toolkit';

@Injectable()
export class ProvisioningService implements OnModuleInit {
  private readonly log = new Logger(ProvisioningService.name);
  private readonly orders: Collection<Document>;

  constructor(@Inject(MONGO_DB) db: Db, private readonly publisher: EventPublisher) {
    this.orders = db.collection('provisioning_orders');
  }

  async onModuleInit() {
    await this.orders.createIndex({ activationId: 1 }, { unique: true });
  }

  async handleRequested(e: EventEnvelope<ActivationRequestedPayload>) {
    const id = e.correlationId;
    await sleep(1000 + Math.random() * 2000); // retardo simulado de 1 a 3 s
    const now = new Date().toISOString();
    const failing = e.payload.simulateFailure === 'provisioning';
    const orderId = `PO-${id}`;

    let before: Document | null;
    try {
      before = await this.orders.findOneAndUpdate(
        { activationId: id },
        { $setOnInsert: { activationId: id, orderId, status: failing ? 'FAILED' : 'COMPLETED', createdAt: now } },
        { upsert: true, returnDocument: 'before' },
      );
    } catch (err) {
      if (isDuplicateKey(err)) return;
      throw err;
    }
    if (before) {
      this.log.warn(`Orden de ${id} ya procesada; se ignora el duplicado`);
      return;
    }

    if (failing) {
      // Error de negocio simulado: se publica como evento, no va a la DLQ.
      await this.emit<ProvisioningFailedPayload>('ProvisioningFailed', e, { reason: 'Fallo de aprovisionamiento simulado' });
    } else {
      await this.emit<ProvisioningCompletedPayload>('ProvisioningCompleted', e, { orderId });
    }
  }

  private emit<T>(eventType: EventType, cause: EventEnvelope<any>, payload: T) {
    return this.publisher.publish(
      TOPICS.PROVISIONING_EVENTS,
      buildEvent<T>({ eventType, correlationId: cause.correlationId, customerId: cause.customerId, source: 'provisioning', payload }),
    );
  }
}
