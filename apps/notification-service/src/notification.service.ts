import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientSession, Collection, Db, Document } from 'mongodb';
import * as nodemailer from 'nodemailer';
import { EventEnvelope } from '@poc/contracts';
import { MONGO_DB } from '@poc/kafka-toolkit';

@Injectable()
export class NotificationService implements OnModuleInit {
  private readonly log = new Logger(NotificationService.name);
  private readonly notifications: Collection<Document>;
  private readonly transport = nodemailer.createTransport({
    host: process.env.SMTP_HOST ?? 'localhost',
    port: Number(process.env.SMTP_PORT ?? 1025),
    secure: false,
  });

  constructor(@Inject(MONGO_DB) db: Db) {
    this.notifications = db.collection('notifications');
  }

  async onModuleInit() {
    await this.notifications.createIndex({ activationId: 1, eventType: 1 }, { unique: true });
  }

  /**
   * RF-07: un email por cada ActivationCompleted o ActivationFailed.
   * Si el envío falla, la excepción aborta la transacción (no queda registro) y el
   * EventConsumer reintenta; tras los reintentos el mensaje va a la DLQ.
   */
  async handle(e: EventEnvelope<any>, session: ClientSession) {
    if (e.eventType !== 'ActivationCompleted' && e.eventType !== 'ActivationFailed') return;

    // Segunda barrera contra duplicados: un mismo resultado de activación nunca manda dos emails.
    const already = await this.notifications.findOne(
      { activationId: e.correlationId, eventType: e.eventType },
      { session },
    );
    if (already) {
      this.log.warn(`Email de ${e.eventType} para ${e.correlationId} ya enviado; se ignora`);
      return;
    }

    const ok = e.eventType === 'ActivationCompleted';
    const to = `${e.customerId.toLowerCase()}@example.com`;
    const subject = ok ? `¡Tu plan ${e.payload.planId} está activo!` : `No pudimos activar tu plan ${e.payload.planId}`;
    const text = ok
      ? `Hola ${e.customerId},\n\nTu plan ${e.payload.planId} ya está activo.\nReferencia: ${e.correlationId}\n`
      : `Hola ${e.customerId},\n\nNo pudimos activar tu plan ${e.payload.planId}.\nMotivo: ${e.payload.reason} (paso: ${e.payload.failedStep}).\nNo se te cobrará. Referencia: ${e.correlationId}\n`;

    await this.notifications.insertOne(
      { activationId: e.correlationId, eventType: e.eventType, to, subject, sentAt: new Date().toISOString() },
      { session },
    );
    await this.transport.sendMail({ from: 'activaciones@poc.local', to, subject, text });
    this.log.log(`Email enviado (${e.eventType}) a ${to}`);
  }
}
