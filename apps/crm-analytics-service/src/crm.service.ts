import { ConflictException, Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientSession, Collection, Db, Document } from 'mongodb';
import { EventEnvelope, TOPICS } from '@poc/contracts';
import { MONGO_DB, replayFromBeginning } from '@poc/kafka-toolkit';

@Injectable()
export class CrmService implements OnModuleInit {
  private readonly log = new Logger(CrmService.name);
  private readonly eventLog: Collection<Document>;
  private replaying = false;

  constructor(@Inject(MONGO_DB) db: Db) {
    this.eventLog = db.collection('event_log');
  }

  async onModuleInit() {
    await this.eventLog.createIndex({ eventId: 1 }, { unique: true });
    await this.eventLog.createIndex({ correlationId: 1 });
  }

  /** RF-10: copia de cada evento recibido (idempotente por eventId). */
  async record(e: EventEnvelope<any>, session: ClientSession) {
    await this.upsert(e, session);
  }

  private upsert(e: EventEnvelope<any>, session?: ClientSession) {
    return this.eventLog.updateOne(
      { eventId: e.eventId },
      { $setOnInsert: { ...e, receivedAt: new Date().toISOString() } },
      { upsert: true, session },
    );
  }

  list(correlationId?: string) {
    return this.eventLog
      .find(correlationId ? { correlationId } : {}, { projection: { _id: 0 } })
      .sort({ occurredAt: 1 })
      .limit(500)
      .toArray();
  }

  async stats() {
    const byType = await this.eventLog
      .aggregate([{ $group: { _id: '$eventType', count: { $sum: 1 } } }, { $sort: { _id: 1 } }])
      .toArray();
    return { total: await this.eventLog.countDocuments(), byType: Object.fromEntries(byType.map((r) => [r._id, r.count])) };
  }

  /**
   * RF-10: reprocesa el historial desde el inicio. Vacía event_log y lo reconstruye releyendo
   * los 4 topics con un grupo efímero (el grupo "crm-analytics" en vivo no se toca).
   */
  async replay() {
    if (this.replaying) throw new ConflictException('Ya hay un replay en curso');
    this.replaying = true;
    try {
      await this.eventLog.deleteMany({});
      const replayed = await replayFromBeginning(Object.values(TOPICS), (e) => this.upsert(e).then(() => undefined));
      this.log.log(`Replay terminado: ${replayed} eventos reprocesados`);
      return { replayed, ...(await this.stats()) };
    } finally {
      this.replaying = false;
    }
  }
}
