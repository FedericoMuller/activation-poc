import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientSession, Collection, Db, Document } from 'mongodb';
import { EventEnvelope } from '@poc/contracts';
import { MONGO_DB } from '@poc/kafka-toolkit';

const POINTS: Record<string, number> = { 'PLAN BASICO': 10, 'PLAN DIGITAL': 20, 'PLAN PREMIUM': 50 };

@Injectable()
export class LoyaltyService implements OnModuleInit {
  private readonly log = new Logger(LoyaltyService.name);
  private readonly points: Collection<Document>;

  constructor(@Inject(MONGO_DB) db: Db) {
    this.points = db.collection('loyalty_points');
  }

  async onModuleInit() {
    await this.points.createIndex({ activationId: 1 }, { unique: true });
  }

  /** Suma puntos por cada activación completada. */
  async award(e: EventEnvelope<any>, session: ClientSession) {
    if (e.eventType !== 'ActivationCompleted') return;
    const points = POINTS[e.payload.planId] ?? 5;
    const res = await this.points.updateOne(
      { activationId: e.correlationId },
      { $setOnInsert: { activationId: e.correlationId, customerId: e.customerId, planId: e.payload.planId, points, at: e.occurredAt } },
      { upsert: true, session },
    );
    if (res.upsertedCount) this.log.log(`+${points} puntos para ${e.customerId} (${e.correlationId})`);
  }

  summary() {
    return this.points
      .aggregate([
        { $group: { _id: '$customerId', points: { $sum: '$points' }, activations: { $sum: 1 } } },
        { $project: { _id: 0, customerId: '$_id', points: 1, activations: 1 } },
        { $sort: { customerId: 1 } },
      ])
      .toArray();
  }
}
