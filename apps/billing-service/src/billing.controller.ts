import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventConsumer, sleep } from '@poc/kafka-toolkit';
import { BillingService } from './billing.service';

@Controller()
export class BillingController {
  constructor(
    private readonly billing: BillingService,
    private readonly consumer: EventConsumer,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async onRequested(@Payload() event: unknown, @Ctx() ctx: KafkaContext) {
    await sleep(500 + Math.random() * 500); // simula la llamada al sistema de facturación (fuera de la transacción)
    return this.consumer.handle(event, ctx, (e, session) => this.billing.handleRequested(e, session));
  }

  /** Compensación: solo actúa con ActivationFailed. El resto igual se confirma (offset) y se ignora. */
  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Payload() event: unknown, @Ctx() ctx: KafkaContext) {
    return this.consumer.handle(event, ctx, async (e, session) => {
      if (e.eventType === 'ActivationFailed') await this.billing.handleFailed(e, session);
    });
  }
}
