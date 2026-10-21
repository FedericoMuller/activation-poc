import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventConsumer } from '@poc/kafka-toolkit';
import { ActivationsService } from './activations.service';

/** Consume los resultados de billing y provisioning (el "agregador" de la saga). */
@Controller()
export class ResultsConsumer {
  constructor(
    private readonly service: ActivationsService,
    private readonly consumer: EventConsumer,
  ) {}

  @EventPattern(TOPICS.BILLING_EVENTS)
  onBilling(@Payload() event: unknown, @Ctx() ctx: KafkaContext) {
    return this.consumer.handle(event, ctx, (e, session) => this.service.handleResult(e, session));
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  onProvisioning(@Payload() event: unknown, @Ctx() ctx: KafkaContext) {
    return this.consumer.handle(event, ctx, (e, session) => this.service.handleResult(e, session));
  }
}
