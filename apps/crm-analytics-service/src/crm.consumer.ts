import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventConsumer } from '@poc/kafka-toolkit';
import { CrmService } from './crm.service';

/** Consume todos los topics y no publica nada: se suma sin tocar a los demás servicios. */
@Controller()
export class CrmConsumer {
  constructor(
    private readonly crm: CrmService,
    private readonly consumer: EventConsumer,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onRequested(@Payload() event: unknown, @Ctx() ctx: KafkaContext) {
    return this.ingest(event, ctx);
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  onBilling(@Payload() event: unknown, @Ctx() ctx: KafkaContext) {
    return this.ingest(event, ctx);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  onProvisioning(@Payload() event: unknown, @Ctx() ctx: KafkaContext) {
    return this.ingest(event, ctx);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivation(@Payload() event: unknown, @Ctx() ctx: KafkaContext) {
    return this.ingest(event, ctx);
  }

  private ingest(event: unknown, ctx: KafkaContext) {
    return this.consumer.handle(event, ctx, (e, session) => this.crm.record(e, session));
  }
}
