import { Controller, Get } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventConsumer } from '@poc/kafka-toolkit';
import { LoyaltyService } from './loyalty.service';

/**
 * Escenario 5: un consumidor nuevo, con su propio consumer group, lee todo el historial
 * de activation.events desde el inicio sin tocar a ningún otro servicio.
 */
@Controller()
export class LoyaltyController {
  constructor(
    private readonly loyalty: LoyaltyService,
    private readonly consumer: EventConsumer,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Payload() event: unknown, @Ctx() ctx: KafkaContext) {
    return this.consumer.handle(event, ctx, (e, session) => this.loyalty.award(e, session));
  }

  @Get('points')
  points() {
    return this.loyalty.summary();
  }
}
