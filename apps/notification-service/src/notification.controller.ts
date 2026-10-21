import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventConsumer } from '@poc/kafka-toolkit';
import { NotificationService } from './notification.service';

@Controller()
export class NotificationController {
  constructor(
    private readonly notifications: NotificationService,
    private readonly consumer: EventConsumer,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Payload() event: unknown, @Ctx() ctx: KafkaContext) {
    return this.consumer.handle(event, ctx, (e, session) => this.notifications.handle(e, session));
  }
}
