import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { EventEnvelope, TOPICS } from '@poc/contracts';
import { NotificationService } from './notification.service';

@Controller()
export class NotificationController {
  constructor(private readonly notifications: NotificationService) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Payload() event: EventEnvelope<any>) {
    return this.notifications.handle(event);
  }
}
