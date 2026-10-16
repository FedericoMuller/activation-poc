import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { ActivationRequestedPayload, EventEnvelope, TOPICS } from '@poc/contracts';
import { BillingService } from './billing.service';

@Controller()
export class BillingController {
  constructor(private readonly billing: BillingService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onRequested(@Payload() event: EventEnvelope<ActivationRequestedPayload>) {
    return this.billing.handleRequested(event);
  }

  /** Compensación: solo reacciona a ActivationFailed; ignora ActivationCompleted. */
  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Payload() event: EventEnvelope<any>) {
    if (event.eventType === 'ActivationFailed') return this.billing.handleFailed(event);
  }
}
