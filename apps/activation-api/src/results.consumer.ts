import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { EventEnvelope, TOPICS } from '@poc/contracts';
import { ActivationsService } from './activations.service';

/** Consume los resultados de billing y provisioning (el "agregador" de la saga). */
@Controller()
export class ResultsConsumer {
  constructor(private readonly service: ActivationsService) {}

  @EventPattern(TOPICS.BILLING_EVENTS)
  onBilling(@Payload() event: EventEnvelope<any>) {
    return this.service.handleResult(event);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  onProvisioning(@Payload() event: EventEnvelope<any>) {
    return this.service.handleResult(event);
  }
}
