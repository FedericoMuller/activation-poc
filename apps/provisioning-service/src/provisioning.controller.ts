import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { ActivationRequestedPayload, EventEnvelope, TOPICS } from '@poc/contracts';
import { ProvisioningService } from './provisioning.service';

@Controller()
export class ProvisioningController {
  constructor(private readonly provisioning: ProvisioningService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onRequested(@Payload() event: EventEnvelope<ActivationRequestedPayload>) {
    return this.provisioning.handleRequested(event);
  }
}
