import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventConsumer, sleep } from '@poc/kafka-toolkit';
import { ProvisioningService } from './provisioning.service';

@Controller()
export class ProvisioningController {
  constructor(
    private readonly provisioning: ProvisioningService,
    private readonly consumer: EventConsumer,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async onRequested(@Payload() event: unknown, @Ctx() ctx: KafkaContext) {
    await sleep(1000 + Math.random() * 2000); // retardo simulado de 1 a 3 s (fuera de la transacción)
    return this.consumer.handle(event, ctx, (e, session) => this.provisioning.handleRequested(e, session));
  }
}
