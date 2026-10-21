import { Module } from '@nestjs/common';
import { EventConsumer, KafkaPublisherModule, mongoProviders } from '@poc/kafka-toolkit';
import { ProvisioningController } from './provisioning.controller';
import { ProvisioningService } from './provisioning.service';

@Module({
  imports: [KafkaPublisherModule.register()],
  controllers: [ProvisioningController],
  providers: [...mongoProviders('provisioning'), EventConsumer, ProvisioningService],
})
export class AppModule {}
