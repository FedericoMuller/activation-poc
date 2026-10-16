import { Module } from '@nestjs/common';
import { KafkaPublisherModule, mongoProvider } from '@poc/kafka-toolkit';
import { ProvisioningController } from './provisioning.controller';
import { ProvisioningService } from './provisioning.service';

@Module({
  imports: [KafkaPublisherModule.register()],
  controllers: [ProvisioningController],
  providers: [mongoProvider('provisioning'), ProvisioningService],
})
export class AppModule {}
