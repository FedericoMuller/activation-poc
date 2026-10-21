import { Module } from '@nestjs/common';
import { EventConsumer, KafkaPublisherModule, mongoProviders } from '@poc/kafka-toolkit';
import { BillingController } from './billing.controller';
import { BillingService } from './billing.service';

@Module({
  imports: [KafkaPublisherModule.register()],
  controllers: [BillingController],
  providers: [...mongoProviders('billing'), EventConsumer, BillingService],
})
export class AppModule {}
