import { Module } from '@nestjs/common';
import { KafkaPublisherModule, mongoProvider } from '@poc/kafka-toolkit';
import { BillingController } from './billing.controller';
import { BillingService } from './billing.service';

@Module({
  imports: [KafkaPublisherModule.register()],
  controllers: [BillingController],
  providers: [mongoProvider('billing'), BillingService],
})
export class AppModule {}
