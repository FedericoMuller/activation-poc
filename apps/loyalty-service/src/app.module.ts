import { Module } from '@nestjs/common';
import { EventConsumer, KafkaPublisherModule, mongoProviders } from '@poc/kafka-toolkit';
import { LoyaltyController } from './loyalty.controller';
import { LoyaltyService } from './loyalty.service';

@Module({
  imports: [KafkaPublisherModule.register()],
  controllers: [LoyaltyController],
  providers: [...mongoProviders('loyalty'), EventConsumer, LoyaltyService],
})
export class AppModule {}
