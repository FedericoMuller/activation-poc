import { Module } from '@nestjs/common';
import { EventConsumer, KafkaPublisherModule, mongoProviders } from '@poc/kafka-toolkit';
import { NotificationController } from './notification.controller';
import { NotificationService } from './notification.service';

@Module({
  imports: [KafkaPublisherModule.register()],
  controllers: [NotificationController],
  providers: [...mongoProviders('notification'), EventConsumer, NotificationService],
})
export class AppModule {}
