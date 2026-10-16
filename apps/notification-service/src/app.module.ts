import { Module } from '@nestjs/common';
import { mongoProvider } from '@poc/kafka-toolkit';
import { NotificationController } from './notification.controller';
import { NotificationService } from './notification.service';

@Module({
  controllers: [NotificationController],
  providers: [mongoProvider('notification'), NotificationService],
})
export class AppModule {}
