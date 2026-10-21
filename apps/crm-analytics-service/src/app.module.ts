import { Module } from '@nestjs/common';
import { EventConsumer, KafkaPublisherModule, mongoProviders } from '@poc/kafka-toolkit';
import { CrmController } from './crm.controller';
import { CrmConsumer } from './crm.consumer';
import { CrmService } from './crm.service';

@Module({
  imports: [KafkaPublisherModule.register()],
  controllers: [CrmConsumer, CrmController],
  providers: [...mongoProviders('crm_analytics'), EventConsumer, CrmService],
})
export class AppModule {}
