import { Module } from '@nestjs/common';
import { EventConsumer, KafkaPublisherModule, mongoProviders } from '@poc/kafka-toolkit';
import { ActivationsController } from './activations.controller';
import { ResultsConsumer } from './results.consumer';
import { ActivationsService } from './activations.service';
import { ActivationsGateway } from './activations.gateway';

@Module({
  imports: [KafkaPublisherModule.register()],
  controllers: [ActivationsController, ResultsConsumer],
  providers: [...mongoProviders('activation_api'), EventConsumer, ActivationsService, ActivationsGateway],
})
export class AppModule {}
