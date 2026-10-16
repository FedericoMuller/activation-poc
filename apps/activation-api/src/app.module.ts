import { Module } from '@nestjs/common';
import { KafkaPublisherModule, mongoProvider } from '@poc/kafka-toolkit';
import { ActivationsController } from './activations.controller';
import { ResultsConsumer } from './results.consumer';
import { ActivationsService } from './activations.service';
import { ActivationsGateway } from './activations.gateway';

@Module({
  imports: [KafkaPublisherModule.register()],
  controllers: [ActivationsController, ResultsConsumer],
  providers: [mongoProvider('activation_api'), ActivationsService, ActivationsGateway],
})
export class AppModule {}
