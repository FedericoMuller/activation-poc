import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions } from '@nestjs/microservices';
import { kafkaConsumerOptions } from '@poc/kafka-toolkit';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, kafkaConsumerOptions());
  // Al detener el contenedor el consumidor sale del grupo y no demora el rebalanceo.
  app.enableShutdownHooks();
  await app.listen();
}
bootstrap();
