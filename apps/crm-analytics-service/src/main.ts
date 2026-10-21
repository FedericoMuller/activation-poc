import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions } from '@nestjs/microservices';
import { kafkaConsumerOptions } from '@poc/kafka-toolkit';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableShutdownHooks();
  app.connectMicroservice<MicroserviceOptions>(kafkaConsumerOptions());
  await app.startAllMicroservices();
  await app.listen(Number(process.env.PORT ?? 3001), '0.0.0.0');
}
bootstrap();
