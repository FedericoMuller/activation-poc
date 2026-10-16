/**
 * kafka-toolkit: único lugar que conoce el cliente Kafka (kafkajs vía
 * @nestjs/microservices) y la conexión a Mongo. Si se reemplaza kafkajs por
 * el cliente de Confluent, solo cambia este archivo.
 *
 * Fase 2: publicación directa. Idempotencia, reintentos/DLQ y outbox van en
 * las fases 3 y 4.
 */
import { DynamicModule, Inject, Injectable, Module, Provider } from '@nestjs/common';
import { ClientKafka, ClientsModule, KafkaOptions, Transport } from '@nestjs/microservices';
import { Db, MongoClient } from 'mongodb';
import { lastValueFrom } from 'rxjs';
import { EventEnvelope } from '@poc/contracts';

export const KAFKA_CLIENT = 'KAFKA_CLIENT';
export const MONGO_DB = 'MONGO_DB';

const brokers = () => (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const clientId = () => process.env.KAFKA_CLIENT_ID ?? 'svc';

/** Opciones del consumidor (microservicio híbrido). Los grupos nuevos leen desde el inicio. */
export function kafkaConsumerOptions(): KafkaOptions {
  return {
    transport: Transport.KAFKA,
    options: {
      client: { clientId: clientId(), brokers: brokers(), retry: { retries: 10 } },
      consumer: { groupId: process.env.KAFKA_GROUP_ID ?? clientId(), allowAutoTopicCreation: false },
      subscribe: { fromBeginning: true },
    },
  };
}

@Injectable()
export class EventPublisher {
  constructor(@Inject(KAFKA_CLIENT) private readonly client: ClientKafka) {}

  /** Publica con customerId como clave de partición (RNF-02). */
  async publish(topic: string, event: EventEnvelope<any>): Promise<void> {
    await lastValueFrom(
      this.client.emit(topic, { key: event.customerId, value: event }),
      { defaultValue: undefined },
    );
  }
}

@Module({})
export class KafkaPublisherModule {
  static register(): DynamicModule {
    return {
      module: KafkaPublisherModule,
      imports: [
        ClientsModule.register([
          {
            name: KAFKA_CLIENT,
            transport: Transport.KAFKA,
            options: {
              client: { clientId: `${clientId()}-producer`, brokers: brokers(), retry: { retries: 10 } },
              producerOnlyMode: true,
            },
          },
        ]),
      ],
      providers: [EventPublisher],
      exports: [EventPublisher],
    };
  }
}

/** Una base de Mongo por servicio (nadie lee la base de otro). */
export function mongoProvider(dbName: string): Provider {
  return {
    provide: MONGO_DB,
    useFactory: async (): Promise<Db> => {
      const client = new MongoClient(process.env.MONGO_URI ?? 'mongodb://localhost:27017/?replicaSet=rs0');
      await client.connect();
      return client.db(dbName);
    },
  };
}

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
export const isDuplicateKey = (e: any) => e?.code === 11000;
