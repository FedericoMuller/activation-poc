/**
 * kafka-toolkit: único lugar que conoce el cliente Kafka (kafkajs vía
 * @nestjs/microservices) y la conexión a Mongo. Si se reemplaza kafkajs por
 * el cliente de Confluent, solo cambia este archivo.
 *
 * Fase 3: EventConsumer.handle() aplica, para cada mensaje y en este orden:
 *   1. validación del sobre de eventos,
 *   2. idempotencia (processed_events) en la MISMA transacción de Mongo que el efecto,
 *   3. reintentos con espera creciente (1 s, 2 s, 4 s),
 *   4. envío a <topic>.dlq si sigue fallando (la partición no se bloquea),
 *   5. commit manual del offset, siempre DESPUÉS de procesar.
 */
import { DynamicModule, Inject, Injectable, Logger, Module, OnModuleInit, Provider } from '@nestjs/common';
import { ClientKafka, ClientsModule, KafkaContext, KafkaOptions, Transport } from '@nestjs/microservices';
import { Kafka } from 'kafkajs';
import { ClientSession, Collection, Db, Document, MongoClient } from 'mongodb';
import { lastValueFrom } from 'rxjs';
import { CONTRACT_VERSION, dlqOf, EventEnvelope } from '@poc/contracts';

export const KAFKA_CLIENT = 'KAFKA_CLIENT';
export const MONGO_CLIENT = 'MONGO_CLIENT';
export const MONGO_DB = 'MONGO_DB';

/** Reintentos tras el primer intento (esperas: 1 s, 2 s, 4 s). Configurable por entorno. */
export const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? 3);
export const RETRY_BASE_MS = Number(process.env.RETRY_BASE_MS ?? 1000);
/** El TTL de processed_events coincide con la retención de los topics (7 días). */
export const PROCESSED_TTL_SECONDS = 7 * 24 * 60 * 60;

const brokers = () => (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const clientId = () => process.env.KAFKA_CLIENT_ID ?? 'svc';
const groupId = () => process.env.KAFKA_GROUP_ID ?? clientId();

/**
 * Opciones del consumidor (microservicio híbrido).
 * - Los grupos nuevos leen desde el inicio (auto.offset.reset = earliest).
 * - autoCommit desactivado: el offset se confirma a mano, después de procesar.
 */
export function kafkaConsumerOptions(): KafkaOptions {
  return {
    transport: Transport.KAFKA,
    options: {
      postfixId: '', // evita que Nest agregue "-server" al groupId y al clientId
      client: { clientId: clientId(), brokers: brokers(), retry: { retries: 10 } },
      consumer: { groupId: groupId(), allowAutoTopicCreation: false },
      subscribe: { fromBeginning: true },
      run: { autoCommit: false },
    },
  };
}

@Injectable()
export class EventPublisher {
  constructor(@Inject(KAFKA_CLIENT) private readonly client: ClientKafka) {}

  /** Publica con customerId como clave de partición (RNF-02). */
  publish(topic: string, event: EventEnvelope<any>, headers?: Record<string, string>): Promise<void> {
    return this.send(topic, event.customerId, event, headers);
  }

  /** Publicación de bajo nivel (la usa también la DLQ, donde el mensaje puede ser inválido). */
  async send(topic: string, key: string | undefined, value: unknown, headers?: Record<string, string>): Promise<void> {
    await lastValueFrom(this.client.emit(topic, { key, value, headers }), { defaultValue: undefined });
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

/** Una base de Mongo por servicio (nadie lee la base de otro). Expone también el cliente, para sesiones/transacciones. */
export function mongoProviders(dbName: string): Provider[] {
  return [
    {
      provide: MONGO_CLIENT,
      useFactory: async (): Promise<MongoClient> => {
        const client = new MongoClient(process.env.MONGO_URI ?? 'mongodb://localhost:27017/?replicaSet=rs0');
        await client.connect();
        return client;
      },
    },
    { provide: MONGO_DB, inject: [MONGO_CLIENT], useFactory: (client: MongoClient): Db => client.db(dbName) },
  ];
}

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Efecto de negocio de un consumidor. Debe usar `session` en TODAS sus operaciones de Mongo. */
export type Effect = (event: EventEnvelope<any>, session: ClientSession) => Promise<void>;

export function validateEnvelope(e: any): asserts e is EventEnvelope<any> {
  const ok =
    e &&
    typeof e === 'object' &&
    typeof e.eventId === 'string' &&
    typeof e.eventType === 'string' &&
    typeof e.correlationId === 'string' &&
    typeof e.customerId === 'string' &&
    e.payload !== undefined;
  if (!ok) throw new Error('Mensaje inválido: no cumple el sobre de eventos');
  if (typeof e.version === 'number' && e.version > CONTRACT_VERSION) {
    throw new Error(`Versión de contrato no soportada: ${e.version} (máxima ${CONTRACT_VERSION})`);
  }
}

@Injectable()
export class EventConsumer implements OnModuleInit {
  private readonly log = new Logger('EventConsumer');
  private readonly processed: Collection<Document>;

  constructor(
    @Inject(MONGO_CLIENT) private readonly client: MongoClient,
    @Inject(MONGO_DB) db: Db,
    private readonly publisher: EventPublisher,
  ) {
    this.processed = db.collection('processed_events');
  }

  async onModuleInit() {
    await this.processed.createIndex({ eventId: 1 }, { unique: true });
    await this.processed.createIndex({ processedAt: 1 }, { expireAfterSeconds: PROCESSED_TTL_SECONDS });
  }

  async handle(event: unknown, ctx: KafkaContext, effect: Effect): Promise<void> {
    const topic = ctx.getTopic();
    let ok = false;
    let failure: unknown;

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      try {
        validateEnvelope(event);
        await this.processOnce(event, effect);
        ok = true;
        break;
      } catch (err) {
        failure = err;
        if (attempt < MAX_RETRIES) {
          const wait = RETRY_BASE_MS * 2 ** attempt;
          this.log.warn(`[${topic}] falló (${this.msg(err)}). Reintento ${attempt + 1}/${MAX_RETRIES} en ${wait} ms`);
          await sleep(wait);
        }
      }
    }

    // Si falla el envío a la DLQ se propaga el error: no se confirma el offset y no se pierde el mensaje.
    if (!ok) await this.toDlq(topic, event, ctx, failure);
    await this.commit(ctx);
  }

  /** Idempotencia + efecto en una sola transacción: si el efecto falla, el eventId no queda registrado. */
  private async processOnce(event: EventEnvelope<any>, effect: Effect): Promise<void> {
    const session = this.client.startSession();
    try {
      await session.withTransaction(async () => {
        const seen = await this.processed.findOne({ eventId: event.eventId }, { session });
        if (seen) {
          this.log.warn(`Evento ${event.eventId} (${event.eventType}) ya procesado; se descarta`);
          return;
        }
        await this.processed.insertOne(
          { eventId: event.eventId, eventType: event.eventType, processedAt: new Date() },
          { session },
        );
        await effect(event, session);
      });
    } finally {
      await session.endSession();
    }
  }

  private async toDlq(topic: string, event: unknown, ctx: KafkaContext, err: unknown) {
    const headers: Record<string, string> = {
      'x-error': this.msg(err).slice(0, 500),
      'x-original-topic': topic,
      'x-original-partition': String(ctx.getPartition()),
      'x-original-offset': String(ctx.getMessage().offset),
      'x-attempts': String(MAX_RETRIES + 1),
      'x-failed-consumer': groupId(),
    };
    const key = (event as any)?.customerId;
    await this.publisher.send(dlqOf(topic), typeof key === 'string' ? key : undefined, event, headers);
    this.log.error(`[${topic}] mensaje enviado a ${dlqOf(topic)} tras ${MAX_RETRIES + 1} intentos: ${headers['x-error']}`);
  }

  /** Commit manual: confirma offset + 1 de este mensaje, nunca antes de procesarlo. */
  private async commit(ctx: KafkaContext) {
    await ctx.getConsumer().commitOffsets([
      {
        topic: ctx.getTopic(),
        partition: ctx.getPartition(),
        offset: (BigInt(ctx.getMessage().offset) + 1n).toString(),
      },
    ]);
  }

  private msg(err: unknown) {
    return (err as Error)?.message ?? String(err);
  }
}

/**
 * Relee TODOS los topics desde el principio con un grupo efímero y entrega cada evento a `onEvent`.
 * Termina cuando alcanza el último offset que existía al empezar. Devuelve cuántos eventos entregó.
 */
export async function replayFromBeginning(topics: string[], onEvent: (event: any) => Promise<void>): Promise<number> {
  const kafka = new Kafka({ clientId: `${clientId()}-replay`, brokers: brokers() });
  const admin = kafka.admin();
  await admin.connect();

  const lastOffset = new Map<string, bigint>(); // "topic:partition" -> último offset a leer
  for (const topic of topics) {
    for (const p of await admin.fetchTopicOffsets(topic)) {
      if (BigInt(p.high) > BigInt(p.low)) lastOffset.set(`${topic}:${p.partition}`, BigInt(p.high) - 1n);
    }
  }
  if (lastOffset.size === 0) {
    await admin.disconnect();
    return 0;
  }

  const replayGroup = `${groupId()}-replay-${Date.now()}`;
  const consumer = kafka.consumer({ groupId: replayGroup });
  await consumer.connect();
  for (const topic of topics) await consumer.subscribe({ topic, fromBeginning: true });

  let delivered = 0;
  try {
    await new Promise<void>((resolve, reject) => {
      consumer
        .run({
          eachMessage: async ({ topic, partition, message }) => {
            try {
              const raw = message.value?.toString();
              let parsed: any;
              try {
                parsed = raw ? JSON.parse(raw) : undefined;
              } catch {
                parsed = undefined; // mensaje inválido: se omite
              }
              if (parsed?.eventId) {
                await onEvent(parsed);
                delivered++;
              }
              const key = `${topic}:${partition}`;
              const last = lastOffset.get(key);
              if (last !== undefined && BigInt(message.offset) >= last) lastOffset.delete(key);
              if (lastOffset.size === 0) resolve();
            } catch (err) {
              reject(err);
            }
          },
        })
        .catch(reject);
    });
  } finally {
    await consumer.disconnect();
    try {
      await admin.deleteGroups([replayGroup]);
    } catch {
      /* el grupo efímero se limpia solo si falla */
    }
    await admin.disconnect();
  }
  return delivered;
}
