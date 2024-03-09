import { RedisService } from "./redis/redisService";
import { RedisOptions } from "ioredis";
import { Redis as UpstashRedis } from "@upstash/redis";
import { randomBytes } from "crypto";
import sleep from "../utils/sleep";
import { z } from "zod";
import { PoolOptions } from "./redis/redisPool";
import { UpstashRedisService } from "./redis/upstashRedis";

type Maybe<T> = T | null | undefined;

interface IBus {
  subscribe<T>(
    topic: string,
    callback: (msgData: { data: T; msgId: string }) => Promise<void>
  ): void | Promise<void>;
  publish<T>(topic: string, data: T): Promise<void>;
}

export interface IMsg {
  id: string;
  ack: () => Promise<void>;
}

export interface IEvent<MessagePayload> {
  subscribe(
    fn: (msgData: { data: MessagePayload; msg: IMsg }) => void | Promise<void>
  ): Promise<void>;
  publish(data: MessagePayload): Promise<void>;
}

export interface IService<RequestType, ResponseType> {
  reply(
    fn: (msgData: {
      reqData: RequestType;
      msgId: string;
    }) => ResponseType | Promise<ResponseType>
  ): void | Promise<void>;
  request(reqData: RequestType): Promise<ResponseType>;
}

export type HermesConfig = {
  durableName: string;
} & (
  | {
      persistanceType: "REDIS";
      redisOptions: RedisOptions;
      poolOptions?: PoolOptions;
    }
  | { persistanceType: "UPSTASH"; upstashRedis: UpstashRedis }
);

export interface IHermes {
  connect(): Promise<IHermes>;
  disconnect(): Promise<void>;
  registerEvent<MessagePayload>(
    topic: string,
    payloadSchema: z.Schema<MessagePayload>
  ): Promise<IEvent<MessagePayload>>;
  registerService<RequestType, ResponseType>(
    topic: string,
    requestSchema: z.Schema<RequestType>,
    responseSchema: z.Schema<ResponseType>
  ): Promise<IService<RequestType, ResponseType>>;
}

const KEY_PREFIX = "hermes:";
const DEFAULT_POOL_OPTIONS = { min: 0, max: 20 };

export function Hermes(config: HermesConfig): IHermes {
  let redisService: RedisService | UpstashRedisService;
  let isAlive = false;

  const consumerName = randomBytes(16).toString("hex");
  const groupName = config.durableName;

  async function connect() {
    if (config.persistanceType === "REDIS") {
      if (!config.poolOptions) {
        config.poolOptions = DEFAULT_POOL_OPTIONS;
      }

      redisService = RedisService(
        {
          ...config.redisOptions,
          keyPrefix: KEY_PREFIX,
          poolOptions: config.poolOptions,
        },
        groupName,
        consumerName
      );

      await redisService.connect();
    }
    if (config.persistanceType === "UPSTASH") {
      redisService = UpstashRedisService(
        config.upstashRedis,
        { keyPrefix: KEY_PREFIX },
        groupName,
        consumerName
      );
    }

    isAlive = true;

    // @ts-expect-error ignore
    return this;
  }

  async function disconnect() {
    isAlive = false;

    await sleep(100);

    if (config.persistanceType === "REDIS") {
      // @ts-expect-error ignore
      await redisService.disconnect();
    } else if (config.persistanceType === "UPSTASH") {
      // @ts-expect-error ignore
      await redisService.cleanup();
    }
  }

  async function* getStreamMessageGenerator(streamName: string, count: number) {
    let fetchNewMessages = true;
    while (isAlive) {
      const results = fetchNewMessages
        ? await redisService.readStreamAsConsumerGroup(streamName, count)
        : await redisService.autoClaimMessages(streamName, count);

      fetchNewMessages = !fetchNewMessages;

      if (!results || !results.length) {
        continue;
      }

      for (const message of results) {
        yield message;
      }
    }
  }

  async function registerEvent<MessagePayload>(
    topic: string,
    payloadSchema: z.Schema<MessagePayload>
  ): Promise<IEvent<MessagePayload>> {
    async function subscribe(
      callback: (msgData: {
        data: MessagePayload;
        msg: { id: string; ack: () => Promise<void> };
      }) => Promise<void>
    ) {
      try {
        const generator = getStreamMessageGenerator(topic, 10);

        for await (const message of generator) {
          if (message.length && message[1] && message[1][1]) {
            const data: Maybe<MessagePayload> = JSON.parse(message[1][1]);
            const msgId: string = String(message[0]);

            let parsedData: MessagePayload;
            try {
              parsedData = payloadSchema.parse(data);
            } catch (error) {
              console.error(
                `[HERMES] Message Bus: ${topic}, message parse error`
              );
              throw new Error("Message Parse Error");
            }

            await callback({
              data: parsedData,
              msg: {
                id: msgId,
                ack: () => redisService.ackMessages(topic, groupName, msgId),
              },
            });
          }
        }
      } catch (error) {
        console.error("[HERMES] Error while subscribing:", error);
        throw error;
      }
    }

    async function publish(payload: MessagePayload): Promise<void> {
      let parsedData: MessagePayload;
      try {
        parsedData = payloadSchema.parse(payload);
      } catch (error) {
        console.error(`[HERMES] Message Bus: ${topic}, message parse error`);
        throw new Error("Message Parse Error");
      }
      await redisService.addToStream(
        topic,
        "*",
        "data",
        JSON.stringify(parsedData)
      );
    }

    return { subscribe, publish };
  }

  const bus: IBus = {
    async subscribe<T>(
      topic: string,
      callback: (msgData: { data: T; msgId: string }) => Promise<void>
    ): Promise<void> {
      try {
        const generator = getStreamMessageGenerator(topic, 10);

        for await (const message of generator) {
          if (message.length && message[1] && message[1][1]) {
            const data: T = JSON.parse(message[1][1]);
            const msgId: string = String(message[0]);

            await redisService.ackMessages(topic, groupName, msgId);

            await callback({ data, msgId });
          }
        }
      } catch (error) {
        console.error("[HERMES] Error while subscribing:", error);
        throw error;
      }
    },
    async publish<T>(topic: string, data: T): Promise<void> {
      await redisService.addToStream(topic, "*", "data", JSON.stringify(data));
    },
  };

  async function registerService<RequestType, ResponseType>(
    topic: string,
    requestSchema: z.ZodTypeAny,
    responseSchema: z.ZodTypeAny
  ) {
    async function reply(
      callback: (msgData: {
        reqData: RequestType;
        msgId: string;
      }) => ResponseType | Promise<ResponseType>
    ): Promise<void> {
      await bus.subscribe<RequestType>(topic, async ({ msgId, data }) => {
        let parsedReqData: RequestType;

        try {
          parsedReqData = requestSchema.parse(data);
        } catch (error) {
          console.error(`[HERMES] Service: ${topic}, request parse error`);
          throw new Error("Request Parse Error");
        }

        const res = await callback({ reqData: parsedReqData, msgId });

        let parsedResponseData: ResponseType;

        try {
          parsedResponseData = responseSchema.parse(res);
        } catch (error) {
          console.error(`[HERMES] Service: ${topic}, reply parse error`);
          throw new Error("Reply Parse Error");
        }

        await redisService.ackMessages(topic, groupName, msgId);

        await redisService.addToStream(
          `${topic}-res`,
          "*",
          "data",
          JSON.stringify(parsedResponseData),
          "reqMsgId",
          msgId
        );
      });
    }

    async function request(reqData: RequestType): Promise<ResponseType> {
      let parsedRequestData: RequestType;

      try {
        parsedRequestData = requestSchema.parse(reqData);
      } catch (error) {
        console.error(`[HERMES] Service: ${topic}, request parse error`);
        throw new Error("Request Parse Error");
      }

      const responseTopic = `${topic}-res`;

      await bus.publish<RequestType>(topic, parsedRequestData);

      const responseGenerator = getStreamMessageGenerator(responseTopic, 1);

      const messageResp = await responseGenerator.next();

      if (!messageResp.done && messageResp.value) {
        const message = messageResp.value;

        if (message[0] && message[1] && message[1][1]) {
          const data: Maybe<ResponseType> = JSON.parse(message[1][1]);
          const msgId = message[0];

          let parsedData: ResponseType;

          try {
            parsedData = responseSchema.parse(data);
          } catch (error) {
            console.error(`[HERMES] Service: ${topic}, reply parse error`);
            throw new Error("Reply Parse Error");
          }

          await redisService.ackMessages(responseTopic, groupName, msgId);

          return parsedData;
        } else {
          console.error("[HERMES] Invalid redis payload");
          throw Error("Invalid redis payload");
        }
      } else {
        console.error("[HERMES] Unexpected Error while making a request");
        throw Error("Unexpected Error while making a request");
      }
    }

    return {
      request,
      reply,
    };
  }

  return {
    connect,
    disconnect,
    registerEvent,
    registerService,
  };
}
