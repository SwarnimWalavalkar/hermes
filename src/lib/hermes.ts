import { RedisService } from "./redisService";
import { RedisOptions } from "ioredis";
import { randomBytes } from "crypto";
import sleep from "../utils/sleep";
import { z } from "zod";

type Maybe<T> = T | null | undefined;

export interface IBus {
  subscribe<T>(
    topic: string,
    callback: (msgData: { data: T; msgId: string }) => Promise<void>
  ): void | Promise<void>;
  publish<T>(topic: string, data: T): Promise<void>;
}

export interface Event<MessagePayload> {
  subscribe(
    fn: (msgData: {
      data: MessagePayload;
      msgId: string;
    }) => void | Promise<void>
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

export interface IHermes {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  registerEvent<MessagePayload>(
    topic: string,
    payloadSchema: z.Schema<MessagePayload>
  ): Promise<Event<MessagePayload>>;
  registerService<RequestType, ResponseType>(
    topic: string,
    requestSchema: z.Schema<RequestType>,
    responseSchema: z.Schema<ResponseType>
  ): Promise<IService<RequestType, ResponseType>>;
}

const KEY_PREFIX = "hermes:";

export function Hermes({
  durableName,
  redisOptions,
}: {
  durableName: string;
  redisOptions: RedisOptions;
}): IHermes {
  let redisService: RedisService;
  let isAlive = false;

  const consumerName = randomBytes(16).toString("hex");
  const groupName = durableName;

  async function connect() {
    redisService = RedisService(
      { ...redisOptions, keyPrefix: KEY_PREFIX },
      groupName,
      consumerName
    );

    await redisService.connect();

    isAlive = true;
  }

  async function disconnect() {
    isAlive = false;

    await sleep(100);

    await redisService.disconnect();
  }

  async function* getStreamMessageGenerator(streamName: string, count: number) {
    while (isAlive) {
      const results = await redisService.readStreamAsConsumerGroup(
        streamName,
        count
      );

      if (!results || !results?.length) {
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
  ): Promise<Event<MessagePayload>> {
    async function subscribe(
      callback: (msgData: {
        data: MessagePayload;
        msgId: string;
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

            /** @TODO Remove this and require messages to be manually acknowledged */
            await redisService.ackMessages(topic, groupName, msgId);

            await callback({ data: parsedData, msgId });
          }
        }
      } catch (error) {
        console.error("[HERMES] Error while subscribing:", error);
        throw error;
      }
    }
    async function publish(reqData: MessagePayload): Promise<void> {
      let parsedData: MessagePayload;
      try {
        parsedData = payloadSchema.parse(reqData);
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

            /** @TODO Remove this and require messages to be manually acknowledged */
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
