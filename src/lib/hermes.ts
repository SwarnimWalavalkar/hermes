import { RedisService } from "./redis/redisService";
import { PoolOptions } from "./redis/redisPool";
import { IEvent, IHermes, IMsg } from "./types";
import { RedisOptions } from "ioredis";
import { randomBytes } from "crypto";
import sleep from "../utils/sleep";
import { z } from "zod";

import handleSubscription from "./events/handleSubscription";
import handleRequest from "./services/handleRequest";
import handlePublish from "./events/handlePublish";
import handleReply from "./services/handleReply";

const KEY_PREFIX = "hermes:";
const DEFAULT_MAX_RETRIES = 3;

export function Hermes({
  durableName,
  redisOptions,
  poolOptions,
}: {
  durableName: string;
  redisOptions: RedisOptions;
  poolOptions: PoolOptions;
}): IHermes {
  let redisService: RedisService;
  let isAlive = false;

  const consumerName = randomBytes(16).toString("hex");
  const groupName = durableName;

  async function connect() {
    redisService = RedisService(
      {
        ...redisOptions,
        keyPrefix: KEY_PREFIX,
        poolOptions,
      },
      groupName,
      consumerName
    );

    await redisService.connect();

    isAlive = true;

    // @ts-expect-error ignore
    return this;
  }

  async function disconnect() {
    isAlive = false;

    await sleep(100);

    await redisService.disconnect();
  }

  async function registerEvent<MessagePayload>(
    topic: string,
    payloadSchema: z.Schema<MessagePayload>,
    { maxRetries = DEFAULT_MAX_RETRIES }: { maxRetries?: number } = {}
  ): Promise<IEvent<MessagePayload>> {
    async function subscribe(
      callback: (msgData: { data: MessagePayload; msg: IMsg }) => Promise<void>
    ) {
      await handleSubscription<MessagePayload>(callback, {
        topic,
        groupName,
        maxRetries,
        payloadSchema,
        redisService,
        isAlive: () => isAlive,
      });
    }

    async function publish(
      payload: MessagePayload,
      { maxRetries: taskMaxRetries = maxRetries }: { maxRetries?: number } = {}
    ): Promise<void> {
      await handlePublish<MessagePayload>(payloadSchema, payload, {
        topic,
        taskMaxRetries,
        redisService,
      });
    }

    return { subscribe, publish };
  }

  async function registerService<RequestType, ResponseType>(
    topic: string,
    requestSchema: z.Schema<RequestType>,
    responseSchema: z.Schema<ResponseType>
  ) {
    async function reply(
      callback: (msgData: {
        reqData: RequestType;
        msgId: string;
      }) => ResponseType | Promise<ResponseType>
    ): Promise<void> {
      await handleReply<RequestType, ResponseType>(callback, {
        requestSchema,
        responseSchema,
        topic,
        redisService,
        groupName,
        isAlive: () => isAlive,
      });
    }

    async function request(reqData: RequestType): Promise<ResponseType> {
      return await handleRequest({
        reqData,
        requestSchema,
        responseSchema,
        redisService,
        topic,
        groupName,
        isAlive: () => isAlive,
      });
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
