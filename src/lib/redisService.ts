import Redis, { RedisOptions } from "ioredis";
import { PoolOptions, RedisPool } from "./redisPool";

export interface RedisService {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  addToStream(
    streamName: string,
    id: string,
    ...args: Array<string>
  ): Promise<void>;
  ackMessages(
    streamName: string,
    groupName: string,
    ...messageIds: Array<string>
  ): Promise<void>;
  createConsumerGroup(streamName: string, groupName: string): Promise<void>;
  readStreamAsConsumerGroup(
    streamName: string,
    count?: number,
    blockMs?: number,
    group?: string,
    consumer?: string
  ): Promise<Array<string> | null>;
  autoClaimMessages(
    streamName: string,
    count?: number,
    group?: string,
    consumer?: string,
    minIdleTime?: number
  ): Promise<Array<string> | null>;
  scheduleMessage<T>(
    topic: string,
    msgData: T,
    timestamp: number
  ): Promise<void>;
  getNextScheduledMessage<T>(
    topic: string
  ): Promise<{ topic: string; msgData: T } | null>;
}

export function RedisService(
  options: RedisOptions & { poolOptions: PoolOptions },
  groupName: string,
  consumerName: string
): RedisService {
  let redisPool: RedisPool;

  let publisher: Redis;

  async function connect() {
    const { poolOptions, ...redisOptions } = options;

    redisPool = new RedisPool(redisOptions, poolOptions);

    publisher = await redisPool.getConnection();
  }

  async function disconnect() {
    await deleteConsumer();

    await redisPool.release(publisher);

    await redisPool.end();
  }

  async function deleteConsumer() {
    const subscriber = await redisPool.getConnection();
    try {
      const key = `${options.keyPrefix}*`;

      const keys = (await subscriber.keys(key)).filter(
        (k) => !k.endsWith("-scheduled")
      );

      if (keys.length) {
        for (const key of keys) {
          const groupsRes: string[][] = (await subscriber.xinfo(
            "GROUPS",
            key
          )) as string[][];

          const groups = groupsRes.map(
            (grp) =>
              new Map(
                grp.reduce(
                  (acc, val, i) =>
                    (i % 2
                      ? // @ts-expect-error ignore
                        acc[acc.length - 1].push(val)
                      : // @ts-expect-error ignore
                        acc.push([val])) && acc,
                  []
                )
              )
          );

          const groupNames = groups.map((grp) => grp.get("name"));

          if (groupsRes.length && groupNames.includes(groupName))
            await subscriber.xgroup(
              "DELCONSUMER",
              key,
              groupName,
              consumerName
            );
        }
      }

      await redisPool.release(subscriber);
    } catch (error: any) {
      await redisPool.release(subscriber);

      console.error("[HERMES] Error deleting consumer");
      throw error;
    }
  }

  async function addToStream(
    streamName: string,
    id: string,
    ...args: Array<string>
  ) {
    try {
      await publisher.xadd(streamName, id, ...args);
    } catch (error: any) {
      console.error(`[HERMES] Error adding to stream: ${error.message}`);
      throw error;
    }
  }

  async function ackMessages(
    streamName: string,
    groupName: string,
    ...messageIds: Array<string>
  ) {
    const subscriber = await redisPool.getConnection();
    try {
      await subscriber.xack(streamName, groupName, ...messageIds);
      // await subscriber.xdel(streamName, groupName, ...messageIds);

      await redisPool.release(subscriber);
    } catch (error: any) {
      await redisPool.release(subscriber);

      console.error(`[HERMES] Error acknowledging messages: ${error.message}`);
      throw error;
    }
  }

  async function createConsumerGroup(streamName: string, groupName: string) {
    const subscriber = await redisPool.getConnection();
    try {
      await subscriber.xgroup(
        "CREATE",
        `${options.keyPrefix}${streamName}`,
        groupName,
        "0",
        "MKSTREAM"
      );

      await redisPool.release(subscriber);
    } catch (error: any) {
      await redisPool.release(subscriber);

      if (error.message.includes("BUSYGROUP")) {
        return;
      }
      console.error(
        `[HERMES] Error while creating consumer group: ${error.message}`
      );
      throw error;
    }
  }

  async function readStreamAsConsumerGroup(
    streamName: string,
    count: number = 1,
    blockMs: number = 1,
    group: string = groupName,
    consumer: string = consumerName
  ): Promise<Array<string> | null> {
    const subscriber = await redisPool.getConnection();
    try {
      const results: string[][] = (await subscriber.xreadgroup(
        "GROUP",
        group,
        consumer,
        "COUNT",
        count,
        "BLOCK",
        blockMs,
        "STREAMS",
        streamName,
        ">"
      )) as string[][];

      await redisPool.release(subscriber);

      if (results && results.length && results[0]) {
        const [_key, messages] = results[0];

        if (messages) return messages as unknown as string[];
      }
      return null;
    } catch (error: any) {
      await redisPool.release(subscriber);

      if (error.message.includes("NOGROUP")) {
        console.log(`${error.message} ...CREATING GROUP`);
        await createConsumerGroup(streamName, group);
        return null;
      }
      console.error(`[HERMES] Error reading stream: ${error.message}`);
      throw error;
    }
  }

  async function autoClaimMessages(
    streamName: string,
    count: number = 1,
    group: string = groupName,
    consumer: string = consumerName,
    minIdleTime: number = 5000
  ): Promise<Array<string> | null> {
    const subscriber = await redisPool.getConnection();
    try {
      const results: string[][] = (await subscriber.xautoclaim(
        streamName,
        group,
        consumer,
        minIdleTime,
        "0-0",
        "COUNT",
        count
      )) as string[][];

      await redisPool.release(subscriber);

      const [_, messages] = results;

      if (messages && messages.length) {
        return messages;
      }
      return null;
    } catch (error: any) {
      await redisPool.release(subscriber);

      console.error(`[HERMES] Error auto claiming messages: ${error.message}`);
      throw error;
    }
  }

  async function scheduleMessage<T>(
    topic: string,
    msgData: T,
    timestamp: number
  ): Promise<void> {
    try {
      await publisher.zadd(
        `${topic}-scheduled`,
        timestamp,
        JSON.stringify(msgData)
      );
    } catch (error: any) {
      console.error(`[HERMES] Error scheduling message: ${error.message}`);
      throw error;
    }
  }

  async function getNextScheduledMessage<T>(
    topic: string
  ): Promise<{ topic: string; msgData: T } | null> {
    const subscriber = await redisPool.getConnection();
    try {
      const results: string[] = (await subscriber.zrange(
        `${topic}-scheduled`,
        0,
        new Date().getTime(),
        "BYSCORE",
        "LIMIT",
        0,
        1
      )) as string[];

      await redisPool.release(subscriber);

      if (results && results.length && results[0]) {
        const msgData = JSON.parse(results[0]);

        await subscriber.zrem(`${topic}-scheduled`, results[0]);

        return { topic, msgData };
      }
      return null;
    } catch (error: any) {
      await redisPool.release(subscriber);

      console.error(
        `[HERMES] Error getting next scheduled message: ${error.message}`
      );
      throw error;
    }
  }

  return {
    connect,
    disconnect,
    addToStream,
    ackMessages,
    scheduleMessage,
    autoClaimMessages,
    createConsumerGroup,
    getNextScheduledMessage,
    readStreamAsConsumerGroup,
  };
}
