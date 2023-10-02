import Redis, { RedisOptions } from "ioredis";

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
}

export function RedisService(
  redisOptions: RedisOptions,
  groupName: string,
  consumerName: string
): RedisService {
  let subscriber: Redis;
  let publisher: Redis;

  async function connect() {
    subscriber = new Redis({ ...redisOptions });
    publisher = new Redis({ ...redisOptions });
  }

  async function disconnect() {
    await deleteConsumer();

    subscriber.disconnect();
    publisher.disconnect();
  }

  async function deleteConsumer() {
    try {
      const key = `${redisOptions.keyPrefix}*`;

      const keys = await subscriber.keys(key);

      if (keys.length) {
        for (const key of keys) {
          const groups: string[] = (await subscriber.xinfo(
            "GROUPS",
            key
          )) as string[];

          if (groups.length)
            await subscriber.xgroup(
              "DELCONSUMER",
              key,
              groupName,
              consumerName
            );
        }
      }
    } catch (error: any) {
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
    try {
      await subscriber.xack(streamName, groupName, ...messageIds);
    } catch (error: any) {
      console.error(`[HERMES] Error acknowledging messages: ${error.message}`);
      throw error;
    }
  }

  async function createConsumerGroup(streamName: string, groupName: string) {
    try {
      await subscriber.xgroup(
        "CREATE",
        `${redisOptions.keyPrefix}${streamName}`,
        groupName,
        "0",
        "MKSTREAM"
      );
    } catch (error: any) {
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

      if (results && results.length && results[0]) {
        const [_key, messages] = results[0];

        if (messages) return messages as unknown as string[];
      }
      return null;
    } catch (error: any) {
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

      if (results && results.length && results[0]) {
        const [_key, messages] = results[0];

        if (messages) return messages as unknown as string[];
      }
      return null;
    } catch (error: any) {
      console.error(`[HERMES] Error auto claiming messages: ${error.message}`);
      throw error;
    }
  }

  return {
    connect,
    disconnect,
    addToStream,
    ackMessages,
    autoClaimMessages,
    createConsumerGroup,
    readStreamAsConsumerGroup,
  };
}
