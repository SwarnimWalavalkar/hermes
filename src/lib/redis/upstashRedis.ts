import { Redis as UpstashRedis } from "@upstash/redis";

export interface UpstashRedisService {
  cleanup(): Promise<void>;
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

export function UpstashRedisService(
  redis: UpstashRedis,
  options: { keyPrefix: string },
  groupName: string,
  consumerName: string
): UpstashRedisService {
  async function cleanup() {
    try {
      const key = `${options.keyPrefix}*`;

      const keys = await redis.keys(key);

      if (keys.length) {
        for (const key of keys) {
          const groupsRes: string[][] = (await redis.xinfo(key, {
            type: "GROUPS",
          })) as string[][];

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
            await redis.xgroup(key, {
              type: "DELCONSUMER",
              group: groupName,
              consumer: consumerName,
            });
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
      const map = args.reduce(
        (acc: Record<string, string>, str: string, i: number) => {
          if (i % 2 === 0) {
            const value = args[i + 1];
            if (str && value) {
              acc[str] = value;
            }
          }
          return acc;
        },
        {}
      );

      await redis.xadd(streamName, id, map);
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
      await redis.xack(streamName, groupName, [...messageIds]);
    } catch (error: any) {
      console.error(`[HERMES] Error acknowledging messages: ${error.message}`);
      throw error;
    }
  }

  async function autoClaimMessages(
    streamName: string,
    count: number = 1,
    group: string = groupName,
    consumer: string = consumerName,
    minIdleTime: number = 50000
  ): Promise<Array<string> | null> {
    try {
      const results = (await redis.xautoclaim(
        streamName,
        group,
        consumer,
        minIdleTime,
        "0-0",
        { count }
      )) as string[][];

      const firstMessage = results?.[1]?.[0];
      const streamId = firstMessage?.[0];
      const messagesRes = firstMessage?.[1] as unknown as string[][];

      if (!streamId || !messagesRes.length) {
        return null;
      }

      const messages = messagesRes.map((msg: string[]) => msg[1] as string);

      return messages;
    } catch (error: any) {
      console.error(`[HERMES] Error auto claiming messages: ${error.message}`);
      throw error;
    }
  }

  async function createConsumerGroup(streamName: string, groupName: string) {
    try {
      await redis.xgroup(streamName, {
        type: "CREATE",
        group: groupName,
        id: "0",
        options: { MKSTREAM: true },
      });
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
    group: string = groupName,
    consumer: string = consumerName
  ): Promise<Array<string> | null> {
    try {
      const results = (await redis.xreadgroup(
        group,
        consumer,
        [streamName],
        [">"],
        {
          count: count,
        }
      )) as string[][];

      const firstMessage = results?.[1]?.[0];
      const streamId = firstMessage?.[0];
      const messagesRes = firstMessage?.[1] as unknown as string[][];

      if (!streamId || !messagesRes.length) {
        return null;
      }

      const messages = messagesRes.map((msg: string[]) => msg[1] as string);

      return messages;
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

  return {
    cleanup,
    addToStream,
    ackMessages,
    autoClaimMessages,
    createConsumerGroup,
    readStreamAsConsumerGroup,
  };
}
