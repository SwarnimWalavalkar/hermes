import { RedisService } from "../redis/redisService";
import getStreamMessageGenerator from "./getStreamMessageGenerator";

export interface BusSubscribeOptions {
  redisService: RedisService;
  topic: string;
  groupName: string;
  maxRetries: number;
  isAlive: () => boolean;
}

export interface BusPublishOptions {
  redisService: RedisService;
}

export default {
  async subscribe<T>(
    callback: (msgData: { data: T; msgId: string }) => Promise<void>,
    { topic, groupName, maxRetries, redisService, isAlive }: BusSubscribeOptions
  ): Promise<void> {
    try {
      const generator = getStreamMessageGenerator({
        streamName: topic,
        count: 10,
        redisService,
        isAlive,
      });

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
  async publish<T>(
    topic: string,
    data: T,
    { redisService }: BusPublishOptions
  ): Promise<void> {
    await redisService.addToStream(topic, "*", "data", JSON.stringify(data));
  },
};
