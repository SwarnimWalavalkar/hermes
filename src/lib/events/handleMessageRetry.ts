import { RedisService } from "../redis/redisService";
import { ExactlyOneKey, RedisMessage } from "../types";

export type MessageRetryOptions = ExactlyOneKey<{
  at: number;
  immediately: boolean;
  exponentialBackoff: boolean;
}> & { force?: boolean };

export interface HandleMessageRetryOptions<T> {
  topic: string;
  msgData: RedisMessage<T>;
  msgId: string;
  redisService: RedisService;
  retryOptions: MessageRetryOptions;
}

export default async function <T>({
  topic,
  msgData,
  msgId,
  redisService,
  retryOptions,
}: HandleMessageRetryOptions<T>) {
  if (msgData.retryCount < msgData.maxRetries || retryOptions.force) {
    if (retryOptions.immediately) {
      await redisService.addToStream(
        topic,
        "*",
        "data",
        msgData.data as string,
        "maxRetries",
        String(msgData.maxRetries),
        "retryCount",
        String(Number(msgData.retryCount) + 1)
      );
    } else if (retryOptions.exponentialBackoff) {
      const retryTime = Date.now() + 1000 * Math.pow(2, msgData.retryCount + 1);
      console.log(`[HERMES] Retrying ${topic}:${msgId} in ${retryTime}ms`);

      await redisService.scheduleMessage(
        topic,
        { ...msgData, retryCount: msgData.retryCount + 1 },
        retryTime
      );
    } else if (retryOptions.at) {
      await redisService.scheduleMessage(topic, msgData, retryOptions.at);
    }
  } else {
    console.error(
      `[HERMES] ${topic}:${msgId} Max retries exhausted... Adding to dead-letter queue...`
    );
    await redisService.addToDLQ(topic, msgData, Date.now());
  }
}
