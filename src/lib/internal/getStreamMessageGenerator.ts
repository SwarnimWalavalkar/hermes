import { RedisService } from "../redis/redisService";
import { RawRedisMessage, RedisMessage } from "../types";

export interface StreamMessagesGeneratorOptions {
  streamName: string;
  count: number;
  redisService: RedisService;
  isAlive: () => boolean;
}

export default async function* ({
  redisService,
  streamName,
  count,
  isAlive,
}: StreamMessagesGeneratorOptions): AsyncGenerator<RawRedisMessage> {
  let fetchNewMessages = true;
  while (isAlive()) {
    const results = fetchNewMessages
      ? await redisService.readStreamAsConsumerGroup(streamName, count)
      : await redisService.autoClaimMessages(streamName, count);

    fetchNewMessages = !fetchNewMessages;

    const nextScheduledMsg = await redisService.getNextScheduledMessage<
      RedisMessage<string>
    >(streamName);

    if (nextScheduledMsg) {
      await redisService.addToStream(
        streamName,
        "*",
        "data",
        nextScheduledMsg.msgData.data,
        "maxRetries",
        String(nextScheduledMsg.msgData.maxRetries),
        "retryCount",
        String(nextScheduledMsg.msgData.retryCount)
      );
    }

    if (!results || !results.length) {
      continue;
    }

    for (const message of results) {
      yield message;
    }
  }
}
