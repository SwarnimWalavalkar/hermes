import z from "zod";
import { IMsg, Maybe, RedisMessage } from "../types";
import { RedisService } from "../redis/redisService";
import getStreamMessageGenerator from "../internal/getStreamMessageGenerator";

export interface HandleSubscriptionsOptions<MsgPayload> {
  payloadSchema: z.Schema<MsgPayload>;
  redisService: RedisService;
  topic: string;
  groupName: string;
  maxRetries: number;
  isAlive: () => boolean;
}

export default async function <MsgPayload>(
  callback: (msgData: { data: MsgPayload; msg: IMsg }) => Promise<void>,
  {
    topic,
    groupName,
    maxRetries,
    payloadSchema,
    redisService,
    isAlive,
  }: HandleSubscriptionsOptions<MsgPayload>
) {
  try {
    const generator = getStreamMessageGenerator({
      streamName: topic,
      count: 10,
      redisService,
      isAlive,
    });

    for await (const message of generator) {
      if (message.length && message[1] && message[1][1]) {
        const msgId: string = String(message[0]);

        const messageDataArray = message[1] as unknown as string[];

        const redisMessage = messageDataArray.reduce(
          (
            acc: RedisMessage<Maybe<MsgPayload>>,
            item: string,
            index: number
          ) => {
            if (index % 2 === 0) {
              if (index + 1 < messageDataArray.length) {
                const value = messageDataArray[index + 1];
                // @ts-expect-error ignore
                acc[item] = value;
              }
            }
            return acc;
          },
          {} as RedisMessage<Maybe<MsgPayload>>
        );

        let parsedData: MsgPayload;
        try {
          parsedData = payloadSchema.parse(
            JSON.parse(redisMessage.data as string)
          );
        } catch (error) {
          console.error(`[HERMES] Message Bus: ${topic}, message parse error`);
          throw new Error("Message Parse Error");
        }

        try {
          await callback({
            data: parsedData,
            msg: {
              id: msgId,
              retryCount: Number(redisMessage.retryCount),
              maxRetries: Number(redisMessage.maxRetries),
              ack: async () =>
                await redisService.ackMessages(topic, groupName, msgId),
            },
          });
        } catch (_error) {
          console.error(`[HERMES] ${topic}:${msgId} Callback Error...`);

          const retryCount = Number(redisMessage.retryCount);

          if (retryCount < maxRetries) {
            const retryTime = Date.now() + 1000 * Math.pow(2, retryCount + 1);

            console.log(
              `[HERMES] Retrying ${topic}:${msgId} in ${retryTime}ms`
            );

            await redisService.scheduleMessage(
              topic,
              { ...redisMessage, retryCount: retryCount + 1 },
              retryTime
            );
          }

          await redisService.ackMessages(topic, groupName, msgId);
        }
      }
    }
  } catch (error) {
    console.error("[HERMES] Error while subscribing:", error);
    throw error;
  }
}
