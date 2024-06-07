import z from "zod";
import { RedisService } from "../redis/redisService";
import bus from "../internal/bus";

export interface HandleReplyOptions<RequestType, ResponseType> {
  requestSchema: z.Schema<RequestType>;
  responseSchema: z.Schema<ResponseType>;
  redisService: RedisService;
  topic: string;
  groupName: string;
  isAlive: () => boolean;
}

export default async function <RequestType, ResponseType>(
  callback: (msgData: {
    reqData: RequestType;
    msgId: string;
  }) => ResponseType | Promise<ResponseType>,
  {
    requestSchema,
    responseSchema,
    topic,
    redisService,
    groupName,
    isAlive,
  }: HandleReplyOptions<RequestType, ResponseType>
) {
  await bus.subscribe<RequestType>(
    async ({ msgId, data }) => {
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
    },
    {
      topic,
      groupName,
      redisService,
      isAlive,
      maxRetries: 1,
    }
  );
}
