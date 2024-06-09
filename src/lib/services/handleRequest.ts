import z from "zod";
import { RedisService } from "../redis/redisService";
import bus from "../internal/bus";
import { Maybe } from "../types";
import getStreamMessageGenerator from "../internal/getStreamMessageGenerator";

export interface HandleRequestOptions<RequestType, ResponseType> {
  reqData: RequestType;
  redisService: RedisService;
  requestSchema: z.Schema<RequestType>;
  responseSchema: z.Schema<ResponseType>;
  topic: string;
  groupName: string;
  isAlive: () => boolean;
}

export default async function <RequestType, ResponseType>({
  reqData,
  requestSchema,
  responseSchema,
  redisService,
  topic,
  groupName,
  isAlive,
}: HandleRequestOptions<RequestType, ResponseType>): Promise<ResponseType> {
  let parsedRequestData: RequestType;

  try {
    parsedRequestData = requestSchema.parse(reqData);
  } catch (error) {
    console.error(`[HERMES] Service: ${topic}, request parse error`);
    throw new Error("Request Parse Error");
  }

  const responseTopic = `${topic}-res`;

  await bus.publish<RequestType>(topic, parsedRequestData, {
    redisService,
  });

  const responseGenerator = getStreamMessageGenerator({
    streamName: responseTopic,
    count: 1,
    redisService,
    isAlive,
  });

  const messageResp = await responseGenerator.next();

  if (!messageResp.done && messageResp.value) {
    const message = messageResp.value;

    if (message[0] && message[1] && message[1][1]) {
      const data: Maybe<ResponseType> = JSON.parse(
        message[1][1] as unknown as string
      );
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
