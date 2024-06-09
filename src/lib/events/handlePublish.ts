import z from "zod";
import { RedisService } from "../redis/redisService";

export interface HandlePublishOptions {
  topic: string;
  taskMaxRetries: number;
  redisService: RedisService;
}

export default async function <MsgPayload>(
  payloadSchema: z.Schema<MsgPayload>,
  payload: MsgPayload,
  { topic, taskMaxRetries, redisService }: HandlePublishOptions
) {
  let parsedData: MsgPayload;
  try {
    parsedData = payloadSchema.parse(payload);
  } catch (error) {
    console.error(`[HERMES] Message Bus: ${topic}, message parse error`);
    throw new Error("Message Parse Error");
  }

  await redisService.addToStream(
    topic,
    "*",
    "data",
    JSON.stringify(parsedData),
    "maxRetries",
    taskMaxRetries.toString(),
    "retryCount",
    "0"
  );
}
