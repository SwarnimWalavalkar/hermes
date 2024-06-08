import { afterAll, expect, describe, it, beforeAll, vi } from "vitest";
import { Hermes } from "..";
import { z } from "zod";
import { IHermes, IMsg } from "../lib/types";

const redisConfig = {
  host: process.env.REDIS_HOST || "0.0.0.0",
  port: Number(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || "",
};

describe("Queue", async () => {
  let hermes: IHermes;
  beforeAll(async () => {
    hermes = await Hermes({
      durableName: "hermesTest",
      redisOptions: { ...redisConfig },
      poolOptions: { min: 0, max: 20 },
    }).connect();
  });

  it("should work as a fifo queue", async () => {
    const messageSchema = z.object({ message: z.string() });
    type MessageData = z.infer<typeof messageSchema>;

    const topic = await hermes.registerEvent("fifo-test", messageSchema);

    const messages: Array<MessageData> = [
      { message: "first" },
      { message: "second" },
      { message: "third" },
    ];

    const messageResults: Array<string> = [];

    const messageCallback = {
      fn: async ({ msg, data }: { msg: IMsg; data: MessageData }) => {
        console.log("Message received:", data);
        messageResults.push(data.message);

        await msg.ack();
      },
    };

    const callbackFnSpy = vi.spyOn(messageCallback, "fn");

    topic.subscribe(messageCallback.fn);

    for (const message of messages) {
      await topic.publish(message);
    }

    await new Promise((resolve) => setTimeout(resolve, 24));

    for (let i = 0; i < messages.length; i++) {
      expect(callbackFnSpy).toHaveBeenCalledWith({
        msg: expect.any(Object),
        data: messages[i],
      });

      expect(messageResults[i]).toBe(messages[i]?.message);
    }
  });

  afterAll(async () => {
    await hermes.disconnect();
  });
});
