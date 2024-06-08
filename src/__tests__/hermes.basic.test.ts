import {
  afterAll,
  expect,
  describe,
  it,
  beforeAll,
  expectTypeOf,
  vi,
} from "vitest";
import { Hermes } from "..";
import { z } from "zod";
import { IHermes, IMsg } from "../lib/types";
import Redis from "ioredis";

const redisConfig = {
  host: process.env.REDIS_HOST || "0.0.0.0",
  port: Number(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || "",
};

const testRedis = new Redis({ ...redisConfig });

describe("Initialize", async () => {
  it("should establish a connection and initialize successfully", async () => {
    const hermes = await Hermes({
      durableName: "hermesTest",
      redisOptions: { ...redisConfig },
      poolOptions: { min: 0, max: 20 },
    }).connect();

    await hermes.disconnect();

    expect(hermes).toBeDefined();
  });
});

describe("Message Bus", async () => {
  let hermes: IHermes;
  beforeAll(async () => {
    hermes = await Hermes({
      durableName: "hermesTest",
      redisOptions: { ...redisConfig },
      poolOptions: { min: 0, max: 20 },
    }).connect();
  });

  it("should publish and subscribe to a topic", async () => {
    const topic = "test-topic";

    const payloadSchema = z.object({ message: z.string() });
    const msgPayload: z.infer<typeof payloadSchema> = { message: "hello" };

    const event = await hermes.registerEvent(topic, payloadSchema);
    const eventCallback = {
      fn: async ({
        msg,
        data,
      }: {
        msg: IMsg;
        data: z.infer<typeof payloadSchema>;
      }) => {
        console.log("Message received:", data);

        await msg.ack();
      },
    };

    const callbackFnSpy = vi.spyOn(eventCallback, "fn");

    event.subscribe(eventCallback.fn);
    await event.publish(msgPayload);

    await new Promise((resolve) => setTimeout(resolve, 24));
    expect(callbackFnSpy).toHaveBeenCalledOnce();
  });

  it(
    "should retry with exponential backoff on error",
    { timeout: 7500 },
    async () => {
      const topic = "fail-test-topic";

      const payloadSchema = z.object({ message: z.string() });
      const msgPayload: z.infer<typeof payloadSchema> = { message: "hello" };

      const event = await hermes.registerEvent(topic, payloadSchema);
      const eventCallback = {
        fn: async ({
          msg,
          data,
        }: {
          msg: IMsg;
          data: z.infer<typeof payloadSchema>;
        }) => {
          console.log("MSG_MAX_RETRIES", msg.maxRetries);
          console.log("MSG_RETRY_COUNT", msg.retryCount);

          if (msg.retryCount === msg.maxRetries - 1) {
            console.log("SUCCESS");

            await msg.ack();
          } else {
            console.log("FAIL");
            throw new Error("Test Error");
          }
        },
      };

      const callbackFnSpy = vi.spyOn(eventCallback, "fn");

      event.subscribe(eventCallback.fn);
      await event.publish(msgPayload);

      await new Promise((resolve) => setTimeout(resolve, 7000));
      expect(callbackFnSpy).toHaveBeenCalledTimes(3);
    }
  );

  it("should go to the dlq on final failure", { timeout: 15000 }, async () => {
    const topic = "dlq-test-topic";

    const payloadSchema = z.object({ message: z.string() });
    const msgPayload: z.infer<typeof payloadSchema> = { message: "hello" };

    const event = await hermes.registerEvent(topic, payloadSchema);
    const eventCallback = {
      fn: async () => {
        throw new Error("DQL Test Error");
      },
    };

    const callbackFnSpy = vi.spyOn(eventCallback, "fn");

    event.subscribe(eventCallback.fn);
    await event.publish(msgPayload);

    await new Promise((resolve) => setTimeout(resolve, 14500));

    expect(callbackFnSpy).toHaveBeenCalledTimes(4);

    const DQLKey = `hermes:${topic}-dlq`;
    const dlqSize = await testRedis.zcard(DQLKey);

    expect(dlqSize).toBeGreaterThanOrEqual(1);
  });

  afterAll(async () => {
    await hermes.disconnect();
  });
});

describe("Service", async () => {
  let hermes: IHermes;
  beforeAll(async () => {
    hermes = await Hermes({
      durableName: "hermesTest",
      redisOptions: { ...redisConfig },
      poolOptions: { min: 0, max: 20 },
    }).connect();
  });

  it("should request a reply for a service", async () => {
    const topic = "say-hello";

    const requestSchema = z.object({ name: z.string() });
    const responseSchema = z.object({ message: z.string() });

    const service = await hermes.registerService(
      topic,
      requestSchema,
      responseSchema
    );

    const requestData: z.infer<typeof requestSchema> = { name: "Joe" };

    // Register the reply
    service.reply(async ({ reqData }) => {
      expect(reqData).toMatchObject(requestData);
      return { message: `Hello, ${reqData.name}!` };
    });

    const response = await service.request(requestData);

    expectTypeOf(response).toMatchTypeOf<z.infer<typeof responseSchema>>();
    expect(response).toMatchSnapshot();
  });

  afterAll(async () => {
    await hermes.disconnect();
  });
});

afterAll(async () => {
  testRedis.disconnect();
});
