import {
  afterAll,
  expect,
  describe,
  it,
  beforeAll,
  expectTypeOf,
  vi,
} from "vitest";
import { Hermes, IHermes, IMsg } from "..";
import { z } from "zod";

import { Redis as UpstashRedis } from "@upstash/redis";

if (
  !String(process.env.UPSTASH_REDIS_REST_URL).length ||
  !String(process.env.UPSTASH_REDIS_REST_TOKEN).length
) {
  throw new Error(
    "UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN are not defined in the environment"
  );
}
const upstashRedis = new UpstashRedis({
  url: String(process.env.UPSTASH_REDIS_REST_URL),
  token: String(process.env.UPSTASH_REDIS_REST_TOKEN),
});

describe("Initialize", async () => {
  it("should establish a connection and initialize successfully", async () => {
    const hermes = Hermes({
      durableName: "hermesTest",
      persistanceType: "UPSTASH",
      upstashRedis,
    });

    await hermes.connect();

    await hermes.disconnect();

    expect(hermes).toBeDefined();
  });
});

describe("Message Bus", async () => {
  let hermes: IHermes;
  beforeAll(async () => {
    hermes = await Hermes({
      durableName: "hermesTest",
      persistanceType: "UPSTASH",
      upstashRedis,
    }).connect();
  });

  it.fails("should publish and subscribe to a topic", async () => {
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

  afterAll(async () => {
    await hermes.disconnect();
  });
});

describe("Service", async () => {
  let hermes: IHermes;
  beforeAll(async () => {
    hermes = await Hermes({
      durableName: "hermesTest",
      persistanceType: "UPSTASH",
      upstashRedis,
    }).connect();
  });

  it.fails("should request a reply for a service", async () => {
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
