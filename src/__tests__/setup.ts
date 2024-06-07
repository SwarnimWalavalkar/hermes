import Redis from "ioredis";
import { afterAll, beforeAll } from "vitest";

afterAll(async () => {
  const redisConfig = {
    host: process.env.REDIS_HOST || "0.0.0.0",
    port: Number(process.env.REDIS_PORT) || 6379,
    password: process.env.REDIS_PASSWORD || "",
  };

  const testRedis = new Redis({ ...redisConfig });

  await testRedis.keys(`hermes:*`, async (_, keys) => {
    if (!!keys && keys.length > 0) {
      await testRedis.del(keys);
    }
  });

  testRedis.disconnect();
});
