import { z } from "zod";
import { Hermes } from "./src";

const main = async () => {
  // Instantiate and connect to Hermes
  const hermesTest = await Hermes({
    poolOptions: { min: 0, max: 20 },
    durableName: "playground",
    redisOptions: {
      host: process.env.REDIS_HOST || "0.0.0.0",
      port: Number(process.env.REDIS_PORT) || 6379,
      password: process.env.REDIS_PASSWORD || "",
    },
  }).connect();

  /** SERVICES **/

  // Register a service
  const sayHelloService = await hermesTest.registerService(
    "say-hello",
    z.object({
      name: z.string(),
      age: z.number(),
      favorites: z.object({ color: z.string() }),
    }),
    z.object({ message: z.string() })
  );

  // Register a reply handler for that service
  sayHelloService.reply(({ reqData, msgId }) => {
    return { message: `Hello, ${reqData.name}!` };
  });

  // Make a request to that service, the reply handler should process it, and return a response
  const response = await sayHelloService.request({
    name: "Swarnim",
    age: 12,
    favorites: { color: "Azure" },
  });

  // Print out the response
  console.log("GOT_RESP", response);

  /** MESSAGE BUS **/

  // Register an event
  const userSignUpEvent = await hermesTest.registerEvent(
    "user-signup",
    z.object({
      userId: z.number(),
      username: z.string(),
      deviceType: z.enum(["desktop", "mobile"]),
    })
  );

  // Register a subscriber handler to that event
  userSignUpEvent.subscribe(async ({ data, msg }) => {
    console.log("RECEIVED USER SIGNUP EVENT", data);
    await msg.ack();
  });

  // Publish event, the subscriber handler, should be invoked
  await userSignUpEvent.publish({
    userId: 1,
    username: "testUser",
    deviceType: "desktop",
  });

  await new Promise((resolve) => setTimeout(resolve, 24));
  await hermesTest.disconnect();
};

main();
