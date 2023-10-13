import { z } from "zod";
import { Hermes } from "./src";

const main = async () => {
  const hermesTest = await Hermes({
    durableName: "playground",
    redisOptions: {},
  }).connect();

  const sayHelloService = await hermesTest.registerService(
    "say-hello",
    z.object({
      name: z.string(),
      age: z.number(),
      favorites: z.object({ color: z.string() }),
    }),
    z.object({ message: z.string() })
  );

  sayHelloService.reply(({ reqData, msgId }) => {
    return { message: `Hello, ${reqData.name}!` };
  });

  const response = await sayHelloService.request({
    name: "Swarnim",
    age: 12,
    favorites: { color: "Azure" },
  });

  console.log("GOT_RESP", response);

  const messagePayloadSchema = z.object({
    userId: z.number(),
    username: z.string(),
    deviceType: z.enum(["desktop", "mobile"]),
  });

  const userSignUpEvent = await hermesTest.registerEvent(
    "user-signup",
    messagePayloadSchema
  );

  userSignUpEvent.subscribe(async ({ data, msg }) => {
    console.log("RECEIVED USER SIGNUP EVENT", data);
    await msg.ack();
  });

  await userSignUpEvent.publish({
    userId: 1,
    username: "testUser",
    deviceType: "desktop",
  });

  await new Promise((resolve) => setTimeout(resolve, 24));
  await hermesTest.disconnect();
};

main();
