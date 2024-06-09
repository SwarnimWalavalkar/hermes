import z from "zod";
import { MessageRetryOptions } from "../events/handleMessageRetry";

export type Maybe<T> = T | null | undefined;

export type ExactlyOneKey<T, Keys extends keyof T = keyof T> = {
  [K in Keys]: Required<Pick<T, K>> &
    Partial<Record<Exclude<Keys, K>, undefined>> extends infer O
    ? { [P in keyof O]: O[P] }
    : never;
}[Keys];

export interface IBus {
  subscribe<T>(
    topic: string,
    callback: (msgData: { data: T; msgId: string }) => Promise<void>
  ): void | Promise<void>;
  publish<T>(topic: string, data: T): Promise<void>;
}

export type RawRedisMessage = [string, string[][]];

export interface RedisMessage<T> {
  data: T;
  maxRetries: number;
  retryCount: number;
}

export interface IMsg {
  id: string;
  retryCount: number;
  maxRetries: number;
  ack: () => Promise<void>;
  retry: (options: MessageRetryOptions) => Promise<void>;
}

export interface IEvent<MessagePayload> {
  subscribe(
    fn: (msgData: { data: MessagePayload; msg: IMsg }) => void | Promise<void>
  ): Promise<void>;
  publish(
    data: MessagePayload,
    options?: { maxRetries?: number }
  ): Promise<void>;
}

export interface IService<RequestType, ResponseType> {
  reply(
    fn: (msgData: {
      reqData: RequestType;
      msgId: string;
    }) => ResponseType | Promise<ResponseType>
  ): void | Promise<void>;
  request(reqData: RequestType): Promise<ResponseType>;
}

export interface IHermes {
  connect(): Promise<IHermes>;
  disconnect(): Promise<void>;
  registerEvent<MessagePayload>(
    topic: string,
    payloadSchema: z.Schema<MessagePayload>,
    options?: { maxRetries?: number }
  ): Promise<IEvent<MessagePayload>>;
  registerService<RequestType, ResponseType>(
    topic: string,
    requestSchema: z.Schema<RequestType>,
    responseSchema: z.Schema<ResponseType>
  ): Promise<IService<RequestType, ResponseType>>;
}
