import z from "zod";

export type Maybe<T> = T | null | undefined;

export interface IBus {
  subscribe<T>(
    topic: string,
    callback: (msgData: { data: T; msgId: string }) => Promise<void>
  ): void | Promise<void>;
  publish<T>(topic: string, data: T): Promise<void>;
}

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
