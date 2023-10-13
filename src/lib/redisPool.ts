import {
  Factory,
  Pool,
  createPool,
  Options as PoolOptions,
} from "generic-pool";
import Redis, { Redis as IRedis, RedisOptions } from "ioredis";
import { EventEmitter } from "stream";

export class RedisPool extends EventEmitter {
  private redisOpts: RedisOptions;
  private poolOpts: PoolOptions;
  private pool: Pool<IRedis>;

  constructor(redisOpts: RedisOptions, poolOpts: PoolOptions) {
    super();

    this.redisOpts = redisOpts;
    this.poolOpts = poolOpts;

    this.pool = this.constructPool();
  }

  private constructPool(): Pool<IRedis> {
    const factory: Factory<IRedis> = {
      create: (): Promise<IRedis> => {
        const context = this;
        return new Promise((resolve, reject) => {
          const client: IRedis = new Redis(this.redisOpts);

          client
            .on("error", (e: Error) => {
              context.emit("error", e, client);
              reject();
            })
            .on("connect", () => {
              context.emit("connect", client);
            })
            .on("ready", () => {
              context.emit("ready", client);
              resolve(client);
            })
            .on("reconnecting", () => {
              context.emit("reconnecting", client);
            });
        });
      },

      destroy: (client: IRedis): Promise<void> => {
        const context = this;
        return new Promise((resolve) => {
          client
            .on("close", (e: Error) => {
              context.emit("close", e, client);
            })
            .on("end", () => {
              context.emit("disconnected", client);
              resolve();
            })
            .disconnect();
        });
      },

      validate: (client): Promise<boolean> => {
        return new Promise((resolve) => {
          if (
            client.status === "connecting" ||
            client.status === "connect" ||
            client.status === "ready"
          ) {
            resolve(true);
          } else {
            resolve(false);
          }
        });
      },
    };

    return createPool(factory, this.poolOpts);
  }

  getConnection(priority?: number) {
    return this.pool.acquire(priority);
  }

  release(client: IRedis) {
    return this.pool.release(client);
  }

  disconnect(client: IRedis) {
    return this.pool.destroy(client);
  }

  async end() {
    await this.pool.drain();

    const res = await this.pool.clear();
    this.emit("end");
    return res;
  }

  async execute<T>(fn: (client: IRedis) => Promise<T>, priority?: number) {
    const client = await this.pool.acquire(priority);
    const result = await fn(client);
    await this.release(client);
    return result;
  }
}

export { Options as PoolOptions } from "generic-pool";
