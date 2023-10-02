# Hermes

Type safe message bus, and "request-response" style services. Powered by Redis Streams

# Features

- Type-safety
- Schema validation with [Zod](https://github.com/colinhacks/zod)
- Horizontally scalable
- Highly reliable (guaranteed at-least-once message delivery)
  - at-least-once system
  - consumers need to explicitly acknowledge that a message has been processed
  - If (when) a consumer dies, all of the pending messages assigned to that consumer are transferred to another consumer after a timeout

# Example

Type-Safe Service
![Type Safe Service Example](static/type-safe-service-exmaple.png)

Type-Safe Message Bus
![Type-Safe Message Bus](static/type-safe-messaege-bus-exmaple.png)

# Improvement Ideas

- [ ] Connection Pooling for Redis
- [x] Consumer transfer on timeout
- [ ] Custom Logger
  - [ ] log levels
- [ ] Worker threads for subscriptions
- [ ] Better observability tools

### New Features

- [ ] Job scheduler

# Contributing
