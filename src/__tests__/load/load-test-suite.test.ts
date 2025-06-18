import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { z } from 'zod';
import { performance } from 'perf_hooks';
import { 
  LoadTestRunner, 
  createHermesInstance, 
  getDefaultConfig, 
  sleep 
} from './load-test-utils';
import type { IHermes } from '../../index';

describe('Hermes Load Testing Suite', () => {
  let hermes: IHermes;
  
  beforeAll(async () => {
    hermes = await createHermesInstance(getDefaultConfig());
  });

  afterAll(async () => {
    if (hermes) {
      await hermes.disconnect();
    }
  });

  describe('Service Load Tests', () => {
    it('should handle concurrent service requests', async () => {
      const runner = new LoadTestRunner();
      const requestSchema = z.object({ id: z.number(), message: z.string() });
      const responseSchema = z.object({ id: z.number(), response: z.string() });

      const service = await hermes.registerService(
        'test-service-concurrent',
        requestSchema,
        responseSchema
      );

      service.reply(async ({ reqData }) => {
        await sleep(Math.random() * 20);
        return {
          id: reqData.id,
          response: `Processed: ${reqData.message}`,
        };
      });

      await sleep(100);

      runner.start();

      const requests = Array.from({ length: 100 }, (_, i) => 
        async () => {
          const start = performance.now();
          try {
            await service.request({ id: i, message: `Test ${i}` });
            runner.recordLatency(performance.now() - start);
          } catch (error) {
            runner.recordError(error instanceof Error ? error.message : String(error));
          }
        }
      );

      await Promise.all(requests.map(req => req()));

      runner.end();
      const result = runner.getResults('Concurrent Service Test', { requests: 100 });

      expect(result.metrics.successfulRequests).toBeGreaterThan(90);
      expect(result.metrics.errorRate).toBeLessThan(10);
      expect(result.metrics.averageLatency).toBeLessThan(1000);
    }, 30000);

    it('should maintain performance under high service load', async () => {
      const runner = new LoadTestRunner();
      const requestSchema = z.object({ id: z.number() });
      const responseSchema = z.object({ id: z.number(), timestamp: z.number() });

      const service = await hermes.registerService(
        'test-service-high-load',
        requestSchema,
        responseSchema
      );

      service.reply(async ({ reqData }) => {
        await sleep(5);
        return { id: reqData.id, timestamp: Date.now() };
      });

      await sleep(100);

      runner.start();

      const concurrentRequests = 50;
      const requestsPerBatch = 10;
      const batches = Array.from({ length: concurrentRequests }, (_, batchIndex) =>
        Array.from({ length: requestsPerBatch }, (_, i) => 
          async () => {
            const start = performance.now();
            try {
              await service.request({ id: batchIndex * requestsPerBatch + i });
              runner.recordLatency(performance.now() - start);
            } catch (error) {
              runner.recordError(error instanceof Error ? error.message : String(error));
            }
          }
        )
      );

      await Promise.all(
        batches.map(batch => Promise.all(batch.map(req => req())))
      );

      runner.end();
      const result = runner.getResults('High Load Service Test', { 
        totalRequests: concurrentRequests * requestsPerBatch 
      });

      expect(result.metrics.throughput).toBeGreaterThan(10); // At least 10 req/s
      expect(result.metrics.errorRate).toBeLessThan(5);
    }, 60000);
  });

  describe('Event Load Tests', () => {
    it('should handle high volume event publishing', async () => {
      const runner = new LoadTestRunner();
      const eventSchema = z.object({ id: z.number(), data: z.string() });

      const event = await hermes.registerEvent('test-event-volume', eventSchema);
      
      let processedCount = 0;
      event.subscribe(async ({ data, msg }) => {
        await sleep(Math.random() * 10);
        processedCount++;
        await msg.ack();
      });

      await sleep(200);

      runner.start();

      const eventCount = 200;
      const publishTasks = Array.from({ length: eventCount }, (_, i) => 
        async () => {
          const start = performance.now();
          try {
            await event.publish({ id: i, data: `Event data ${i}` });
            runner.recordLatency(performance.now() - start);
          } catch (error) {
            runner.recordError(error instanceof Error ? error.message : String(error));
          }
        }
      );

      await Promise.all(publishTasks.map(task => task()));

      // Wait for processing
      const maxWait = 15000;
      const startWait = Date.now();
      while (processedCount < eventCount && (Date.now() - startWait) < maxWait) {
        await sleep(100);
      }

      runner.end();
      const result = runner.getResults('High Volume Event Test', { 
        events: eventCount,
        processed: processedCount 
      });

      expect(result.metrics.successfulRequests).toBe(eventCount);
      expect(processedCount).toBeGreaterThan(eventCount * 0.9); // At least 90% processed
      expect(result.metrics.errorRate).toBeLessThan(5);
    }, 45000);

    it('should handle multiple subscribers efficiently', async () => {
      const runner = new LoadTestRunner();
      const eventSchema = z.object({ id: z.number(), message: z.string() });

      const event = await hermes.registerEvent('test-event-multi-sub', eventSchema);
      
      const subscriberCounts = [0, 0, 0];
      
      // Register multiple subscribers
      for (let i = 0; i < 3; i++) {
        event.subscribe(async ({ data, msg }) => {
          await sleep(Math.random() * 15);
          subscriberCounts[i]++;
          await msg.ack();
        });
      }

      await sleep(200);

      runner.start();

      const eventCount = 150;
      const publishTasks = Array.from({ length: eventCount }, (_, i) => 
        async () => {
          const start = performance.now();
          try {
            await event.publish({ id: i, message: `Multi-sub event ${i}` });
            runner.recordLatency(performance.now() - start);
          } catch (error) {
            runner.recordError(error instanceof Error ? error.message : String(error));
          }
        }
      );

      await Promise.all(publishTasks.map(task => task()));

      // Wait for processing
      await sleep(10000);

      runner.end();
      const result = runner.getResults('Multi-Subscriber Event Test', { 
        events: eventCount,
        subscriberCounts 
      });

      const totalProcessed = subscriberCounts.reduce((sum, count) => sum + count, 0);
      
      expect(result.metrics.successfulRequests).toBe(eventCount);
      expect(totalProcessed).toBeGreaterThan(eventCount * 2); // At least 2 subscribers processing
      expect(result.metrics.errorRate).toBeLessThan(5);
    }, 30000);
  });

  describe('Mixed Workload Tests', () => {
    it('should handle mixed service and event workload', async () => {
      const runner = new LoadTestRunner();
      
      // Setup service
      const serviceRequestSchema = z.object({ id: z.number(), type: z.string() });
      const serviceResponseSchema = z.object({ id: z.number(), result: z.string() });
      
      const service = await hermes.registerService(
        'test-mixed-service',
        serviceRequestSchema,
        serviceResponseSchema
      );

      service.reply(async ({ reqData }) => {
        await sleep(Math.random() * 25);
        return { id: reqData.id, result: `Mixed result for ${reqData.type}` };
      });

      // Setup event
      const eventSchema = z.object({ id: z.number(), action: z.string() });
      const event = await hermes.registerEvent('test-mixed-event', eventSchema);
      
      let eventProcessedCount = 0;
      event.subscribe(async ({ data, msg }) => {
        await sleep(Math.random() * 15);
        eventProcessedCount++;
        await msg.ack();
      });

      await sleep(200);

      runner.start();

      const serviceRequests = 50;
      const eventPublishes = 75;
      
      // Create mixed tasks
      const allTasks: (() => Promise<void>)[] = [];
      
      // Service tasks
      for (let i = 0; i < serviceRequests; i++) {
        allTasks.push(async () => {
          const start = performance.now();
          try {
            await service.request({ id: i, type: 'mixed-test' });
            runner.recordLatency(performance.now() - start);
          } catch (error) {
            runner.recordError(`Service: ${error instanceof Error ? error.message : String(error)}`);
          }
        });
      }
      
      // Event tasks
      for (let i = 0; i < eventPublishes; i++) {
        allTasks.push(async () => {
          const start = performance.now();
          try {
            await event.publish({ id: i, action: 'mixed-action' });
            runner.recordLatency(performance.now() - start);
          } catch (error) {
            runner.recordError(`Event: ${error instanceof Error ? error.message : String(error)}`);
          }
        });
      }

      // Shuffle for realistic mixed workload
      for (let i = allTasks.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [allTasks[i], allTasks[j]] = [allTasks[j], allTasks[i]];
      }

      await Promise.all(allTasks.map(task => task()));

      // Wait for event processing
      await sleep(8000);

      runner.end();
      const result = runner.getResults('Mixed Workload Test', { 
        serviceRequests,
        eventPublishes,
        eventProcessedCount 
      });

      expect(result.metrics.successfulRequests).toBe(serviceRequests + eventPublishes);
      expect(eventProcessedCount).toBeGreaterThan(eventPublishes * 0.8);
      expect(result.metrics.errorRate).toBeLessThan(10);
      expect(result.metrics.throughput).toBeGreaterThan(5);
    }, 45000);
  });

  describe('Error Handling and Recovery', () => {
    it('should handle service errors gracefully', async () => {
      const runner = new LoadTestRunner();
      const requestSchema = z.object({ id: z.number(), shouldFail: z.boolean() });
      const responseSchema = z.object({ id: z.number(), result: z.string() });

      const service = await hermes.registerService(
        'test-error-service',
        requestSchema,
        responseSchema
      );

      service.reply(async ({ reqData }) => {
        if (reqData.shouldFail) {
          throw new Error(`Intentional failure for request ${reqData.id}`);
        }
        await sleep(10);
        return { id: reqData.id, result: 'Success' };
      });

      await sleep(100);

      runner.start();

      const totalRequests = 100;
      const failureRate = 0.3; // 30% failure rate
      
      const requests = Array.from({ length: totalRequests }, (_, i) => 
        async () => {
          const start = performance.now();
          try {
            await service.request({ 
              id: i, 
              shouldFail: Math.random() < failureRate 
            });
            runner.recordLatency(performance.now() - start);
          } catch (error) {
            runner.recordError(error instanceof Error ? error.message : String(error));
          }
        }
      );

      await Promise.all(requests.map(req => req()));

      runner.end();
      const result = runner.getResults('Error Handling Test', { 
        totalRequests,
        expectedFailureRate: failureRate * 100 
      });

      // Should handle errors without crashing
      expect(result.metrics.totalRequests).toBe(totalRequests);
      expect(result.metrics.errorRate).toBeGreaterThan(20); // Should have some errors
      expect(result.metrics.errorRate).toBeLessThan(40); // But not too many
    }, 30000);

    it('should recover from temporary failures', async () => {
      const runner = new LoadTestRunner();
      const eventSchema = z.object({ id: z.number(), data: z.string() });

      const event = await hermes.registerEvent('test-recovery-event', eventSchema);
      
      let processedCount = 0;
      let failureCount = 0;
      
      event.subscribe(async ({ data, msg }) => {
        try {
          // Fail first 20% of messages, then recover
          if (data.id < 20 && Math.random() < 0.5) {
            failureCount++;
            throw new Error(`Temporary failure for event ${data.id}`);
          }
          
          await sleep(Math.random() * 10);
          processedCount++;
          await msg.ack();
        } catch (error) {
          runner.recordError(error instanceof Error ? error.message : String(error));
          // Don't ack failed messages to test retry
        }
      });

      await sleep(200);

      runner.start();

      const eventCount = 100;
      const publishTasks = Array.from({ length: eventCount }, (_, i) => 
        async () => {
          const start = performance.now();
          try {
            await event.publish({ id: i, data: `Recovery test ${i}` });
            runner.recordLatency(performance.now() - start);
          } catch (error) {
            runner.recordError(error instanceof Error ? error.message : String(error));
          }
        }
      );

      await Promise.all(publishTasks.map(task => task()));

      // Wait longer for retries and recovery
      await sleep(15000);

      runner.end();
      const result = runner.getResults('Recovery Test', { 
        events: eventCount,
        processed: processedCount,
        failures: failureCount 
      });

      expect(result.metrics.successfulRequests).toBe(eventCount);
      expect(processedCount).toBeGreaterThan(eventCount * 0.7); // Most should eventually process
    }, 45000);
  });

  describe('Performance Benchmarks', () => {
    it('should meet minimum throughput requirements', async () => {
      const runner = new LoadTestRunner();
      const requestSchema = z.object({ id: z.number() });
      const responseSchema = z.object({ id: z.number(), timestamp: z.number() });

      const service = await hermes.registerService(
        'test-benchmark-service',
        requestSchema,
        responseSchema
      );

      service.reply(async ({ reqData }) => {
        // Minimal processing time
        await sleep(1);
        return { id: reqData.id, timestamp: Date.now() };
      });

      await sleep(100);

      runner.start();

      const requestCount = 500;
      const concurrency = 25;
      
      const batches = [];
      for (let i = 0; i < requestCount; i += concurrency) {
        const batch = Array.from({ length: Math.min(concurrency, requestCount - i) }, (_, j) => 
          async () => {
            const start = performance.now();
            try {
              await service.request({ id: i + j });
              runner.recordLatency(performance.now() - start);
            } catch (error) {
              runner.recordError(error instanceof Error ? error.message : String(error));
            }
          }
        );
        batches.push(batch);
      }

      for (const batch of batches) {
        await Promise.all(batch.map(req => req()));
      }

      runner.end();
      const result = runner.getResults('Throughput Benchmark', { 
        requests: requestCount,
        concurrency 
      });

      // Performance assertions
      expect(result.metrics.throughput).toBeGreaterThan(50); // At least 50 req/s
      expect(result.metrics.averageLatency).toBeLessThan(100); // Under 100ms average
      expect(result.metrics.p95Latency).toBeLessThan(200); // P95 under 200ms
      expect(result.metrics.errorRate).toBeLessThan(1); // Less than 1% errors
    }, 60000);
  });
});