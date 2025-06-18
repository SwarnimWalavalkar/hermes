#!/usr/bin/env tsx

import { z } from 'zod';
import { performance } from 'perf_hooks';
import { 
  LoadTestRunner, 
  printResults, 
  createHermesInstance, 
  getDefaultConfig, 
  sleep,
  runConcurrent 
} from './load-test-utils';

// Test configurations
const TEST_CONFIGS = [
  { name: 'Low Load', requests: 100, concurrency: 5 },
  { name: 'Medium Load', requests: 500, concurrency: 20 },
  { name: 'High Load', requests: 1000, concurrency: 50 },
  { name: 'Burst Load', requests: 2000, concurrency: 100 },
];

// Schemas for testing
const simpleRequestSchema = z.object({
  id: z.number(),
  message: z.string(),
});

const simpleResponseSchema = z.object({
  id: z.number(),
  response: z.string(),
  timestamp: z.number(),
});

const complexRequestSchema = z.object({
  userId: z.number(),
  operation: z.enum(['create', 'update', 'delete', 'read']),
  data: z.object({
    name: z.string(),
    email: z.string().email(),
    metadata: z.record(z.any()),
  }),
  options: z.object({
    timeout: z.number().optional(),
    retries: z.number().optional(),
  }).optional(),
});

const complexResponseSchema = z.object({
  success: z.boolean(),
  data: z.any(),
  processingTime: z.number(),
  serverId: z.string(),
});

async function runSimpleServiceLoadTest(config: typeof TEST_CONFIGS[0]) {
  console.log(`\n🚀 Running Simple Service Load Test: ${config.name}`);
  
  const hermesConfig = getDefaultConfig();
  const hermes = await createHermesInstance(hermesConfig);
  const runner = new LoadTestRunner();

  try {
    // Register service
    const service = await hermes.registerService(
      'simple-service',
      simpleRequestSchema,
      simpleResponseSchema
    );

    // Register reply handler
    service.reply(async ({ reqData }) => {
      // Simulate some processing time
      await sleep(Math.random() * 10);
      return {
        id: reqData.id,
        response: `Processed: ${reqData.message}`,
        timestamp: Date.now(),
      };
    });

    // Wait for service to be ready
    await sleep(100);

    runner.start();

    // Create request tasks
    const tasks = Array.from({ length: config.requests }, (_, i) => 
      async () => {
        const startTime = performance.now();
        try {
          await service.request({
            id: i,
            message: `Test message ${i}`,
          });
          const latency = performance.now() - startTime;
          runner.recordLatency(latency);
        } catch (error) {
          runner.recordError(error instanceof Error ? error.message : String(error));
        }
      }
    );

    // Execute requests with controlled concurrency
    await runConcurrent(tasks, config.concurrency);

    runner.end();
    const result = runner.getResults(`Simple Service - ${config.name}`, config);
    printResults(result);

    return result;
  } finally {
    await hermes.disconnect();
  }
}

async function runComplexServiceLoadTest(config: typeof TEST_CONFIGS[0]) {
  console.log(`\n🚀 Running Complex Service Load Test: ${config.name}`);
  
  const hermesConfig = getDefaultConfig();
  const hermes = await createHermesInstance(hermesConfig);
  const runner = new LoadTestRunner();

  try {
    // Register service
    const service = await hermes.registerService(
      'complex-service',
      complexRequestSchema,
      complexResponseSchema
    );

    // Register reply handler with more complex processing
    service.reply(async ({ reqData }) => {
      const processingStart = performance.now();
      
      // Simulate different processing times based on operation
      const processingTimes = {
        create: 50,
        update: 30,
        delete: 20,
        read: 10,
      };
      
      await sleep(processingTimes[reqData.operation] + Math.random() * 20);
      
      // Simulate some data processing
      const processedData = {
        ...reqData.data,
        processed: true,
        processedAt: new Date().toISOString(),
      };

      const processingTime = performance.now() - processingStart;

      return {
        success: true,
        data: processedData,
        processingTime,
        serverId: `server-${Math.floor(Math.random() * 3) + 1}`,
      };
    });

    await sleep(100);

    runner.start();

    // Create request tasks with varied complexity
    const operations = ['create', 'update', 'delete', 'read'] as const;
    const tasks = Array.from({ length: config.requests }, (_, i) => 
      async () => {
        const startTime = performance.now();
        try {
          await service.request({
            userId: Math.floor(Math.random() * 1000) + 1,
            operation: operations[i % operations.length],
            data: {
              name: `User ${i}`,
              email: `user${i}@example.com`,
              metadata: {
                source: 'load-test',
                batch: Math.floor(i / 100),
                timestamp: Date.now(),
              },
            },
            options: {
              timeout: 5000,
              retries: 3,
            },
          });
          const latency = performance.now() - startTime;
          runner.recordLatency(latency);
        } catch (error) {
          runner.recordError(error instanceof Error ? error.message : String(error));
        }
      }
    );

    await runConcurrent(tasks, config.concurrency);

    runner.end();
    const result = runner.getResults(`Complex Service - ${config.name}`, config);
    printResults(result);

    return result;
  } finally {
    await hermes.disconnect();
  }
}

async function runErrorHandlingLoadTest() {
  console.log(`\n🚀 Running Error Handling Load Test`);
  
  const hermesConfig = getDefaultConfig();
  const hermes = await createHermesInstance(hermesConfig);
  const runner = new LoadTestRunner();

  try {
    const service = await hermes.registerService(
      'error-service',
      simpleRequestSchema,
      simpleResponseSchema
    );

    // Register reply handler that fails 30% of the time
    service.reply(async ({ reqData }) => {
      if (Math.random() < 0.3) {
        throw new Error(`Simulated error for request ${reqData.id}`);
      }
      
      await sleep(Math.random() * 20);
      return {
        id: reqData.id,
        response: `Success: ${reqData.message}`,
        timestamp: Date.now(),
      };
    });

    await sleep(100);

    runner.start();

    const tasks = Array.from({ length: 200 }, (_, i) => 
      async () => {
        const startTime = performance.now();
        try {
          await service.request({
            id: i,
            message: `Error test message ${i}`,
          });
          const latency = performance.now() - startTime;
          runner.recordLatency(latency);
        } catch (error) {
          runner.recordError(error instanceof Error ? error.message : String(error));
        }
      }
    );

    await runConcurrent(tasks, 20);

    runner.end();
    const result = runner.getResults('Error Handling Test', { requests: 200, concurrency: 20 });
    printResults(result);

    return result;
  } finally {
    await hermes.disconnect();
  }
}

async function runTimeoutLoadTest() {
  console.log(`\n🚀 Running Timeout Load Test`);
  
  const hermesConfig = getDefaultConfig();
  const hermes = await createHermesInstance(hermesConfig);
  const runner = new LoadTestRunner();

  try {
    const service = await hermes.registerService(
      'timeout-service',
      simpleRequestSchema,
      simpleResponseSchema
    );

    // Register reply handler with variable response times
    service.reply(async ({ reqData }) => {
      // Some requests take longer than others
      const delay = reqData.id % 10 === 0 ? 2000 : Math.random() * 100;
      await sleep(delay);
      
      return {
        id: reqData.id,
        response: `Delayed response: ${reqData.message}`,
        timestamp: Date.now(),
      };
    });

    await sleep(100);

    runner.start();

    const tasks = Array.from({ length: 100 }, (_, i) => 
      async () => {
        const startTime = performance.now();
        try {
          await service.request({
            id: i,
            message: `Timeout test message ${i}`,
          });
          const latency = performance.now() - startTime;
          runner.recordLatency(latency);
        } catch (error) {
          runner.recordError(error instanceof Error ? error.message : String(error));
        }
      }
    );

    await runConcurrent(tasks, 10);

    runner.end();
    const result = runner.getResults('Timeout Test', { requests: 100, concurrency: 10 });
    printResults(result);

    return result;
  } finally {
    await hermes.disconnect();
  }
}

async function main() {
  console.log('🧪 Starting Hermes Services Load Testing Suite');
  console.log('================================================');

  const results = [];

  try {
    // Run simple service tests
    console.log('\n📊 Simple Service Load Tests');
    for (const config of TEST_CONFIGS) {
      const result = await runSimpleServiceLoadTest(config);
      results.push(result);
      await sleep(1000); // Brief pause between tests
    }

    // Run complex service tests
    console.log('\n📊 Complex Service Load Tests');
    for (const config of TEST_CONFIGS.slice(0, 2)) { // Run fewer complex tests
      const result = await runComplexServiceLoadTest(config);
      results.push(result);
      await sleep(1000);
    }

    // Run error handling test
    const errorResult = await runErrorHandlingLoadTest();
    results.push(errorResult);
    await sleep(1000);

    // Run timeout test
    const timeoutResult = await runTimeoutLoadTest();
    results.push(timeoutResult);

    // Summary
    console.log('\n📈 LOAD TEST SUMMARY');
    console.log('====================');
    results.forEach(result => {
      console.log(`${result.testName}:`);
      console.log(`  Throughput: ${result.metrics.throughput.toFixed(2)} req/s`);
      console.log(`  Avg Latency: ${result.metrics.averageLatency.toFixed(2)}ms`);
      console.log(`  Error Rate: ${result.metrics.errorRate.toFixed(2)}%`);
      console.log(`  Memory Peak: ${(result.memoryUsage.peak.heapUsed / 1024 / 1024).toFixed(2)}MB`);
      console.log('');
    });

  } catch (error) {
    console.error('❌ Load test failed:', error);
    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  main().catch(console.error);
}

export { main as runServicesLoadTest };