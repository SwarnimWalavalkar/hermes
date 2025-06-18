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
  { name: 'Low Volume', events: 200, concurrency: 10, subscribers: 2 },
  { name: 'Medium Volume', events: 1000, concurrency: 25, subscribers: 5 },
  { name: 'High Volume', events: 2000, concurrency: 50, subscribers: 10 },
  { name: 'Burst Volume', events: 5000, concurrency: 100, subscribers: 15 },
];

// Event schemas
const userEventSchema = z.object({
  userId: z.number(),
  action: z.enum(['signup', 'login', 'logout', 'update_profile', 'delete_account']),
  timestamp: z.number(),
  metadata: z.record(z.any()).optional(),
});

const orderEventSchema = z.object({
  orderId: z.string(),
  customerId: z.number(),
  amount: z.number(),
  currency: z.string(),
  items: z.array(z.object({
    productId: z.string(),
    quantity: z.number(),
    price: z.number(),
  })),
  status: z.enum(['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']),
});

const systemEventSchema = z.object({
  eventType: z.string(),
  severity: z.enum(['info', 'warning', 'error', 'critical']),
  message: z.string(),
  source: z.string(),
  timestamp: z.number(),
  details: z.record(z.any()).optional(),
});

async function runBasicEventLoadTest(config: typeof TEST_CONFIGS[0]) {
  console.log(`\n🚀 Running Basic Event Load Test: ${config.name}`);
  
  const hermesConfig = getDefaultConfig();
  const hermes = await createHermesInstance(hermesConfig);
  const runner = new LoadTestRunner();

  const processedEvents = new Set<number>();
  let totalProcessed = 0;

  try {
    // Register event
    const userEvent = await hermes.registerEvent('user-events', userEventSchema);

    // Register multiple subscribers
    const subscribers = [];
    for (let i = 0; i < config.subscribers; i++) {
      const subscriberPromise = userEvent.subscribe(async ({ data, msg }) => {
        try {
          // Simulate processing time
          await sleep(Math.random() * 10);
          
          processedEvents.add(data.userId);
          totalProcessed++;
          
          await msg.ack();
        } catch (error) {
          runner.recordError(`Subscriber ${i}: ${error instanceof Error ? error.message : String(error)}`);
        }
      });
      subscribers.push(subscriberPromise);
    }

    // Wait for subscribers to be ready
    await sleep(200);

    runner.start();

    // Create publishing tasks
    const actions = ['signup', 'login', 'logout', 'update_profile', 'delete_account'] as const;
    const tasks = Array.from({ length: config.events }, (_, i) => 
      async () => {
        const startTime = performance.now();
        try {
          await userEvent.publish({
            userId: i,
            action: actions[i % actions.length],
            timestamp: Date.now(),
            metadata: {
              source: 'load-test',
              batch: Math.floor(i / 100),
            },
          });
          const latency = performance.now() - startTime;
          runner.recordLatency(latency);
        } catch (error) {
          runner.recordError(error instanceof Error ? error.message : String(error));
        }
      }
    );

    // Publish events with controlled concurrency
    await runConcurrent(tasks, config.concurrency);

    // Wait for all events to be processed
    const maxWaitTime = 30000; // 30 seconds
    const startWait = Date.now();
    while (totalProcessed < config.events && (Date.now() - startWait) < maxWaitTime) {
      await sleep(100);
    }

    runner.end();
    
    const result = runner.getResults(`Basic Events - ${config.name}`, {
      ...config,
      eventsProcessed: totalProcessed,
      processingRate: (totalProcessed / config.events) * 100,
    });
    
    printResults(result);
    console.log(`Events Processed: ${totalProcessed}/${config.events} (${((totalProcessed / config.events) * 100).toFixed(2)}%)`);

    return result;
  } finally {
    await hermes.disconnect();
  }
}

async function runComplexEventLoadTest(config: typeof TEST_CONFIGS[0]) {
  console.log(`\n🚀 Running Complex Event Load Test: ${config.name}`);
  
  const hermesConfig = getDefaultConfig();
  const hermes = await createHermesInstance(hermesConfig);
  const runner = new LoadTestRunner();

  let totalProcessed = 0;
  const processingTimes: number[] = [];

  try {
    // Register order event
    const orderEvent = await hermes.registerEvent('order-events', orderEventSchema);

    // Register subscribers with different processing logic
    for (let i = 0; i < config.subscribers; i++) {
      orderEvent.subscribe(async ({ data, msg }) => {
        const processingStart = performance.now();
        try {
          // Simulate complex processing based on order amount
          const processingTime = data.amount > 1000 ? 50 : 20;
          await sleep(processingTime + Math.random() * 30);
          
          // Simulate some business logic
          if (data.status === 'pending' && data.amount > 10000) {
            // Simulate additional validation for high-value orders
            await sleep(20);
          }
          
          totalProcessed++;
          processingTimes.push(performance.now() - processingStart);
          
          await msg.ack();
        } catch (error) {
          runner.recordError(`Order processor ${i}: ${error instanceof Error ? error.message : String(error)}`);
        }
      });
    }

    await sleep(200);

    runner.start();

    // Create order publishing tasks
    const statuses = ['pending', 'confirmed', 'shipped', 'delivered'] as const;
    const tasks = Array.from({ length: config.events }, (_, i) => 
      async () => {
        const startTime = performance.now();
        try {
          const itemCount = Math.floor(Math.random() * 5) + 1;
          const items = Array.from({ length: itemCount }, (_, j) => ({
            productId: `product-${i}-${j}`,
            quantity: Math.floor(Math.random() * 3) + 1,
            price: Math.floor(Math.random() * 100) + 10,
          }));

          const totalAmount = items.reduce((sum, item) => sum + (item.price * item.quantity), 0);

          await orderEvent.publish({
            orderId: `order-${i}`,
            customerId: Math.floor(Math.random() * 1000) + 1,
            amount: totalAmount,
            currency: 'USD',
            items,
            status: statuses[i % statuses.length],
          });
          
          const latency = performance.now() - startTime;
          runner.recordLatency(latency);
        } catch (error) {
          runner.recordError(error instanceof Error ? error.message : String(error));
        }
      }
    );

    await runConcurrent(tasks, config.concurrency);

    // Wait for processing
    const maxWaitTime = 45000; // 45 seconds for complex events
    const startWait = Date.now();
    while (totalProcessed < config.events && (Date.now() - startWait) < maxWaitTime) {
      await sleep(100);
    }

    runner.end();
    
    const avgProcessingTime = processingTimes.length > 0 
      ? processingTimes.reduce((sum, time) => sum + time, 0) / processingTimes.length 
      : 0;

    const result = runner.getResults(`Complex Events - ${config.name}`, {
      ...config,
      eventsProcessed: totalProcessed,
      processingRate: (totalProcessed / config.events) * 100,
      avgProcessingTime,
    });
    
    printResults(result);
    console.log(`Events Processed: ${totalProcessed}/${config.events} (${((totalProcessed / config.events) * 100).toFixed(2)}%)`);
    console.log(`Avg Processing Time: ${avgProcessingTime.toFixed(2)}ms`);

    return result;
  } finally {
    await hermes.disconnect();
  }
}

async function runMultiTopicLoadTest() {
  console.log(`\n🚀 Running Multi-Topic Load Test`);
  
  const hermesConfig = getDefaultConfig();
  const hermes = await createHermesInstance(hermesConfig);
  const runner = new LoadTestRunner();

  const topicStats = {
    user: { published: 0, processed: 0 },
    order: { published: 0, processed: 0 },
    system: { published: 0, processed: 0 },
  };

  try {
    // Register multiple events
    const userEvent = await hermes.registerEvent('multi-user-events', userEventSchema);
    const orderEvent = await hermes.registerEvent('multi-order-events', orderEventSchema);
    const systemEvent = await hermes.registerEvent('multi-system-events', systemEventSchema);

    // Register subscribers for each topic
    userEvent.subscribe(async ({ data, msg }) => {
      await sleep(Math.random() * 15);
      topicStats.user.processed++;
      await msg.ack();
    });

    orderEvent.subscribe(async ({ data, msg }) => {
      await sleep(Math.random() * 25);
      topicStats.order.processed++;
      await msg.ack();
    });

    systemEvent.subscribe(async ({ data, msg }) => {
      await sleep(Math.random() * 10);
      topicStats.system.processed++;
      await msg.ack();
    });

    await sleep(200);

    runner.start();

    // Create mixed publishing tasks
    const totalEvents = 600;
    const tasks = Array.from({ length: totalEvents }, (_, i) => 
      async () => {
        const startTime = performance.now();
        try {
          const topicChoice = i % 3;
          
          if (topicChoice === 0) {
            // User event
            await userEvent.publish({
              userId: i,
              action: 'login',
              timestamp: Date.now(),
            });
            topicStats.user.published++;
          } else if (topicChoice === 1) {
            // Order event
            await orderEvent.publish({
              orderId: `multi-order-${i}`,
              customerId: i,
              amount: Math.floor(Math.random() * 500) + 50,
              currency: 'USD',
              items: [{
                productId: `product-${i}`,
                quantity: 1,
                price: Math.floor(Math.random() * 100) + 10,
              }],
              status: 'pending',
            });
            topicStats.order.published++;
          } else {
            // System event
            await systemEvent.publish({
              eventType: 'api_call',
              severity: 'info',
              message: `API call ${i}`,
              source: 'load-test',
              timestamp: Date.now(),
            });
            topicStats.system.published++;
          }
          
          const latency = performance.now() - startTime;
          runner.recordLatency(latency);
        } catch (error) {
          runner.recordError(error instanceof Error ? error.message : String(error));
        }
      }
    );

    await runConcurrent(tasks, 30);

    // Wait for processing
    const maxWaitTime = 30000;
    const startWait = Date.now();
    const totalExpected = topicStats.user.published + topicStats.order.published + topicStats.system.published;
    
    while (
      (topicStats.user.processed + topicStats.order.processed + topicStats.system.processed) < totalExpected &&
      (Date.now() - startWait) < maxWaitTime
    ) {
      await sleep(100);
    }

    runner.end();
    
    const result = runner.getResults('Multi-Topic Test', {
      totalEvents,
      topicStats,
    });
    
    printResults(result);
    console.log('\nTopic Statistics:');
    console.log(`User Events: ${topicStats.user.processed}/${topicStats.user.published}`);
    console.log(`Order Events: ${topicStats.order.processed}/${topicStats.order.published}`);
    console.log(`System Events: ${topicStats.system.processed}/${topicStats.system.published}`);

    return result;
  } finally {
    await hermes.disconnect();
  }
}

async function runFailureRecoveryTest() {
  console.log(`\n🚀 Running Failure Recovery Test`);
  
  const hermesConfig = getDefaultConfig();
  const hermes = await createHermesInstance(hermesConfig);
  const runner = new LoadTestRunner();

  let totalProcessed = 0;
  let failureCount = 0;
  let recoveryCount = 0;

  try {
    const event = await hermes.registerEvent('failure-test-events', userEventSchema);

    // Subscriber that fails 20% of the time initially, then recovers
    event.subscribe(async ({ data, msg }) => {
      try {
        // Simulate failures for first 20% of events
        if (data.userId < 40 && Math.random() < 0.2) {
          failureCount++;
          throw new Error(`Simulated failure for user ${data.userId}`);
        }
        
        // Simulate recovery processing
        if (data.userId >= 40) {
          recoveryCount++;
        }
        
        await sleep(Math.random() * 15);
        totalProcessed++;
        await msg.ack();
      } catch (error) {
        runner.recordError(error instanceof Error ? error.message : String(error));
        // Don't ack failed messages to test retry mechanism
      }
    });

    await sleep(200);

    runner.start();

    const tasks = Array.from({ length: 200 }, (_, i) => 
      async () => {
        const startTime = performance.now();
        try {
          await event.publish({
            userId: i,
            action: 'signup',
            timestamp: Date.now(),
          });
          const latency = performance.now() - startTime;
          runner.recordLatency(latency);
        } catch (error) {
          runner.recordError(error instanceof Error ? error.message : String(error));
        }
      }
    );

    await runConcurrent(tasks, 20);

    // Wait for processing and retries
    await sleep(10000); // Give time for retries

    runner.end();
    
    const result = runner.getResults('Failure Recovery Test', {
      totalEvents: 200,
      totalProcessed,
      failureCount,
      recoveryCount,
    });
    
    printResults(result);
    console.log(`\nFailure Recovery Stats:`);
    console.log(`Total Processed: ${totalProcessed}/200`);
    console.log(`Initial Failures: ${failureCount}`);
    console.log(`Recovery Events: ${recoveryCount}`);

    return result;
  } finally {
    await hermes.disconnect();
  }
}

async function main() {
  console.log('🧪 Starting Hermes Events Load Testing Suite');
  console.log('==============================================');

  const results = [];

  try {
    // Run basic event tests
    console.log('\n📊 Basic Event Load Tests');
    for (const config of TEST_CONFIGS) {
      const result = await runBasicEventLoadTest(config);
      results.push(result);
      await sleep(2000); // Longer pause for event processing
    }

    // Run complex event tests (fewer to save time)
    console.log('\n📊 Complex Event Load Tests');
    for (const config of TEST_CONFIGS.slice(0, 2)) {
      const result = await runComplexEventLoadTest(config);
      results.push(result);
      await sleep(2000);
    }

    // Run multi-topic test
    const multiTopicResult = await runMultiTopicLoadTest();
    results.push(multiTopicResult);
    await sleep(2000);

    // Run failure recovery test
    const failureResult = await runFailureRecoveryTest();
    results.push(failureResult);

    // Summary
    console.log('\n📈 EVENTS LOAD TEST SUMMARY');
    console.log('============================');
    results.forEach(result => {
      console.log(`${result.testName}:`);
      console.log(`  Throughput: ${result.metrics.throughput.toFixed(2)} events/s`);
      console.log(`  Avg Latency: ${result.metrics.averageLatency.toFixed(2)}ms`);
      console.log(`  Error Rate: ${result.metrics.errorRate.toFixed(2)}%`);
      console.log(`  Memory Peak: ${(result.memoryUsage.peak.heapUsed / 1024 / 1024).toFixed(2)}MB`);
      console.log('');
    });

  } catch (error) {
    console.error('❌ Events load test failed:', error);
    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  main().catch(console.error);
}

export { main as runEventsLoadTest };