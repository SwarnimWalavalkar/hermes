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

// Mixed workload configurations
const MIXED_CONFIGS = [
  { 
    name: 'Balanced Load', 
    serviceRequests: 200, 
    eventPublishes: 300, 
    concurrency: 20,
    duration: 30000 // 30 seconds
  },
  { 
    name: 'Service Heavy', 
    serviceRequests: 500, 
    eventPublishes: 200, 
    concurrency: 30,
    duration: 45000 // 45 seconds
  },
  { 
    name: 'Event Heavy', 
    serviceRequests: 150, 
    eventPublishes: 800, 
    concurrency: 40,
    duration: 60000 // 60 seconds
  },
];

// Schemas
const userServiceRequestSchema = z.object({
  userId: z.number(),
  operation: z.enum(['get_profile', 'update_profile', 'get_preferences']),
  data: z.record(z.any()).optional(),
});

const userServiceResponseSchema = z.object({
  success: z.boolean(),
  data: z.any(),
  timestamp: z.number(),
});

const notificationEventSchema = z.object({
  userId: z.number(),
  type: z.enum(['email', 'push', 'sms']),
  message: z.string(),
  priority: z.enum(['low', 'medium', 'high', 'urgent']),
  metadata: z.record(z.any()).optional(),
});

const analyticsEventSchema = z.object({
  sessionId: z.string(),
  userId: z.number().optional(),
  event: z.string(),
  properties: z.record(z.any()),
  timestamp: z.number(),
});

async function runMixedWorkloadTest(config: typeof MIXED_CONFIGS[0]) {
  console.log(`\n🚀 Running Mixed Workload Test: ${config.name}`);
  
  const hermesConfig = getDefaultConfig();
  const hermes = await createHermesInstance(hermesConfig);
  const runner = new LoadTestRunner();

  const stats = {
    serviceRequests: { sent: 0, successful: 0, failed: 0 },
    eventPublishes: { sent: 0, successful: 0, failed: 0 },
    eventProcessing: { processed: 0 },
  };

  try {
    // Register user service
    const userService = await hermes.registerService(
      'mixed-user-service',
      userServiceRequestSchema,
      userServiceResponseSchema
    );

    // Register service handler
    userService.reply(async ({ reqData }) => {
      // Simulate different processing times for different operations
      const processingTimes = {
        get_profile: 20,
        update_profile: 50,
        get_preferences: 15,
      };
      
      await sleep(processingTimes[reqData.operation] + Math.random() * 20);
      
      return {
        success: true,
        data: {
          userId: reqData.userId,
          operation: reqData.operation,
          result: `Processed ${reqData.operation} for user ${reqData.userId}`,
          ...reqData.data,
        },
        timestamp: Date.now(),
      };
    });

    // Register notification event
    const notificationEvent = await hermes.registerEvent(
      'mixed-notifications',
      notificationEventSchema
    );

    // Register analytics event
    const analyticsEvent = await hermes.registerEvent(
      'mixed-analytics',
      analyticsEventSchema
    );

    // Register event subscribers
    notificationEvent.subscribe(async ({ data, msg }) => {
      try {
        // Simulate notification processing
        const processingTime = data.priority === 'urgent' ? 10 : 30;
        await sleep(processingTime + Math.random() * 15);
        
        stats.eventProcessing.processed++;
        await msg.ack();
      } catch (error) {
        runner.recordError(`Notification processing: ${error instanceof Error ? error.message : String(error)}`);
      }
    });

    analyticsEvent.subscribe(async ({ data, msg }) => {
      try {
        // Simulate analytics processing
        await sleep(Math.random() * 20);
        
        stats.eventProcessing.processed++;
        await msg.ack();
      } catch (error) {
        runner.recordError(`Analytics processing: ${error instanceof Error ? error.message : String(error)}`);
      }
    });

    // Wait for setup
    await sleep(300);

    runner.start();

    // Create mixed workload tasks
    const allTasks: (() => Promise<void>)[] = [];

    // Service request tasks
    const operations = ['get_profile', 'update_profile', 'get_preferences'] as const;
    for (let i = 0; i < config.serviceRequests; i++) {
      allTasks.push(async () => {
        const startTime = performance.now();
        try {
          await userService.request({
            userId: Math.floor(Math.random() * 1000) + 1,
            operation: operations[i % operations.length],
            data: i % 2 === 0 ? { extra: `data-${i}` } : undefined,
          });
          
          const latency = performance.now() - startTime;
          runner.recordLatency(latency);
          stats.serviceRequests.sent++;
          stats.serviceRequests.successful++;
        } catch (error) {
          stats.serviceRequests.sent++;
          stats.serviceRequests.failed++;
          runner.recordError(`Service: ${error instanceof Error ? error.message : String(error)}`);
        }
      });
    }

    // Event publishing tasks
    const notificationTypes = ['email', 'push', 'sms'] as const;
    const priorities = ['low', 'medium', 'high', 'urgent'] as const;
    
    for (let i = 0; i < config.eventPublishes; i++) {
      // Mix of notification and analytics events
      if (i % 3 === 0) {
        // Analytics event
        allTasks.push(async () => {
          const startTime = performance.now();
          try {
            await analyticsEvent.publish({
              sessionId: `session-${Math.floor(Math.random() * 100)}`,
              userId: Math.random() > 0.3 ? Math.floor(Math.random() * 1000) + 1 : undefined,
              event: `action_${i % 10}`,
              properties: {
                page: `/page-${i % 5}`,
                timestamp: Date.now(),
                source: 'load-test',
              },
              timestamp: Date.now(),
            });
            
            const latency = performance.now() - startTime;
            runner.recordLatency(latency);
            stats.eventPublishes.sent++;
            stats.eventPublishes.successful++;
          } catch (error) {
            stats.eventPublishes.sent++;
            stats.eventPublishes.failed++;
            runner.recordError(`Analytics Event: ${error instanceof Error ? error.message : String(error)}`);
          }
        });
      } else {
        // Notification event
        allTasks.push(async () => {
          const startTime = performance.now();
          try {
            await notificationEvent.publish({
              userId: Math.floor(Math.random() * 1000) + 1,
              type: notificationTypes[i % notificationTypes.length],
              message: `Notification message ${i}`,
              priority: priorities[i % priorities.length],
              metadata: {
                campaign: `campaign-${Math.floor(i / 50)}`,
                source: 'mixed-load-test',
              },
            });
            
            const latency = performance.now() - startTime;
            runner.recordLatency(latency);
            stats.eventPublishes.sent++;
            stats.eventPublishes.successful++;
          } catch (error) {
            stats.eventPublishes.sent++;
            stats.eventPublishes.failed++;
            runner.recordError(`Notification Event: ${error instanceof Error ? error.message : String(error)}`);
          }
        });
      }
    }

    // Shuffle tasks to create realistic mixed workload
    for (let i = allTasks.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [allTasks[i], allTasks[j]] = [allTasks[j], allTasks[i]];
    }

    // Execute mixed workload
    await runConcurrent(allTasks, config.concurrency);

    // Wait for event processing
    const maxWaitTime = 15000;
    const startWait = Date.now();
    const expectedEvents = stats.eventPublishes.successful;
    
    while (
      stats.eventProcessing.processed < expectedEvents &&
      (Date.now() - startWait) < maxWaitTime
    ) {
      await sleep(100);
    }

    runner.end();
    
    const result = runner.getResults(`Mixed Workload - ${config.name}`, {
      ...config,
      stats,
    });
    
    printResults(result);
    
    console.log('\nWorkload Statistics:');
    console.log(`Service Requests: ${stats.serviceRequests.successful}/${stats.serviceRequests.sent} successful`);
    console.log(`Event Publishes: ${stats.eventPublishes.successful}/${stats.eventPublishes.sent} successful`);
    console.log(`Events Processed: ${stats.eventProcessing.processed}/${expectedEvents}`);
    console.log(`Service Success Rate: ${((stats.serviceRequests.successful / stats.serviceRequests.sent) * 100).toFixed(2)}%`);
    console.log(`Event Success Rate: ${((stats.eventPublishes.successful / stats.eventPublishes.sent) * 100).toFixed(2)}%`);

    return result;
  } finally {
    await hermes.disconnect();
  }
}

async function runStressTest() {
  console.log(`\n🚀 Running Stress Test`);
  
  const hermesConfig = {
    ...getDefaultConfig(),
    poolOptions: { min: 5, max: 100 }, // Increase pool size for stress test
  };
  
  const hermes = await createHermesInstance(hermesConfig);
  const runner = new LoadTestRunner();

  const stats = {
    operations: 0,
    errors: 0,
    memorySnapshots: [] as NodeJS.MemoryUsage[],
  };

  try {
    // Register a simple service for stress testing
    const stressService = await hermes.registerService(
      'stress-service',
      z.object({ id: z.number(), payload: z.string() }),
      z.object({ id: z.number(), result: z.string(), timestamp: z.number() })
    );

    stressService.reply(async ({ reqData }) => {
      await sleep(Math.random() * 5); // Very fast processing
      return {
        id: reqData.id,
        result: `Processed ${reqData.payload}`,
        timestamp: Date.now(),
      };
    });

    // Register a simple event for stress testing
    const stressEvent = await hermes.registerEvent(
      'stress-events',
      z.object({ id: z.number(), data: z.string() })
    );

    stressEvent.subscribe(async ({ data, msg }) => {
      await sleep(Math.random() * 3); // Very fast processing
      await msg.ack();
    });

    await sleep(200);

    runner.start();

    // Run stress test for a fixed duration
    const stressDuration = 30000; // 30 seconds
    const startTime = Date.now();
    const tasks: (() => Promise<void>)[] = [];

    // Generate a large number of tasks
    for (let i = 0; i < 5000; i++) {
      if (i % 2 === 0) {
        // Service request
        tasks.push(async () => {
          const opStart = performance.now();
          try {
            await stressService.request({
              id: i,
              payload: `stress-data-${i}`,
            });
            runner.recordLatency(performance.now() - opStart);
            stats.operations++;
          } catch (error) {
            stats.errors++;
            runner.recordError(error instanceof Error ? error.message : String(error));
          }
        });
      } else {
        // Event publish
        tasks.push(async () => {
          const opStart = performance.now();
          try {
            await stressEvent.publish({
              id: i,
              data: `stress-event-${i}`,
            });
            runner.recordLatency(performance.now() - opStart);
            stats.operations++;
          } catch (error) {
            stats.errors++;
            runner.recordError(error instanceof Error ? error.message : String(error));
          }
        });
      }
    }

    // Memory monitoring
    const memoryMonitor = setInterval(() => {
      stats.memorySnapshots.push(process.memoryUsage());
    }, 1000);

    // Execute with high concurrency
    await runConcurrent(tasks, 150);

    clearInterval(memoryMonitor);
    
    // Wait a bit for final processing
    await sleep(5000);

    runner.end();
    
    const peakMemory = stats.memorySnapshots.reduce(
      (peak, current) => current.heapUsed > peak.heapUsed ? current : peak,
      stats.memorySnapshots[0] || process.memoryUsage()
    );

    const result = runner.getResults('Stress Test', {
      totalOperations: stats.operations,
      errorCount: stats.errors,
      peakMemoryMB: peakMemory.heapUsed / 1024 / 1024,
      memoryGrowth: stats.memorySnapshots.length > 1 
        ? (stats.memorySnapshots[stats.memorySnapshots.length - 1].heapUsed - stats.memorySnapshots[0].heapUsed) / 1024 / 1024
        : 0,
    });
    
    printResults(result);
    
    console.log('\nStress Test Statistics:');
    console.log(`Total Operations: ${stats.operations}`);
    console.log(`Error Count: ${stats.errors}`);
    console.log(`Error Rate: ${((stats.errors / (stats.operations + stats.errors)) * 100).toFixed(2)}%`);
    console.log(`Peak Memory: ${(peakMemory.heapUsed / 1024 / 1024).toFixed(2)}MB`);

    return result;
  } finally {
    await hermes.disconnect();
  }
}

async function runEnduranceTest() {
  console.log(`\n🚀 Running Endurance Test (2 minutes)`);
  
  const hermesConfig = getDefaultConfig();
  const hermes = await createHermesInstance(hermesConfig);
  const runner = new LoadTestRunner();

  const stats = {
    totalOperations: 0,
    intervals: [] as { timestamp: number; operations: number; memory: number }[],
  };

  try {
    // Simple service for endurance testing
    const enduranceService = await hermes.registerService(
      'endurance-service',
      z.object({ id: z.number() }),
      z.object({ id: z.number(), timestamp: z.number() })
    );

    enduranceService.reply(async ({ reqData }) => {
      await sleep(10 + Math.random() * 10);
      return { id: reqData.id, timestamp: Date.now() };
    });

    await sleep(200);

    runner.start();

    // Run for 2 minutes with consistent load
    const enduranceDuration = 120000; // 2 minutes
    const startTime = Date.now();
    let operationId = 0;

    // Track performance over time
    const intervalTracker = setInterval(() => {
      stats.intervals.push({
        timestamp: Date.now() - startTime,
        operations: stats.totalOperations,
        memory: process.memoryUsage().heapUsed / 1024 / 1024,
      });
    }, 10000); // Every 10 seconds

    while ((Date.now() - startTime) < enduranceDuration) {
      const batchTasks = Array.from({ length: 50 }, () => 
        async () => {
          const opStart = performance.now();
          try {
            await enduranceService.request({ id: operationId++ });
            runner.recordLatency(performance.now() - opStart);
            stats.totalOperations++;
          } catch (error) {
            runner.recordError(error instanceof Error ? error.message : String(error));
          }
        }
      );

      await runConcurrent(batchTasks, 10);
      await sleep(1000); // 1 second between batches
    }

    clearInterval(intervalTracker);
    runner.end();
    
    const result = runner.getResults('Endurance Test', {
      duration: enduranceDuration,
      totalOperations: stats.totalOperations,
      intervals: stats.intervals,
    });
    
    printResults(result);
    
    console.log('\nEndurance Test Statistics:');
    console.log(`Total Operations: ${stats.totalOperations}`);
    console.log(`Operations per minute: ${((stats.totalOperations / 2)).toFixed(2)}`);
    
    if (stats.intervals.length > 1) {
      const memoryGrowth = stats.intervals[stats.intervals.length - 1].memory - stats.intervals[0].memory;
      console.log(`Memory Growth: ${memoryGrowth.toFixed(2)}MB over 2 minutes`);
    }

    return result;
  } finally {
    await hermes.disconnect();
  }
}

async function main() {
  console.log('🧪 Starting Hermes Mixed Load Testing Suite');
  console.log('=============================================');

  const results = [];

  try {
    // Run mixed workload tests
    console.log('\n📊 Mixed Workload Tests');
    for (const config of MIXED_CONFIGS) {
      const result = await runMixedWorkloadTest(config);
      results.push(result);
      await sleep(3000); // Longer pause between mixed tests
    }

    // Run stress test
    const stressResult = await runStressTest();
    results.push(stressResult);
    await sleep(5000);

    // Run endurance test
    const enduranceResult = await runEnduranceTest();
    results.push(enduranceResult);

    // Final summary
    console.log('\n📈 MIXED LOAD TEST SUMMARY');
    console.log('===========================');
    results.forEach(result => {
      console.log(`${result.testName}:`);
      console.log(`  Throughput: ${result.metrics.throughput.toFixed(2)} ops/s`);
      console.log(`  Avg Latency: ${result.metrics.averageLatency.toFixed(2)}ms`);
      console.log(`  P95 Latency: ${result.metrics.p95Latency.toFixed(2)}ms`);
      console.log(`  Error Rate: ${result.metrics.errorRate.toFixed(2)}%`);
      console.log(`  Memory Peak: ${(result.memoryUsage.peak.heapUsed / 1024 / 1024).toFixed(2)}MB`);
      console.log('');
    });

    // Performance recommendations
    console.log('🎯 PERFORMANCE RECOMMENDATIONS');
    console.log('===============================');
    
    const avgThroughput = results.reduce((sum, r) => sum + r.metrics.throughput, 0) / results.length;
    const avgLatency = results.reduce((sum, r) => sum + r.metrics.averageLatency, 0) / results.length;
    const maxErrorRate = Math.max(...results.map(r => r.metrics.errorRate));
    
    console.log(`Average Throughput: ${avgThroughput.toFixed(2)} ops/s`);
    console.log(`Average Latency: ${avgLatency.toFixed(2)}ms`);
    console.log(`Max Error Rate: ${maxErrorRate.toFixed(2)}%`);
    
    if (avgLatency > 100) {
      console.log('⚠️  Consider optimizing processing logic or increasing Redis pool size');
    }
    if (maxErrorRate > 5) {
      console.log('⚠️  High error rate detected - investigate error handling and retry logic');
    }
    if (avgThroughput < 100) {
      console.log('⚠️  Low throughput - consider increasing concurrency or optimizing Redis configuration');
    }

  } catch (error) {
    console.error('❌ Mixed load test failed:', error);
    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  main().catch(console.error);
}

export { main as runMixedLoadTest };