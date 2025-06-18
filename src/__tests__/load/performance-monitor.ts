#!/usr/bin/env tsx

import { z } from 'zod';
import { performance } from 'perf_hooks';
import { 
  createHermesInstance, 
  getDefaultConfig, 
  sleep 
} from './load-test-utils';

interface PerformanceSnapshot {
  timestamp: number;
  memory: NodeJS.MemoryUsage;
  operations: number;
  errors: number;
  latencies: number[];
}

class PerformanceMonitor {
  private snapshots: PerformanceSnapshot[] = [];
  private operations = 0;
  private errors = 0;
  private latencies: number[] = [];
  private isRunning = false;

  start() {
    this.isRunning = true;
    this.takeSnapshot();
    
    // Take snapshots every 5 seconds
    const interval = setInterval(() => {
      if (!this.isRunning) {
        clearInterval(interval);
        return;
      }
      this.takeSnapshot();
    }, 5000);
  }

  stop() {
    this.isRunning = false;
    this.takeSnapshot(); // Final snapshot
  }

  recordOperation(latency: number) {
    this.operations++;
    this.latencies.push(latency);
  }

  recordError() {
    this.errors++;
  }

  private takeSnapshot() {
    this.snapshots.push({
      timestamp: Date.now(),
      memory: process.memoryUsage(),
      operations: this.operations,
      errors: this.errors,
      latencies: [...this.latencies],
    });
    
    // Keep only recent latencies to prevent memory bloat
    if (this.latencies.length > 1000) {
      this.latencies = this.latencies.slice(-500);
    }
  }

  generateReport(): string {
    if (this.snapshots.length < 2) {
      return 'Insufficient data for report';
    }

    const first = this.snapshots[0];
    const last = this.snapshots[this.snapshots.length - 1];
    const duration = (last.timestamp - first.timestamp) / 1000; // seconds

    const totalOps = last.operations - first.operations;
    const totalErrors = last.errors - first.errors;
    const throughput = totalOps / duration;

    const allLatencies = last.latencies.sort((a, b) => a - b);
    const avgLatency = allLatencies.length > 0 
      ? allLatencies.reduce((sum, lat) => sum + lat, 0) / allLatencies.length 
      : 0;

    const p95Index = Math.floor(allLatencies.length * 0.95);
    const p95Latency = allLatencies[p95Index] || 0;

    const memoryGrowth = (last.memory.heapUsed - first.memory.heapUsed) / 1024 / 1024;
    const peakMemory = Math.max(...this.snapshots.map(s => s.memory.heapUsed)) / 1024 / 1024;

    let report = '\n📊 PERFORMANCE MONITORING REPORT\n';
    report += '=================================\n';
    report += `Duration: ${duration.toFixed(1)}s\n`;
    report += `Total Operations: ${totalOps}\n`;
    report += `Total Errors: ${totalErrors}\n`;
    report += `Throughput: ${throughput.toFixed(2)} ops/s\n`;
    report += `Error Rate: ${((totalErrors / (totalOps + totalErrors)) * 100).toFixed(2)}%\n`;
    report += `\nLatency:\n`;
    report += `  Average: ${avgLatency.toFixed(2)}ms\n`;
    report += `  P95: ${p95Latency.toFixed(2)}ms\n`;
    report += `\nMemory:\n`;
    report += `  Growth: ${memoryGrowth.toFixed(2)}MB\n`;
    report += `  Peak: ${peakMemory.toFixed(2)}MB\n`;

    // Performance over time
    report += `\nPerformance Timeline:\n`;
    for (let i = 1; i < this.snapshots.length; i++) {
      const prev = this.snapshots[i - 1];
      const curr = this.snapshots[i];
      const intervalDuration = (curr.timestamp - prev.timestamp) / 1000;
      const intervalOps = curr.operations - prev.operations;
      const intervalThroughput = intervalOps / intervalDuration;
      const intervalMemory = curr.memory.heapUsed / 1024 / 1024;
      
      report += `  ${intervalDuration.toFixed(0)}s: ${intervalThroughput.toFixed(1)} ops/s, ${intervalMemory.toFixed(1)}MB\n`;
    }

    return report;
  }
}

async function runPerformanceMonitoring() {
  console.log('🔍 Starting Hermes Performance Monitoring');
  console.log('==========================================');
  console.log('This will run a continuous load test while monitoring performance metrics.');
  console.log('Press Ctrl+C to stop and generate report.\n');

  const config = getDefaultConfig();
  const hermes = await createHermesInstance(config);
  const monitor = new PerformanceMonitor();

  try {
    // Setup test service
    const testService = await hermes.registerService(
      'monitor-service',
      z.object({ id: z.number(), data: z.string() }),
      z.object({ id: z.number(), result: z.string(), timestamp: z.number() })
    );

    testService.reply(async ({ reqData }) => {
      // Variable processing time
      await sleep(Math.random() * 50 + 10);
      return {
        id: reqData.id,
        result: `Processed: ${reqData.data}`,
        timestamp: Date.now(),
      };
    });

    // Setup test event
    const testEvent = await hermes.registerEvent(
      'monitor-events',
      z.object({ id: z.number(), message: z.string() })
    );

    testEvent.subscribe(async ({ data, msg }) => {
      await sleep(Math.random() * 30 + 5);
      await msg.ack();
    });

    await sleep(200);

    monitor.start();

    // Handle graceful shutdown
    process.on('SIGINT', () => {
      console.log('\n\n🛑 Stopping performance monitoring...');
      monitor.stop();
      console.log(monitor.generateReport());
      hermes.disconnect().then(() => {
        console.log('\n✅ Monitoring stopped. Goodbye!');
        process.exit(0);
      });
    });

    console.log('🚀 Performance monitoring started. Generating load...\n');

    let requestId = 0;
    
    // Continuous load generation
    while (true) {
      // Batch of operations
      const batchSize = Math.floor(Math.random() * 20) + 10;
      const operations = [];

      for (let i = 0; i < batchSize; i++) {
        if (Math.random() < 0.6) {
          // Service request
          operations.push(async () => {
            const start = performance.now();
            try {
              await testService.request({
                id: requestId++,
                data: `monitor-data-${requestId}`,
              });
              monitor.recordOperation(performance.now() - start);
            } catch (error) {
              monitor.recordError();
            }
          });
        } else {
          // Event publish
          operations.push(async () => {
            const start = performance.now();
            try {
              await testEvent.publish({
                id: requestId++,
                message: `monitor-event-${requestId}`,
              });
              monitor.recordOperation(performance.now() - start);
            } catch (error) {
              monitor.recordError();
            }
          });
        }
      }

      // Execute batch
      await Promise.all(operations.map(op => op()));

      // Variable delay between batches
      await sleep(Math.random() * 2000 + 500);
    }

  } catch (error) {
    console.error('❌ Performance monitoring failed:', error);
    monitor.stop();
    console.log(monitor.generateReport());
    await hermes.disconnect();
    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  runPerformanceMonitoring().catch(console.error);
}

export { PerformanceMonitor, runPerformanceMonitoring };