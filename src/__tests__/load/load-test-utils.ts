import { performance } from 'perf_hooks';
import { Hermes, IHermes } from '../../index';

export interface LoadTestConfig {
  durableName: string;
  redisOptions: {
    host: string;
    port: number;
    password: string;
  };
  poolOptions: {
    min: number;
    max: number;
  };
}

export interface LoadTestMetrics {
  totalRequests: number;
  successfulRequests: number;
  failedRequests: number;
  averageLatency: number;
  minLatency: number;
  maxLatency: number;
  p95Latency: number;
  p99Latency: number;
  throughput: number; // requests per second
  duration: number; // total test duration in ms
  errorRate: number; // percentage
}

export interface LoadTestResult {
  testName: string;
  config: any;
  metrics: LoadTestMetrics;
  errors: string[];
  memoryUsage: {
    before: NodeJS.MemoryUsage;
    after: NodeJS.MemoryUsage;
    peak: NodeJS.MemoryUsage;
  };
}

export class LoadTestRunner {
  private latencies: number[] = [];
  private errors: string[] = [];
  private startTime: number = 0;
  private endTime: number = 0;
  private memoryBefore: NodeJS.MemoryUsage;
  private memoryAfter: NodeJS.MemoryUsage;
  private memoryPeak: NodeJS.MemoryUsage;

  constructor() {
    this.memoryBefore = process.memoryUsage();
    this.memoryPeak = { ...this.memoryBefore };
  }

  start() {
    this.startTime = performance.now();
    this.memoryBefore = process.memoryUsage();
  }

  end() {
    this.endTime = performance.now();
    this.memoryAfter = process.memoryUsage();
  }

  recordLatency(latency: number) {
    this.latencies.push(latency);
    this.updatePeakMemory();
  }

  recordError(error: string) {
    this.errors.push(error);
    this.updatePeakMemory();
  }

  private updatePeakMemory() {
    const current = process.memoryUsage();
    if (current.heapUsed > this.memoryPeak.heapUsed) {
      this.memoryPeak = current;
    }
  }

  getResults(testName: string, config: any): LoadTestResult {
    const duration = this.endTime - this.startTime;
    const totalRequests = this.latencies.length + this.errors.length;
    const successfulRequests = this.latencies.length;
    const failedRequests = this.errors.length;

    this.latencies.sort((a, b) => a - b);

    const averageLatency = this.latencies.length > 0 
      ? this.latencies.reduce((sum, lat) => sum + lat, 0) / this.latencies.length 
      : 0;

    const minLatency = this.latencies.length > 0 ? this.latencies[0] : 0;
    const maxLatency = this.latencies.length > 0 ? this.latencies[this.latencies.length - 1] : 0;

    const p95Index = Math.floor(this.latencies.length * 0.95);
    const p99Index = Math.floor(this.latencies.length * 0.99);
    const p95Latency = this.latencies.length > 0 ? this.latencies[p95Index] || 0 : 0;
    const p99Latency = this.latencies.length > 0 ? this.latencies[p99Index] || 0 : 0;

    const throughput = totalRequests / (duration / 1000); // requests per second
    const errorRate = totalRequests > 0 ? (failedRequests / totalRequests) * 100 : 0;

    return {
      testName,
      config,
      metrics: {
        totalRequests,
        successfulRequests,
        failedRequests,
        averageLatency,
        minLatency,
        maxLatency,
        p95Latency,
        p99Latency,
        throughput,
        duration,
        errorRate,
      },
      errors: [...this.errors],
      memoryUsage: {
        before: this.memoryBefore,
        after: this.memoryAfter,
        peak: this.memoryPeak,
      },
    };
  }
}

export function printResults(result: LoadTestResult) {
  console.log(`\n=== ${result.testName} ===`);
  console.log(`Duration: ${result.metrics.duration.toFixed(2)}ms`);
  console.log(`Total Requests: ${result.metrics.totalRequests}`);
  console.log(`Successful: ${result.metrics.successfulRequests}`);
  console.log(`Failed: ${result.metrics.failedRequests}`);
  console.log(`Error Rate: ${result.metrics.errorRate.toFixed(2)}%`);
  console.log(`Throughput: ${result.metrics.throughput.toFixed(2)} req/s`);
  console.log(`\nLatency:`);
  console.log(`  Average: ${result.metrics.averageLatency.toFixed(2)}ms`);
  console.log(`  Min: ${result.metrics.minLatency.toFixed(2)}ms`);
  console.log(`  Max: ${result.metrics.maxLatency.toFixed(2)}ms`);
  console.log(`  P95: ${result.metrics.p95Latency.toFixed(2)}ms`);
  console.log(`  P99: ${result.metrics.p99Latency.toFixed(2)}ms`);
  
  console.log(`\nMemory Usage:`);
  console.log(`  Before: ${(result.memoryUsage.before.heapUsed / 1024 / 1024).toFixed(2)}MB`);
  console.log(`  After: ${(result.memoryUsage.after.heapUsed / 1024 / 1024).toFixed(2)}MB`);
  console.log(`  Peak: ${(result.memoryUsage.peak.heapUsed / 1024 / 1024).toFixed(2)}MB`);
  
  if (result.errors.length > 0) {
    console.log(`\nFirst 5 Errors:`);
    result.errors.slice(0, 5).forEach((error, index) => {
      console.log(`  ${index + 1}. ${error}`);
    });
  }
}

export async function createHermesInstance(config: LoadTestConfig): Promise<IHermes> {
  return await Hermes(config).connect();
}

export function getDefaultConfig(): LoadTestConfig {
  return {
    durableName: `load-test-${Date.now()}`,
    redisOptions: {
      host: process.env.REDIS_HOST || "0.0.0.0",
      port: Number(process.env.REDIS_PORT) || 6379,
      password: process.env.REDIS_PASSWORD || "",
    },
    poolOptions: { min: 0, max: 50 },
  };
}

export async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export async function runConcurrent<T>(
  tasks: (() => Promise<T>)[],
  concurrency: number = 10
): Promise<T[]> {
  const results: T[] = [];
  const executing: Promise<void>[] = [];

  for (const task of tasks) {
    const promise = task().then(result => {
      results.push(result);
    });
    
    executing.push(promise);

    if (executing.length >= concurrency) {
      await Promise.race(executing);
      executing.splice(executing.findIndex(p => p === promise), 1);
    }
  }

  await Promise.all(executing);
  return results;
}