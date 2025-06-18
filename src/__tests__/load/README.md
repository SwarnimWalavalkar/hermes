# Hermes Load Testing Suite

This directory contains comprehensive load testing tools for the Hermes message bus and RPC service library.

## Overview

The load testing suite evaluates Hermes performance across multiple dimensions:

- **Service Load Testing** - RPC-style request/response patterns
- **Event Load Testing** - Pub/sub message bus patterns  
- **Mixed Workload Testing** - Combined service and event scenarios
- **Stress Testing** - High concurrency and volume scenarios
- **Endurance Testing** - Long-running stability tests
- **Error Recovery Testing** - Failure handling and retry mechanisms

## Test Files

### Core Utilities
- `load-test-utils.ts` - Shared utilities, metrics collection, and test infrastructure

### Standalone Test Scripts
- `services-load-test.ts` - Comprehensive service (RPC) load testing
- `events-load-test.ts` - Event (pub/sub) load testing scenarios
- `mixed-load-test.ts` - Combined workload and stress testing

### Vitest Integration
- `load-test-suite.test.ts` - Vitest-compatible load tests for CI/CD

## Prerequisites

1. **Redis Server** - Hermes requires a running Redis instance
   ```bash
   # Start Redis using Docker
   npm run docker:redis
   # Or use your own Redis instance
   ```

2. **Dependencies** - Install load testing dependencies
   ```bash
   pnpm install
   ```

## Running Load Tests

### Quick Test Suite (Vitest)
```bash
# Run all load tests via Vitest
npm run test:load

# Run specific test patterns
npx vitest --run src/__tests__/load/load-test-suite.test.ts
```

### Comprehensive Standalone Tests
```bash
# Service load testing
npm run load:services

# Event load testing  
npm run load:events

# Mixed workload and stress testing
npm run load:mixed
```

### Manual Execution
```bash
# Run individual test files directly
npx tsx src/__tests__/load/services-load-test.ts
npx tsx src/__tests__/load/events-load-test.ts
npx tsx src/__tests__/load/mixed-load-test.ts
```

## Test Scenarios

### Service Load Tests

1. **Simple Service Tests**
   - Low Load: 100 requests, 5 concurrent
   - Medium Load: 500 requests, 20 concurrent
   - High Load: 1000 requests, 50 concurrent
   - Burst Load: 2000 requests, 100 concurrent

2. **Complex Service Tests**
   - Multi-field request/response schemas
   - Variable processing times
   - Business logic simulation

3. **Error Handling Tests**
   - 30% failure rate simulation
   - Error recovery patterns
   - Timeout scenarios

### Event Load Tests

1. **Basic Event Tests**
   - Multiple volume levels (200-5000 events)
   - Multiple subscribers (2-15 subscribers)
   - Concurrent publishing

2. **Complex Event Tests**
   - Rich event schemas (orders, analytics)
   - Variable processing logic
   - High-value event handling

3. **Multi-Topic Tests**
   - Simultaneous event types
   - Cross-topic performance
   - Resource sharing

4. **Failure Recovery Tests**
   - Subscriber failures
   - Retry mechanisms
   - Dead letter queue testing

### Mixed Workload Tests

1. **Balanced Workloads**
   - 50/50 service/event split
   - Realistic usage patterns
   - Resource contention

2. **Service-Heavy Workloads**
   - 70% services, 30% events
   - RPC-focused scenarios

3. **Event-Heavy Workloads**
   - 30% services, 70% events
   - Message bus focused scenarios

4. **Stress Tests**
   - Maximum concurrency
   - Resource exhaustion
   - Memory pressure

5. **Endurance Tests**
   - 2+ minute duration
   - Stability over time
   - Memory leak detection

## Metrics Collected

### Performance Metrics
- **Throughput** - Operations per second
- **Latency** - Response times (avg, min, max, P95, P99)
- **Error Rate** - Percentage of failed operations
- **Duration** - Total test execution time

### Resource Metrics
- **Memory Usage** - Before, after, and peak memory consumption
- **Memory Growth** - Memory increase over time
- **Connection Pool** - Redis connection utilization

### Business Metrics
- **Processing Rate** - Event processing success rate
- **Recovery Rate** - Error recovery effectiveness
- **Subscriber Efficiency** - Multi-subscriber performance

## Configuration

### Redis Configuration
```typescript
const config = {
  redisOptions: {
    host: process.env.REDIS_HOST || "0.0.0.0",
    port: Number(process.env.REDIS_PORT) || 6379,
    password: process.env.REDIS_PASSWORD || "",
  },
  poolOptions: { min: 0, max: 50 },
  durableName: `load-test-${Date.now()}`,
};
```

### Environment Variables
- `REDIS_HOST` - Redis server hostname (default: 0.0.0.0)
- `REDIS_PORT` - Redis server port (default: 6379)
- `REDIS_PASSWORD` - Redis authentication password

## Interpreting Results

### Good Performance Indicators
- **Throughput** > 50 operations/second
- **Average Latency** < 100ms
- **P95 Latency** < 200ms
- **Error Rate** < 5%
- **Memory Growth** < 50MB over test duration

### Warning Signs
- **High Latency** (>500ms average) - May indicate Redis bottlenecks
- **High Error Rate** (>10%) - Check error handling and retry logic
- **Memory Leaks** - Continuous memory growth over time
- **Low Throughput** (<10 ops/s) - Investigate concurrency limits

### Optimization Recommendations

1. **High Latency**
   - Increase Redis connection pool size
   - Optimize processing logic
   - Consider Redis clustering

2. **High Error Rate**
   - Review error handling patterns
   - Adjust retry configurations
   - Check Redis connectivity

3. **Low Throughput**
   - Increase concurrency levels
   - Optimize Redis configuration
   - Review network latency

4. **Memory Issues**
   - Check for connection leaks
   - Review event acknowledgment patterns
   - Monitor Redis memory usage

## CI/CD Integration

The load tests can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Run Load Tests
  run: |
    docker run -d -p 6379:6379 redis:alpine
    npm run test:load
  env:
    REDIS_HOST: localhost
    REDIS_PORT: 6379
```

## Troubleshooting

### Common Issues

1. **Redis Connection Errors**
   - Ensure Redis is running and accessible
   - Check firewall and network settings
   - Verify Redis authentication

2. **Test Timeouts**
   - Increase test timeout values
   - Reduce test load parameters
   - Check system resources

3. **Memory Errors**
   - Increase Node.js memory limit: `--max-old-space-size=4096`
   - Reduce concurrent operations
   - Monitor system memory

4. **Inconsistent Results**
   - Run tests multiple times
   - Check for background processes
   - Ensure clean Redis state between tests

### Debug Mode

Enable verbose logging:
```bash
DEBUG=hermes:* npm run load:services
```

## Contributing

When adding new load tests:

1. Follow the existing patterns in `load-test-utils.ts`
2. Include comprehensive metrics collection
3. Add appropriate error handling
4. Document test scenarios and expected outcomes
5. Update this README with new test descriptions

## Performance Baselines

These baselines are established on a standard development machine:

- **Simple Services**: >100 req/s, <50ms avg latency
- **Complex Services**: >50 req/s, <100ms avg latency  
- **Event Publishing**: >200 events/s, <25ms avg latency
- **Event Processing**: >150 events/s with acknowledgment
- **Mixed Workload**: >75 ops/s combined throughput

Results may vary based on:
- Hardware specifications
- Redis configuration
- Network latency
- System load