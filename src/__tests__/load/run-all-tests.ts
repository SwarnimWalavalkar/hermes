#!/usr/bin/env tsx

import { runServicesLoadTest } from './services-load-test';
import { runEventsLoadTest } from './events-load-test';
import { runMixedLoadTest } from './mixed-load-test';

async function runAllLoadTests() {
  console.log('🚀 Starting Complete Hermes Load Testing Suite');
  console.log('================================================');
  console.log('This will run comprehensive load tests for:');
  console.log('- Services (RPC patterns)');
  console.log('- Events (Pub/Sub patterns)');
  console.log('- Mixed workloads');
  console.log('- Stress testing');
  console.log('- Endurance testing');
  console.log('');
  console.log('⚠️  Make sure Redis is running before proceeding!');
  console.log('   Run: npm run docker:redis');
  console.log('');

  const startTime = Date.now();

  try {
    // Run services load tests
    console.log('\n🔧 PHASE 1: Services Load Testing');
    console.log('==================================');
    await runServicesLoadTest();

    console.log('\n⏱️  Waiting 5 seconds before next phase...');
    await new Promise(resolve => setTimeout(resolve, 5000));

    // Run events load tests
    console.log('\n📡 PHASE 2: Events Load Testing');
    console.log('===============================');
    await runEventsLoadTest();

    console.log('\n⏱️  Waiting 5 seconds before next phase...');
    await new Promise(resolve => setTimeout(resolve, 5000));

    // Run mixed load tests
    console.log('\n🔀 PHASE 3: Mixed Workload & Stress Testing');
    console.log('===========================================');
    await runMixedLoadTest();

    const totalDuration = Date.now() - startTime;
    const minutes = Math.floor(totalDuration / 60000);
    const seconds = Math.floor((totalDuration % 60000) / 1000);

    console.log('\n✅ ALL LOAD TESTS COMPLETED SUCCESSFULLY!');
    console.log('==========================================');
    console.log(`Total Duration: ${minutes}m ${seconds}s`);
    console.log('');
    console.log('📊 Summary:');
    console.log('- Services: Tested RPC patterns under various loads');
    console.log('- Events: Tested pub/sub patterns with multiple subscribers');
    console.log('- Mixed: Tested realistic combined workloads');
    console.log('- Stress: Tested system limits and recovery');
    console.log('- Endurance: Tested long-running stability');
    console.log('');
    console.log('🎯 Next Steps:');
    console.log('- Review individual test outputs above');
    console.log('- Check for any performance warnings');
    console.log('- Consider running individual tests for deeper analysis');
    console.log('- Use results to optimize your Hermes configuration');

  } catch (error) {
    console.error('\n❌ Load testing suite failed:', error);
    console.error('');
    console.error('🔍 Troubleshooting:');
    console.error('- Ensure Redis is running and accessible');
    console.error('- Check available system memory');
    console.error('- Verify network connectivity');
    console.error('- Review error messages above');
    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  runAllLoadTests().catch(console.error);
}

export { runAllLoadTests };