/**
 * Test Setup
 *
 * Global setup for vitest tests.
 */

import { beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import { existsSync, mkdirSync, rmSync } from 'fs';
import { join } from 'path';

// Test data directory
export const TEST_DATA_DIR = join(__dirname, '../../test-data');
export const TEST_DB_PATH = join(TEST_DATA_DIR, 'test.db');

// Clean up before all tests
beforeAll(() => {
  // Ensure test data directory exists
  if (!existsSync(TEST_DATA_DIR)) {
    mkdirSync(TEST_DATA_DIR, { recursive: true });
  }
});

// Clean up after all tests
afterAll(() => {
  // Clean up test data directory
  if (existsSync(TEST_DATA_DIR)) {
    try {
      rmSync(TEST_DATA_DIR, { recursive: true, force: true });
    } catch {
      // Ignore cleanup errors
    }
  }
});
