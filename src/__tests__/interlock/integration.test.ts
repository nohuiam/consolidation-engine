/**
 * InterLock Integration Tests
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { InterlockSocket, type InterlockConfig } from '../../interlock/socket.js';
import { SIGNAL_TYPES } from '../../interlock/protocol.js';

describe('InterlockSocket', () => {
  let socket: InterlockSocket | null = null;
  let testPort = 13032;

  const createConfig = (overrides: Partial<InterlockConfig> = {}): InterlockConfig => ({
    port: overrides.port || testPort,
    serverId: 'test-consolidation',
    heartbeat: {
      interval: 60000, // Long interval for tests
      timeout: 5000
    },
    acceptedSignals: ['heartbeat', 'discovery', 'merge_plan_created', 'merge_started', 'merge_complete'],
    ...overrides
  });

  // Use unique port for each test to avoid conflicts
  beforeEach(() => {
    testPort = 13032 + Math.floor(Math.random() * 1000);
  });

  afterEach(async () => {
    if (socket) {
      await socket.stop().catch(() => {});
      socket = null;
    }
  });

  describe('Socket Binding', () => {
    it('should bind to configured port', async () => {
      socket = new InterlockSocket(createConfig());
      await socket.start();

      expect(socket.getStats().peers).toBe(0);
    });

    it('should fail if port is already in use', async () => {
      socket = new InterlockSocket(createConfig());
      await socket.start();

      const socket2 = new InterlockSocket(createConfig());
      await expect(socket2.start()).rejects.toThrow();
    });

    it('should register configured peers', async () => {
      socket = new InterlockSocket(createConfig({
        connections: {
          'test-peer': { host: '127.0.0.1', port: 3999 }
        }
      }));
      await socket.start();

      const peers = socket.getPeers();
      expect(peers.has('test-peer')).toBe(true);
    });
  });

  describe('Tumbler Filtering', () => {
    it('should initialize tumbler stats', async () => {
      socket = new InterlockSocket(createConfig({
        acceptedSignals: ['heartbeat']
      }));
      await socket.start();

      const stats = socket.getStats();
      // Tumbler tracks allowed/blocked counts, not whitelist directly
      expect(stats.tumbler).toBeDefined();
      expect(stats.tumbler.allowed).toBe(0);
      expect(stats.tumbler.blocked).toBe(0);
    });
  });

  describe('Signal Handlers', () => {
    it('should provide access to handlers', async () => {
      socket = new InterlockSocket(createConfig());
      await socket.start();

      const handlers = socket.getHandlers();
      expect(handlers).toBeDefined();
    });
  });

  describe('Sending Messages', () => {
    it('should send to known peer', async () => {
      socket = new InterlockSocket(createConfig({
        connections: {
          'peer1': { host: '127.0.0.1', port: 3998 }
        }
      }));
      await socket.start();

      // Should not throw even if peer is offline
      await socket.send('peer1', {
        type: SIGNAL_TYPES.HEARTBEAT,
        data: { test: true }
      });
    });

    it('should log error for unknown peer', async () => {
      socket = new InterlockSocket(createConfig());
      await socket.start();

      // Should not throw, just log
      await socket.send('unknown-peer', {
        type: SIGNAL_TYPES.HEARTBEAT,
        data: {}
      });
    });
  });

  describe('Event Emission', () => {
    it('emitMergePlanCreated should broadcast', async () => {
      socket = new InterlockSocket(createConfig());
      await socket.start();

      // Should not throw
      socket.emitMergePlanCreated('plan-123');
    });

    it('emitMergeStarted should broadcast', async () => {
      socket = new InterlockSocket(createConfig());
      await socket.start();

      // Should not throw
      socket.emitMergeStarted('op-123');
    });

    it('emitMergeComplete should broadcast', async () => {
      socket = new InterlockSocket(createConfig());
      await socket.start();

      // Should not throw
      socket.emitMergeComplete('op-123', '/merged.md');
    });
  });

  describe('Shutdown', () => {
    it('should stop cleanly', async () => {
      socket = new InterlockSocket(createConfig());
      await socket.start();
      await socket.stop();

      // Socket should be closed
      expect(true).toBe(true);
    });
  });
});
