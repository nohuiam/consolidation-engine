/**
 * InterLock Protocol Unit Tests
 */

import { describe, it, expect } from 'vitest';
import {
  SIGNAL_TYPES,
  getSignalName,
  encode,
  decode
} from '../../interlock/protocol.js';

describe('Protocol', () => {
  describe('SIGNAL_TYPES', () => {
    it('should define core signals', () => {
      // Per INTERLOCK-PROTOCOL.barespec.md (2026-01-05 update)
      expect(SIGNAL_TYPES.DOCK_REQUEST).toBe(0x01);
      expect(SIGNAL_TYPES.DOCK_APPROVE).toBe(0x02);
      expect(SIGNAL_TYPES.DOCK_REJECT).toBe(0x03);
      expect(SIGNAL_TYPES.HEARTBEAT).toBe(0x04);
      expect(SIGNAL_TYPES.DISCONNECT).toBe(0x05);
    });

    it('should define legacy aliases', () => {
      // Legacy aliases for backwards compatibility
      expect(SIGNAL_TYPES.DISCOVERY).toBe(SIGNAL_TYPES.DOCK_REQUEST); // 0x01
      expect(SIGNAL_TYPES.SHUTDOWN).toBe(SIGNAL_TYPES.DISCONNECT);    // 0x05
    });

    it('should define merge signals', () => {
      expect(SIGNAL_TYPES.MERGE_PLAN_CREATED).toBe(0x30);
      expect(SIGNAL_TYPES.MERGE_STARTED).toBe(0x31);
      expect(SIGNAL_TYPES.MERGE_COMPLETE).toBe(0x32);
    });

    it('should define error signals', () => {
      expect(SIGNAL_TYPES.ERROR).toBe(0xE0);
      expect(SIGNAL_TYPES.ERROR_CRITICAL).toBe(0xE1);
    });
  });

  describe('getSignalName', () => {
    it('should return name for known signal types', () => {
      expect(getSignalName(SIGNAL_TYPES.HEARTBEAT)).toBe('HEARTBEAT');
      expect(getSignalName(SIGNAL_TYPES.DOCK_REQUEST)).toBe('DOCK_REQUEST');
      expect(getSignalName(SIGNAL_TYPES.MERGE_COMPLETE)).toBe('MERGE_COMPLETE');
      expect(getSignalName(SIGNAL_TYPES.ERROR)).toBe('ERROR');
    });

    it('should return canonical name for aliased signals', () => {
      // DISCOVERY (0x01) is alias for DOCK_REQUEST, returns first match in definition order
      expect(getSignalName(SIGNAL_TYPES.DISCOVERY)).toBe('DOCK_REQUEST');
      // SHUTDOWN (0x05) is alias for DISCONNECT
      expect(getSignalName(SIGNAL_TYPES.SHUTDOWN)).toBe('DISCONNECT');
    });

    it('should return formatted unknown for unrecognized types', () => {
      expect(getSignalName(0xFF)).toBe('UNKNOWN_0xFF');
      expect(getSignalName(0x99)).toBe('UNKNOWN_0x99');
      expect(getSignalName(255)).toBe('UNKNOWN_0xFF');
    });
  });

  describe('encode', () => {
    it('should encode message to buffer', () => {
      const message = {
        type: SIGNAL_TYPES.HEARTBEAT,
        serverId: 'test-server',
        data: { status: 'alive' }
      };

      const buffer = encode(message);

      expect(Buffer.isBuffer(buffer)).toBe(true);
      expect(buffer.length).toBeGreaterThan(0);
    });

    it('should encode without data', () => {
      const message = {
        type: SIGNAL_TYPES.DISCOVERY,
        serverId: 'test-server'
      };

      const buffer = encode(message);
      const decoded = JSON.parse(buffer.toString('utf-8'));

      expect(decoded.d).toEqual({});
    });

    it('should include timestamp', () => {
      const before = Date.now();
      const buffer = encode({
        type: SIGNAL_TYPES.HEARTBEAT,
        serverId: 'test'
      });
      const after = Date.now();

      const decoded = JSON.parse(buffer.toString('utf-8'));

      expect(decoded.ts).toBeGreaterThanOrEqual(before);
      expect(decoded.ts).toBeLessThanOrEqual(after);
    });
  });

  describe('decode', () => {
    it('should decode valid buffer', () => {
      const original = {
        type: SIGNAL_TYPES.MERGE_STARTED,
        serverId: 'consolidation-engine',
        data: { operationId: 'op-123' }
      };

      const buffer = encode(original);
      const decoded = decode(buffer);

      expect(decoded).not.toBeNull();
      expect(decoded!.type).toBe(original.type);
      expect(decoded!.serverId).toBe(original.serverId);
      expect(decoded!.data).toEqual(original.data);
      expect(decoded!.timestamp).toBeDefined();
    });

    it('should return null for invalid JSON', () => {
      const invalidBuffer = Buffer.from('not valid json', 'utf-8');
      const result = decode(invalidBuffer);

      expect(result).toBeNull();
    });

    it('should return null for malformed buffer', () => {
      const malformed = Buffer.from('{ broken json', 'utf-8');
      const result = decode(malformed);

      expect(result).toBeNull();
    });

    it('should return null for empty buffer', () => {
      const empty = Buffer.from('', 'utf-8');
      const result = decode(empty);

      expect(result).toBeNull();
    });

    it('should roundtrip encode/decode correctly', () => {
      const original = {
        type: SIGNAL_TYPES.CONFLICT_DETECTED,
        serverId: 'test-server',
        data: { files: ['/a.md', '/b.md'], severity: 'high' }
      };

      const encoded = encode(original);
      const decoded = decode(encoded);

      expect(decoded!.type).toBe(original.type);
      expect(decoded!.serverId).toBe(original.serverId);
      expect(decoded!.data).toEqual(original.data);
    });
  });
});
