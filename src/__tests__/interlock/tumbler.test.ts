/**
 * Tumbler Unit Tests
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Tumbler } from '../../interlock/tumbler.js';
import { SIGNAL_TYPES } from '../../interlock/protocol.js';

describe('Tumbler', () => {
  describe('constructor', () => {
    it('should parse hex string signals', () => {
      const tumbler = new Tumbler(['0x01', '0x02', '0x30']);

      expect(tumbler.isAllowed(0x01)).toBe(true);
      expect(tumbler.isAllowed(0x02)).toBe(true);
      expect(tumbler.isAllowed(0x30)).toBe(true);
    });

    it('should parse decimal string signals', () => {
      const tumbler = new Tumbler(['1', '2', '48']);

      expect(tumbler.isAllowed(1)).toBe(true);
      expect(tumbler.isAllowed(2)).toBe(true);
      expect(tumbler.isAllowed(48)).toBe(true);
    });

    it('should handle mixed hex and decimal', () => {
      const tumbler = new Tumbler(['0x01', '2', '0x30']);

      expect(tumbler.isAllowed(1)).toBe(true);
      expect(tumbler.isAllowed(2)).toBe(true);
      expect(tumbler.isAllowed(48)).toBe(true);
    });

    it('should handle empty whitelist', () => {
      const tumbler = new Tumbler([]);

      // Empty whitelist allows all
      expect(tumbler.isAllowed(0x01)).toBe(true);
      expect(tumbler.isAllowed(0xFF)).toBe(true);
    });
  });

  describe('isAllowed', () => {
    let tumbler: Tumbler;

    beforeEach(() => {
      // HEARTBEAT=0x04, DOCK_REQUEST/DISCOVERY=0x01 (per 2026-01-05 protocol)
      tumbler = new Tumbler(['0x04', '0x01']);
    });

    it('should allow whitelisted signals', () => {
      expect(tumbler.isAllowed(SIGNAL_TYPES.HEARTBEAT)).toBe(true);     // 0x04
      expect(tumbler.isAllowed(SIGNAL_TYPES.DISCOVERY)).toBe(true);     // 0x01 (alias)
      expect(tumbler.isAllowed(SIGNAL_TYPES.DOCK_REQUEST)).toBe(true);  // 0x01 (canonical)
    });

    it('should block non-whitelisted signals', () => {
      expect(tumbler.isAllowed(SIGNAL_TYPES.SHUTDOWN)).toBe(false);     // 0x05
      expect(tumbler.isAllowed(SIGNAL_TYPES.MERGE_STARTED)).toBe(false); // 0x31
      expect(tumbler.isAllowed(SIGNAL_TYPES.ERROR)).toBe(false);        // 0xE0
    });

    it('should increment allowed count for allowed signals', () => {
      tumbler.isAllowed(SIGNAL_TYPES.HEARTBEAT);   // 0x04 - allowed
      tumbler.isAllowed(SIGNAL_TYPES.HEARTBEAT);   // 0x04 - allowed
      tumbler.isAllowed(SIGNAL_TYPES.DISCOVERY);   // 0x01 - allowed

      const stats = tumbler.getStats();
      expect(stats.allowed).toBe(3);
    });

    it('should increment blocked count for blocked signals', () => {
      tumbler.isAllowed(SIGNAL_TYPES.SHUTDOWN);
      tumbler.isAllowed(SIGNAL_TYPES.ERROR);

      const stats = tumbler.getStats();
      expect(stats.blocked).toBe(2);
    });

    it('should allow all when whitelist is empty', () => {
      const emptyTumbler = new Tumbler([]);

      expect(emptyTumbler.isAllowed(0x01)).toBe(true);
      expect(emptyTumbler.isAllowed(0x99)).toBe(true);
      expect(emptyTumbler.isAllowed(0xFF)).toBe(true);

      const stats = emptyTumbler.getStats();
      expect(stats.allowed).toBe(3);
      expect(stats.blocked).toBe(0);
    });
  });

  describe('getStats', () => {
    it('should return stats with hex-formatted type keys', () => {
      // HEARTBEAT=0x04, MERGE_PLAN_CREATED=0x30 (per 2026-01-05 protocol)
      const tumbler = new Tumbler(['0x04', '0x30']);

      tumbler.isAllowed(SIGNAL_TYPES.HEARTBEAT); // 0x04
      tumbler.isAllowed(SIGNAL_TYPES.HEARTBEAT);
      tumbler.isAllowed(SIGNAL_TYPES.MERGE_PLAN_CREATED); // 0x30

      const stats = tumbler.getStats();

      expect(stats.byType['0x4']).toBe(2);
      expect(stats.byType['0x30']).toBe(1);
    });

    it('should track allowed and blocked counts', () => {
      const tumbler = new Tumbler(['0x01']);

      tumbler.isAllowed(0x01); // allowed
      tumbler.isAllowed(0x01); // allowed
      tumbler.isAllowed(0x02); // blocked
      tumbler.isAllowed(0x03); // blocked
      tumbler.isAllowed(0x04); // blocked

      const stats = tumbler.getStats();

      expect(stats.allowed).toBe(2);
      expect(stats.blocked).toBe(3);
    });

    it('should return empty byType when no signals processed', () => {
      const tumbler = new Tumbler(['0x01']);
      const stats = tumbler.getStats();

      expect(stats.allowed).toBe(0);
      expect(stats.blocked).toBe(0);
      expect(Object.keys(stats.byType)).toHaveLength(0);
    });
  });

  describe('resetStats', () => {
    it('should reset all statistics', () => {
      const tumbler = new Tumbler(['0x01']);

      tumbler.isAllowed(0x01);
      tumbler.isAllowed(0x01);
      tumbler.isAllowed(0x02);

      let stats = tumbler.getStats();
      expect(stats.allowed).toBe(2);
      expect(stats.blocked).toBe(1);

      tumbler.resetStats();

      stats = tumbler.getStats();
      expect(stats.allowed).toBe(0);
      expect(stats.blocked).toBe(0);
      expect(Object.keys(stats.byType)).toHaveLength(0);
    });

    it('should not affect whitelist after reset', () => {
      const tumbler = new Tumbler(['0x01', '0x02']);

      tumbler.isAllowed(0x01);
      tumbler.resetStats();

      // Whitelist should still work
      expect(tumbler.isAllowed(0x01)).toBe(true);
      expect(tumbler.isAllowed(0x03)).toBe(false);
    });
  });
});
