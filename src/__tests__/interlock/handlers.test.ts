/**
 * Signal Handlers Unit Tests
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { SignalHandlers } from '../../interlock/handlers.js';
import { SIGNAL_TYPES, type DecodedMessage } from '../../interlock/protocol.js';
import type dgram from 'dgram';

describe('SignalHandlers', () => {
  let handlers: SignalHandlers;

  const createMessage = (type: number): DecodedMessage => ({
    type,
    serverId: 'test-server',
    data: { test: true },
    timestamp: Date.now()
  });

  const createRinfo = (): dgram.RemoteInfo => ({
    address: '127.0.0.1',
    family: 'IPv4',
    port: 3000,
    size: 100
  });

  beforeEach(() => {
    handlers = new SignalHandlers();
  });

  describe('on', () => {
    it('should register a handler for a signal type', () => {
      const handler = vi.fn();
      handlers.on(SIGNAL_TYPES.HEARTBEAT, handler);

      handlers.route(createMessage(SIGNAL_TYPES.HEARTBEAT), createRinfo());
      expect(handler).toHaveBeenCalledTimes(1);
    });

    it('should register multiple handlers for same signal type', () => {
      const handler1 = vi.fn();
      const handler2 = vi.fn();

      handlers.on(SIGNAL_TYPES.HEARTBEAT, handler1);
      handlers.on(SIGNAL_TYPES.HEARTBEAT, handler2);

      handlers.route(createMessage(SIGNAL_TYPES.HEARTBEAT), createRinfo());

      expect(handler1).toHaveBeenCalledTimes(1);
      expect(handler2).toHaveBeenCalledTimes(1);
    });
  });

  describe('route', () => {
    it('should call registered handler with message and rinfo', () => {
      const handler = vi.fn();
      handlers.on(SIGNAL_TYPES.DISCOVERY, handler);

      const message = createMessage(SIGNAL_TYPES.DISCOVERY);
      const rinfo = createRinfo();

      handlers.route(message, rinfo);

      expect(handler).toHaveBeenCalledWith(message, rinfo);
    });

    it('should not throw when handler errors', () => {
      const errorHandler = vi.fn().mockImplementation(() => {
        throw new Error('Handler failed');
      });
      handlers.on(SIGNAL_TYPES.HEARTBEAT, errorHandler);

      // Should not throw
      expect(() => {
        handlers.route(createMessage(SIGNAL_TYPES.HEARTBEAT), createRinfo());
      }).not.toThrow();

      expect(errorHandler).toHaveBeenCalled();
    });

    it('should continue calling other handlers after one errors', () => {
      const errorHandler = vi.fn().mockImplementation(() => {
        throw new Error('Handler failed');
      });
      const successHandler = vi.fn();

      handlers.on(SIGNAL_TYPES.HEARTBEAT, errorHandler);
      handlers.on(SIGNAL_TYPES.HEARTBEAT, successHandler);

      handlers.route(createMessage(SIGNAL_TYPES.HEARTBEAT), createRinfo());

      expect(errorHandler).toHaveBeenCalled();
      expect(successHandler).toHaveBeenCalled();
    });

    it('should not call handler for unregistered signal type', () => {
      const handler = vi.fn();
      handlers.on(SIGNAL_TYPES.HEARTBEAT, handler);

      handlers.route(createMessage(SIGNAL_TYPES.DISCOVERY), createRinfo());

      expect(handler).not.toHaveBeenCalled();
    });
  });

  describe('setDefault', () => {
    it('should call default handler for unhandled signals', () => {
      const defaultHandler = vi.fn();
      handlers.setDefault(defaultHandler);

      handlers.route(createMessage(SIGNAL_TYPES.SHUTDOWN), createRinfo());

      expect(defaultHandler).toHaveBeenCalled();
    });

    it('should not call default handler when specific handler exists', () => {
      const specificHandler = vi.fn();
      const defaultHandler = vi.fn();

      handlers.on(SIGNAL_TYPES.HEARTBEAT, specificHandler);
      handlers.setDefault(defaultHandler);

      handlers.route(createMessage(SIGNAL_TYPES.HEARTBEAT), createRinfo());

      expect(specificHandler).toHaveBeenCalled();
      expect(defaultHandler).not.toHaveBeenCalled();
    });

    it('should not throw when default handler errors', () => {
      const errorHandler = vi.fn().mockImplementation(() => {
        throw new Error('Default handler failed');
      });
      handlers.setDefault(errorHandler);

      expect(() => {
        handlers.route(createMessage(0xFF), createRinfo()); // Unknown signal type
      }).not.toThrow();
    });
  });

  describe('off', () => {
    it('should remove all handlers for a signal type', () => {
      const handler = vi.fn();
      handlers.on(SIGNAL_TYPES.HEARTBEAT, handler);

      handlers.off(SIGNAL_TYPES.HEARTBEAT);
      handlers.route(createMessage(SIGNAL_TYPES.HEARTBEAT), createRinfo());

      expect(handler).not.toHaveBeenCalled();
    });
  });

  describe('clear', () => {
    it('should remove all handlers', () => {
      const handler1 = vi.fn();
      const handler2 = vi.fn();
      const defaultHandler = vi.fn();

      handlers.on(SIGNAL_TYPES.HEARTBEAT, handler1);
      handlers.on(SIGNAL_TYPES.DISCOVERY, handler2);
      handlers.setDefault(defaultHandler);

      handlers.clear();

      handlers.route(createMessage(SIGNAL_TYPES.HEARTBEAT), createRinfo());
      handlers.route(createMessage(SIGNAL_TYPES.DISCOVERY), createRinfo());
      handlers.route(createMessage(SIGNAL_TYPES.SHUTDOWN), createRinfo());

      expect(handler1).not.toHaveBeenCalled();
      expect(handler2).not.toHaveBeenCalled();
      expect(defaultHandler).not.toHaveBeenCalled();
    });
  });
});
