/**
 * Signal Handlers
 *
 * Routes incoming signals to appropriate handlers.
 */

import dgram from 'dgram';
import { SIGNAL_TYPES, getSignalName, type DecodedMessage } from './protocol.js';

export type SignalHandler = (message: DecodedMessage, rinfo: dgram.RemoteInfo) => void;

export class SignalHandlers {
  private handlers: Map<number, SignalHandler[]> = new Map();
  private defaultHandler: SignalHandler | null = null;

  /**
   * Register a handler for a specific signal type
   */
  on(type: number, handler: SignalHandler): void {
    if (!this.handlers.has(type)) {
      this.handlers.set(type, []);
    }
    this.handlers.get(type)!.push(handler);
  }

  /**
   * Register a default handler for unhandled signals
   */
  setDefault(handler: SignalHandler): void {
    this.defaultHandler = handler;
  }

  /**
   * Route a message to appropriate handlers
   */
  route(message: DecodedMessage, rinfo: dgram.RemoteInfo): void {
    const handlers = this.handlers.get(message.type);

    if (handlers && handlers.length > 0) {
      for (const handler of handlers) {
        try {
          handler(message, rinfo);
        } catch (error) {
          console.error(
            `[InterLock] Handler error for ${getSignalName(message.type)}:`,
            (error as Error).message
          );
        }
      }
    } else if (this.defaultHandler) {
      try {
        this.defaultHandler(message, rinfo);
      } catch (error) {
        console.error(
          `[InterLock] Default handler error:`,
          (error as Error).message
        );
      }
    }
  }

  /**
   * Remove all handlers for a signal type
   */
  off(type: number): void {
    this.handlers.delete(type);
  }

  /**
   * Clear all handlers
   */
  clear(): void {
    this.handlers.clear();
    this.defaultHandler = null;
  }
}
