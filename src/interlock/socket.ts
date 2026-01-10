/**
 * InterLock UDP Socket
 *
 * Main UDP socket for InterLock mesh communication.
 */

import dgram from 'dgram';
import { encode, decode, SIGNAL_TYPES, getSignalName } from './protocol.js';
import { Tumbler } from './tumbler.js';
import { SignalHandlers } from './handlers.js';

export interface InterlockConfig {
  port: number;
  serverId?: string;
  heartbeat?: {
    interval: number;
    timeout: number;
  };
  acceptedSignals?: string[];
  connections?: Record<string, { host: string; port: number }>;
}

export interface PeerInfo {
  host: string;
  port: number;
  lastSeen: number;
  heartbeats: number;
}

export class InterlockSocket {
  private socket: dgram.Socket | null = null;
  private config: InterlockConfig;
  private tumbler: Tumbler;
  private handlers: SignalHandlers;
  private peers: Map<string, PeerInfo> = new Map();
  private heartbeatInterval: NodeJS.Timeout | null = null;
  private running: boolean = false;

  constructor(config: InterlockConfig) {
    this.config = config;
    this.tumbler = new Tumbler(config.acceptedSignals || []);
    this.handlers = new SignalHandlers();

    // Register configured peers
    if (config.connections) {
      for (const [name, info] of Object.entries(config.connections)) {
        this.peers.set(name, {
          host: info.host,
          port: info.port,
          lastSeen: 0,
          heartbeats: 0
        });
      }
    }
  }

  /**
   * Get signal handlers for custom handler registration
   */
  getHandlers(): SignalHandlers {
    return this.handlers;
  }

  /**
   * Start the UDP socket
   */
  async start(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        this.socket = dgram.createSocket('udp4');

        this.socket.on('error', (err) => {
          console.error('[InterLock] Socket error:', err.message);
          if (!this.running) {
            reject(err);
          }
        });

        this.socket.on('message', (msg, rinfo) => {
          this.handleMessage(msg, rinfo);
        });

        this.socket.bind(this.config.port, () => {
          this.running = true;
          console.error(`[InterLock] Listening on UDP port ${this.config.port}`);

          // Start heartbeat
          this.startHeartbeat();

          // Send discovery to known peers
          this.discover();

          resolve();
        });
      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Handle incoming message
   */
  private handleMessage(msg: Buffer, rinfo: dgram.RemoteInfo): void {
    const message = decode(msg);
    if (!message) {
      return;
    }

    // Check tumbler whitelist
    if (!this.tumbler.isAllowed(message.type)) {
      return;
    }

    // Update peer last seen
    const existingPeer = this.peers.get(message.serverId);
    if (existingPeer) {
      existingPeer.lastSeen = Date.now();
      existingPeer.heartbeats++;
    }

    // Route to handlers
    this.handlers.route(message, rinfo);
  }

  /**
   * Start heartbeat interval
   */
  private startHeartbeat(): void {
    const interval = this.config.heartbeat?.interval || 30000;

    this.heartbeatInterval = setInterval(() => {
      this.broadcast({
        type: SIGNAL_TYPES.HEARTBEAT,
        data: {
          uptime: process.uptime(),
          peers: this.peers.size
        }
      });
    }, interval);
  }

  /**
   * Send discovery to all known peers
   */
  private discover(): void {
    this.broadcast({
      type: SIGNAL_TYPES.DISCOVERY,
      data: {
        capabilities: [
          'create_merge_plan',
          'validate_plan',
          'merge_documents',
          'detect_conflicts',
          'resolve_conflicts',
          'get_merge_history'
        ],
        version: '0.1.0'
      }
    });
  }

  /**
   * Send message to specific peer
   */
  async send(target: string, message: { type: number; data?: unknown }): Promise<void> {
    const peer = this.peers.get(target);
    if (!peer) {
      console.error(`[InterLock] Unknown peer: ${target}`);
      return;
    }

    return this.sendTo(peer.host, peer.port, message);
  }

  /**
   * Send message to specific address
   */
  async sendTo(host: string, port: number, message: { type: number; data?: unknown }): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.socket) {
        reject(new Error('Socket not initialized'));
        return;
      }

      const buffer = encode({
        type: message.type,
        serverId: this.config.serverId || 'consolidation-engine',
        data: message.data
      });

      this.socket.send(buffer, port, host, (err) => {
        if (err) {
          console.error(`[InterLock] Send error to ${host}:${port}:`, err.message);
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  /**
   * Broadcast message to all known peers
   */
  async broadcast(message: { type: number; data?: unknown }): Promise<void> {
    const promises: Promise<void>[] = [];

    for (const [name, peer] of this.peers) {
      promises.push(
        this.sendTo(peer.host, peer.port, message).catch((err) => {
          console.error(`[InterLock] Broadcast to ${name} failed:`, err.message);
        })
      );
    }

    await Promise.all(promises);
  }

  /**
   * Emit merge plan created signal
   */
  emitMergePlanCreated(planId: string): void {
    this.broadcast({
      type: SIGNAL_TYPES.MERGE_PLAN_CREATED,
      data: { plan_id: planId }
    });
  }

  /**
   * Emit merge started signal
   */
  emitMergeStarted(operationId: string): void {
    this.broadcast({
      type: SIGNAL_TYPES.MERGE_STARTED,
      data: { operation_id: operationId }
    });
  }

  /**
   * Emit merge complete signal
   */
  emitMergeComplete(operationId: string, outputPath: string): void {
    this.broadcast({
      type: SIGNAL_TYPES.MERGE_COMPLETE,
      data: { operation_id: operationId, output_path: outputPath }
    });
  }

  /**
   * Get peer information
   */
  getPeers(): Map<string, PeerInfo> {
    return this.peers;
  }

  /**
   * Get tumbler statistics
   */
  getStats(): { tumbler: ReturnType<Tumbler['getStats']>; peers: number } {
    return {
      tumbler: this.tumbler.getStats(),
      peers: this.peers.size
    };
  }

  /**
   * Stop the socket
   */
  async stop(): Promise<void> {
    this.running = false;

    // Clear heartbeat
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }

    // Send shutdown signal
    await this.broadcast({
      type: SIGNAL_TYPES.SHUTDOWN,
      data: { reason: 'Server stopping' }
    }).catch(() => {});

    return new Promise((resolve) => {
      if (this.socket) {
        this.socket.close(() => {
          console.error('[InterLock] Socket closed');
          resolve();
        });
      } else {
        resolve();
      }
    });
  }
}
