/**
 * InterLock UDP Socket
 *
 * Main UDP socket for InterLock mesh communication using shared @bop/interlock package.
 */

import {
  InterlockSocket as SharedInterlockSocket,
  type Signal as SharedSignal,
  type SignalInput,
  type RemoteInfo,
} from '@bop/interlock';
import { SIGNAL_TYPES, getSignalName } from './protocol.js';
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
  private socket: SharedInterlockSocket | null = null;
  private config: InterlockConfig;
  private tumbler: Tumbler;
  private handlers: SignalHandlers;
  private peers: Map<string, PeerInfo> = new Map();
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
    // Convert peers to shared package format
    const peerConfig: Record<string, { host: string; port: number }> = {};
    for (const [name, info] of this.peers) {
      peerConfig[name] = { host: info.host, port: info.port };
    }

    this.socket = new SharedInterlockSocket({
      port: this.config.port,
      serverId: this.config.serverId || 'consolidation-engine',
      heartbeat: {
        interval: this.config.heartbeat?.interval || 30000,
        timeout: this.config.heartbeat?.timeout || 90000,
      },
      peers: peerConfig,
    });

    this.socket.on('signal', (sharedSignal: SharedSignal, rinfo: RemoteInfo) => {
      this.handleMessage(sharedSignal, rinfo);
    });

    this.socket.on('error', (err: Error) => {
      console.error('[InterLock] Socket error:', err.message);
    });

    await this.socket.start();
    this.running = true;
    console.error(`[InterLock] Listening on UDP port ${this.config.port}`);

    // Send discovery to known peers
    this.discover();
  }

  /**
   * Handle incoming message
   */
  private handleMessage(sharedSignal: SharedSignal, rinfo: RemoteInfo): void {
    // Convert to local message format
    const message = {
      type: sharedSignal.type,
      serverId: sharedSignal.data.serverId as string || 'unknown',
      data: sharedSignal.data,
      timestamp: sharedSignal.timestamp,
    };

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
    if (!this.socket) {
      throw new Error('Socket not initialized');
    }

    const signalInput: SignalInput = {
      type: message.type,
      data: {
        serverId: this.config.serverId || 'consolidation-engine',
        ...(message.data as Record<string, unknown> || {}),
      },
    };

    await this.socket.send(host, port, signalInput);
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

    // Send shutdown signal
    await this.broadcast({
      type: SIGNAL_TYPES.SHUTDOWN,
      data: { reason: 'Server stopping' }
    }).catch(() => {});

    if (this.socket) {
      await this.socket.stop();
      this.socket = null;
    }
    console.error('[InterLock] Socket closed');
  }
}
