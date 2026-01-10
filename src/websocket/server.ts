/**
 * WebSocket Server
 *
 * Port: 9032
 * Provides real-time event broadcasting for merge operations.
 */

import { WebSocketServer, WebSocket } from 'ws';
import { Server } from 'http';

export type EventType =
  | 'plan_created'
  | 'plan_validated'
  | 'merge_started'
  | 'merge_progress'
  | 'merge_complete'
  | 'conflict_detected'
  | 'conflict_resolved'
  | 'error';

export interface WebSocketEvent {
  type: EventType;
  data: unknown;
  timestamp: string;
}

export class WebSocketService {
  private wss: WebSocketServer | null = null;
  private port: number;
  private clients: Set<WebSocket> = new Set();

  constructor(port: number) {
    this.port = port;
  }

  async start(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        this.wss = new WebSocketServer({ port: this.port });

        this.wss.on('connection', (ws: WebSocket) => {
          this.clients.add(ws);
          console.error(`[consolidation-engine] WebSocket client connected (${this.clients.size} total)`);

          // Send welcome message
          this.sendTo(ws, {
            type: 'plan_created',
            data: { message: 'Connected to Consolidation Engine WebSocket' },
            timestamp: new Date().toISOString()
          });

          ws.on('close', () => {
            this.clients.delete(ws);
            console.error(`[consolidation-engine] WebSocket client disconnected (${this.clients.size} remaining)`);
          });

          ws.on('error', (error) => {
            console.error('[consolidation-engine] WebSocket client error:', error.message);
            this.clients.delete(ws);
          });

          // Handle incoming messages
          ws.on('message', (message: Buffer) => {
            try {
              const data = JSON.parse(message.toString());
              this.handleMessage(ws, data);
            } catch {
              this.sendTo(ws, {
                type: 'error',
                data: { error: 'Invalid message format' },
                timestamp: new Date().toISOString()
              });
            }
          });
        });

        this.wss.on('listening', () => {
          console.error(`[consolidation-engine] WebSocket server listening on port ${this.port}`);
          resolve();
        });

        this.wss.on('error', (error) => {
          reject(error);
        });
      } catch (error) {
        reject(error);
      }
    });
  }

  private handleMessage(ws: WebSocket, message: { type: string; data?: unknown }): void {
    switch (message.type) {
      case 'subscribe_plan':
        // Client wants to subscribe to plan updates
        this.sendTo(ws, {
          type: 'plan_created',
          data: { subscribed: true, plan_id: message.data },
          timestamp: new Date().toISOString()
        });
        break;

      case 'ping':
        this.sendTo(ws, {
          type: 'plan_created',
          data: { pong: true },
          timestamp: new Date().toISOString()
        });
        break;

      default:
        this.sendTo(ws, {
          type: 'error',
          data: { error: `Unknown message type: ${message.type}` },
          timestamp: new Date().toISOString()
        });
    }
  }

  private sendTo(ws: WebSocket, event: WebSocketEvent): void {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(event));
    }
  }

  /**
   * Broadcast event to all connected clients
   */
  broadcast(type: EventType, data: unknown): void {
    const event: WebSocketEvent = {
      type,
      data,
      timestamp: new Date().toISOString()
    };

    const message = JSON.stringify(event);
    for (const client of this.clients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    }
  }

  /**
   * Emit plan created event
   */
  emitPlanCreated(planId: string, clusters: unknown): void {
    this.broadcast('plan_created', { plan_id: planId, clusters });
  }

  /**
   * Emit plan validated event
   */
  emitPlanValidated(planId: string, valid: boolean, issues: unknown[]): void {
    this.broadcast('plan_validated', { plan_id: planId, valid, issues_count: issues.length });
  }

  /**
   * Emit merge started event
   */
  emitMergeStarted(operationId: string, files: string[]): void {
    this.broadcast('merge_started', { operation_id: operationId, files_count: files.length });
  }

  /**
   * Emit merge progress event
   */
  emitMergeProgress(operationId: string, progress: number): void {
    this.broadcast('merge_progress', { operation_id: operationId, progress });
  }

  /**
   * Emit merge complete event
   */
  emitMergeComplete(operationId: string, outputPath: string): void {
    this.broadcast('merge_complete', { operation_id: operationId, output_path: outputPath });
  }

  /**
   * Emit conflict detected event
   */
  emitConflictDetected(conflictId: string, type: string, severity: string): void {
    this.broadcast('conflict_detected', { conflict_id: conflictId, type, severity });
  }

  /**
   * Emit conflict resolved event
   */
  emitConflictResolved(conflictId: string, resolution: string): void {
    this.broadcast('conflict_resolved', { conflict_id: conflictId, resolution });
  }

  /**
   * Get number of connected clients
   */
  getClientCount(): number {
    return this.clients.size;
  }

  async stop(): Promise<void> {
    return new Promise((resolve) => {
      if (this.wss) {
        // Close all client connections
        for (const client of this.clients) {
          client.close();
        }
        this.clients.clear();

        this.wss.close(() => {
          console.error('[consolidation-engine] WebSocket server stopped');
          resolve();
        });
      } else {
        resolve();
      }
    });
  }
}
