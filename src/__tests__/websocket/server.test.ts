/**
 * WebSocket Server Tests
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { WebSocketService } from '../../websocket/server.js';
import WebSocket from 'ws';

// Helper to create a client and wait for connection + welcome message
async function createClient(port: number): Promise<{ client: WebSocket; welcome: any }> {
  const client = new WebSocket(`ws://localhost:${port}`);

  const welcome = await new Promise<any>((resolve, reject) => {
    client.once('message', (data) => {
      resolve(JSON.parse(data.toString()));
    });
    client.once('error', reject);
  });

  return { client, welcome };
}

// Helper to wait for next message
function waitForMessage(client: WebSocket, timeout = 5000): Promise<any> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Timeout waiting for message')), timeout);
    client.once('message', (data) => {
      clearTimeout(timer);
      resolve(JSON.parse(data.toString()));
    });
  });
}

describe('WebSocketService', () => {
  let wsService: WebSocketService;
  const testPort = 19032;

  beforeAll(async () => {
    wsService = new WebSocketService(testPort);
    await wsService.start();
  });

  afterAll(async () => {
    await wsService.stop();
  });

  describe('Connection', () => {
    it('should accept client connection', async () => {
      const { client, welcome } = await createClient(testPort);

      expect(client.readyState).toBe(WebSocket.OPEN);
      expect(welcome).toBeDefined();
      client.close();
    });

    it('should send welcome message on connect', async () => {
      const { client, welcome } = await createClient(testPort);

      expect(welcome.type).toBe('plan_created');
      expect(welcome.data.message).toContain('Connected');
      expect(welcome.timestamp).toBeDefined();
      client.close();
    });

    it('should track client count', async () => {
      // Just verify the count method works and increases
      const { client: client1 } = await createClient(testPort);
      await new Promise(resolve => setTimeout(resolve, 50));
      const countAfterFirst = wsService.getClientCount();
      expect(countAfterFirst).toBeGreaterThan(0);

      const { client: client2 } = await createClient(testPort);
      await new Promise(resolve => setTimeout(resolve, 50));
      const countAfterSecond = wsService.getClientCount();
      expect(countAfterSecond).toBeGreaterThan(countAfterFirst);

      client1.close();
      client2.close();

      // Wait for cleanup
      await new Promise(resolve => setTimeout(resolve, 100));
    });
  });

  describe('Message Handling', () => {
    it('should handle ping message', async () => {
      const { client } = await createClient(testPort);

      // Set up listener BEFORE sending
      const responsePromise = waitForMessage(client);
      client.send(JSON.stringify({ type: 'ping' }));
      const response = await responsePromise;

      expect(response.data.pong).toBe(true);
      client.close();
    });

    it('should handle subscribe_plan message', async () => {
      const { client } = await createClient(testPort);

      const responsePromise = waitForMessage(client);
      client.send(JSON.stringify({ type: 'subscribe_plan', data: 'plan-123' }));
      const response = await responsePromise;

      expect(response.data.subscribed).toBe(true);
      client.close();
    });

    it('should handle unknown message type', async () => {
      const { client } = await createClient(testPort);

      const responsePromise = waitForMessage(client);
      client.send(JSON.stringify({ type: 'unknown_type' }));
      const response = await responsePromise;

      expect(response.type).toBe('error');
      expect(response.data.error).toContain('Unknown message type');
      client.close();
    });
  });

  describe('Broadcast', () => {
    it('should broadcast to all clients', async () => {
      const { client: client1 } = await createClient(testPort);
      const { client: client2 } = await createClient(testPort);

      // Set up listeners BEFORE broadcast
      const promise1 = waitForMessage(client1);
      const promise2 = waitForMessage(client2);

      wsService.broadcast('merge_started', { test: true });

      const [msg1, msg2] = await Promise.all([promise1, promise2]);

      expect(msg1.type).toBe('merge_started');
      expect(msg2.type).toBe('merge_started');

      client1.close();
      client2.close();
    });
  });

  describe('Event Emitters', () => {
    it('emitMergeStarted should broadcast correctly', async () => {
      const { client } = await createClient(testPort);

      const messagePromise = waitForMessage(client);
      wsService.emitMergeStarted('op-123', ['/a.md', '/b.md']);
      const message = await messagePromise;

      expect(message.type).toBe('merge_started');
      expect(message.data.operation_id).toBe('op-123');
      expect(message.data.files_count).toBe(2);

      client.close();
    });

    it('emitMergeComplete should broadcast correctly', async () => {
      const { client } = await createClient(testPort);

      const messagePromise = waitForMessage(client);
      wsService.emitMergeComplete('op-123', '/merged.md');
      const message = await messagePromise;

      expect(message.type).toBe('merge_complete');
      expect(message.data.operation_id).toBe('op-123');
      expect(message.data.output_path).toBe('/merged.md');

      client.close();
    });
  });
});
