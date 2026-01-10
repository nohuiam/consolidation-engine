/**
 * HTTP Server Tests
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { HttpServer } from '../../http/server.js';

describe('HttpServer', () => {
  let server: HttpServer;
  const testPort = 18032; // Use different port for testing
  const baseUrl = `http://localhost:${testPort}`;

  beforeAll(async () => {
    server = new HttpServer(testPort);
    await server.start();
  });

  afterAll(async () => {
    await server.stop();
  });

  describe('Health Endpoint', () => {
    it('should return healthy status', async () => {
      const response = await fetch(`${baseUrl}/health`);
      const data = await response.json() as Record<string, unknown>;

      expect(response.status).toBe(200);
      expect(data.status).toBe('healthy');
      expect(data.server).toBe('consolidation-engine');
      expect(data.port).toBe(testPort);
      expect(data.stats).toBeDefined();
    });
  });

  describe('Plan Endpoints', () => {
    it('GET /api/plans should return list', async () => {
      const response = await fetch(`${baseUrl}/api/plans`);
      const data = await response.json() as Record<string, unknown>;

      expect(response.status).toBe(200);
      expect(data.plans).toBeDefined();
      expect(Array.isArray(data.plans)).toBe(true);
    });

    it('GET /api/plans with status filter should work', async () => {
      const response = await fetch(`${baseUrl}/api/plans?status=pending`);
      expect(response.status).toBe(200);
    });

    it('GET /api/plans/:id should return 404 for non-existent', async () => {
      const response = await fetch(`${baseUrl}/api/plans/nonexistent-id`);
      expect(response.status).toBe(404);
    });

    it('POST /api/plans with missing data should return 400', async () => {
      const response = await fetch(`${baseUrl}/api/plans`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });

      expect(response.status).toBe(400);
    });

    it('POST /api/plans/:id/validate for invalid plan should return 400', async () => {
      const response = await fetch(`${baseUrl}/api/plans/invalid-id/validate`, {
        method: 'POST'
      });

      expect(response.status).toBe(400);
    });
  });

  describe('Merge Endpoints', () => {
    it('POST /api/merge with invalid data should return 400', async () => {
      const response = await fetch(`${baseUrl}/api/merge`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ file_paths: [] })
      });

      expect(response.status).toBe(400);
    });

    it('POST /api/merge with missing strategy should return 400', async () => {
      const response = await fetch(`${baseUrl}/api/merge`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ file_paths: ['/a.md', '/b.md'] })
      });

      expect(response.status).toBe(400);
    });
  });

  describe('Conflict Endpoints', () => {
    it('GET /api/conflicts should return list', async () => {
      const response = await fetch(`${baseUrl}/api/conflicts`);
      const data = await response.json() as Record<string, unknown>;

      expect(response.status).toBe(200);
      expect(data.conflicts).toBeDefined();
      expect(Array.isArray(data.conflicts)).toBe(true);
    });

    it('GET /api/conflicts with operation_id filter should work', async () => {
      const response = await fetch(`${baseUrl}/api/conflicts?operation_id=test`);
      expect(response.status).toBe(200);
    });

    it('POST /api/conflicts/:id/resolve for invalid conflict should return 400', async () => {
      const response = await fetch(`${baseUrl}/api/conflicts/invalid-id/resolve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ resolution: 'keep_first' })
      });

      expect(response.status).toBe(400);
    });
  });

  describe('History Endpoint', () => {
    it('GET /api/history should return operations', async () => {
      const response = await fetch(`${baseUrl}/api/history`);
      const data = await response.json() as Record<string, unknown>;

      expect(response.status).toBe(200);
      expect(data.operations).toBeDefined();
      expect(Array.isArray(data.operations)).toBe(true);
    });

    it('GET /api/history with limit should work', async () => {
      const response = await fetch(`${baseUrl}/api/history?limit=5`);
      expect(response.status).toBe(200);
    });

    it('GET /api/history with filter should work', async () => {
      const response = await fetch(`${baseUrl}/api/history?filter=successful`);
      expect(response.status).toBe(200);
    });
  });

  describe('Stats Endpoint', () => {
    it('GET /api/stats should return statistics', async () => {
      const response = await fetch(`${baseUrl}/api/stats`);
      const data = await response.json() as Record<string, unknown>;

      expect(response.status).toBe(200);
      expect(data.database).toBeDefined();
      expect(data.conflicts).toBeDefined();
    });
  });

  describe('CORS', () => {
    it('should return CORS headers for allowed origins', async () => {
      // With origin whitelist, must send an allowed Origin header
      const response = await fetch(`${baseUrl}/health`, {
        headers: { 'Origin': 'http://localhost:5173' }
      });

      expect(response.headers.get('Access-Control-Allow-Origin')).toBe('http://localhost:5173');
    });

    it('should not return CORS headers for disallowed origins', async () => {
      const response = await fetch(`${baseUrl}/health`, {
        headers: { 'Origin': 'http://evil.com' }
      });

      expect(response.headers.get('Access-Control-Allow-Origin')).toBeNull();
    });

    it('OPTIONS preflight should return 204', async () => {
      // cors package returns 204 for successful preflight (standard behavior)
      const response = await fetch(`${baseUrl}/api/plans`, {
        method: 'OPTIONS',
        headers: { 'Origin': 'http://localhost:5173' }
      });

      expect(response.status).toBe(204);
    });
  });
});
