/**
 * HTTP REST API Server
 *
 * Port: 8032
 * Provides REST endpoints for the Consolidation Engine.
 */

import express, { Request, Response, NextFunction } from 'express';
import { Server } from 'http';
import cors from 'cors';
import rateLimit from 'express-rate-limit';
import { getPlanManager } from '../core/plan-manager.js';
import { getMergeEngine } from '../core/merge-engine.js';
import { getConflictResolver } from '../core/conflict-resolver.js';
import { getDatabase } from '../database/schema.js';

// CORS whitelist - restrict to known origins for security
const ALLOWED_ORIGINS = [
  'http://localhost:5173',   // GMI frontend (Vite)
  'http://127.0.0.1:5173',
  'http://localhost:3099',   // GMI control API
  'http://localhost:8032'    // Self
];

// Rate limiting configuration
const generalLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  limit: 100, // 100 requests per minute
  standardHeaders: 'draft-7',
  legacyHeaders: false,
  message: { error: 'Too many requests, please try again later', retryAfter: '60s' }
});

const mergeLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  limit: 20, // 20 merge operations per minute (conservative for file ops)
  standardHeaders: 'draft-7',
  legacyHeaders: false,
  message: { error: 'Too many merge requests, please try again later', retryAfter: '60s' }
});

export class HttpServer {
  private app: express.Application;
  private server: Server | null = null;
  private port: number;

  constructor(port: number) {
    this.port = port;
    this.app = express();
    this.setupMiddleware();
    this.setupRoutes();
  }

  private setupMiddleware(): void {
    this.app.use(express.json());

    // CORS - Restrict to known origins for security
    this.app.use(cors({
      origin: (origin, callback) => {
        // Allow requests with no origin (same-origin, curl, etc.)
        if (!origin) return callback(null, true);
        if (ALLOWED_ORIGINS.includes(origin)) {
          return callback(null, true);
        }
        callback(null, false);
      },
      methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
      allowedHeaders: ['Content-Type', 'Authorization']
    }));

    // Apply general rate limiting
    this.app.use(generalLimiter);
  }

  private setupRoutes(): void {
    // Health check
    this.app.get('/health', (req: Request, res: Response) => {
      const db = getDatabase();
      const stats = db.getStats();
      res.json({
        status: 'healthy',
        server: 'consolidation-engine',
        port: this.port,
        stats
      });
    });

    // Plan routes
    this.app.post('/api/plans', async (req: Request, res: Response) => {
      try {
        const planManager = getPlanManager();
        const result = await planManager.createPlan(req.body);
        res.json(result);
      } catch (error) {
        res.status(400).json({ error: (error as Error).message });
      }
    });

    this.app.get('/api/plans/:id', (req: Request, res: Response) => {
      try {
        const planManager = getPlanManager();
        const plan = planManager.getPlan(req.params.id);
        if (!plan) {
          res.status(404).json({ error: 'Plan not found' });
          return;
        }
        res.json({
          ...plan,
          clusters: JSON.parse(plan.clusters)
        });
      } catch (error) {
        res.status(500).json({ error: (error as Error).message });
      }
    });

    this.app.get('/api/plans', (req: Request, res: Response) => {
      try {
        const planManager = getPlanManager();
        const status = req.query.status as string | undefined;
        const plans = planManager.listPlans(status);
        res.json({
          plans: plans.map(p => ({
            ...p,
            clusters: JSON.parse(p.clusters)
          }))
        });
      } catch (error) {
        res.status(500).json({ error: (error as Error).message });
      }
    });

    this.app.post('/api/plans/:id/validate', async (req: Request, res: Response) => {
      try {
        const planManager = getPlanManager();
        const result = await planManager.validatePlan(req.params.id);
        res.json(result);
      } catch (error) {
        res.status(400).json({ error: (error as Error).message });
      }
    });

    // Merge routes (stricter rate limit for file ops)
    this.app.post('/api/merge', mergeLimiter, async (req: Request, res: Response) => {
      try {
        const mergeEngine = getMergeEngine();
        const result = await mergeEngine.merge(req.body);
        res.json(result);
      } catch (error) {
        res.status(400).json({ error: (error as Error).message });
      }
    });

    // Conflict routes
    this.app.get('/api/conflicts', (req: Request, res: Response) => {
      try {
        const conflictResolver = getConflictResolver();
        const operationId = req.query.operation_id as string | undefined;
        const conflicts = conflictResolver.listConflicts(operationId);
        res.json({ conflicts });
      } catch (error) {
        res.status(500).json({ error: (error as Error).message });
      }
    });

    this.app.post('/api/conflicts/:id/resolve', async (req: Request, res: Response) => {
      try {
        const conflictResolver = getConflictResolver();
        const result = await conflictResolver.resolve({
          conflict_id: req.params.id,
          ...req.body
        });
        res.json(result);
      } catch (error) {
        res.status(400).json({ error: (error as Error).message });
      }
    });

    // History route
    this.app.get('/api/history', (req: Request, res: Response) => {
      try {
        const mergeEngine = getMergeEngine();
        const limit = parseInt(req.query.limit as string) || 20;
        const filter = (req.query.filter as 'all' | 'successful' | 'failed') || 'all';
        const operations = mergeEngine.getHistory(limit, filter);
        res.json({
          operations: operations.map(op => ({
            ...op,
            source_files: JSON.parse(op.source_files),
            performed_at: new Date(op.performed_at).toISOString()
          }))
        });
      } catch (error) {
        res.status(500).json({ error: (error as Error).message });
      }
    });

    // Stats route
    this.app.get('/api/stats', (req: Request, res: Response) => {
      try {
        const db = getDatabase();
        const conflictResolver = getConflictResolver();
        res.json({
          database: db.getStats(),
          conflicts: conflictResolver.getStats()
        });
      } catch (error) {
        res.status(500).json({ error: (error as Error).message });
      }
    });

    // 404 handler
    this.app.use((req: Request, res: Response) => {
      res.status(404).json({
        error: 'Not found',
        path: req.path,
        method: req.method
      });
    });
  }

  async start(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        this.server = this.app.listen(this.port, () => {
          console.error(`[consolidation-engine] HTTP server listening on port ${this.port}`);
          resolve();
        });

        this.server.on('error', (err) => {
          reject(err);
        });
      } catch (error) {
        reject(error);
      }
    });
  }

  async stop(): Promise<void> {
    return new Promise((resolve) => {
      if (this.server) {
        this.server.close(() => {
          console.error('[consolidation-engine] HTTP server stopped');
          resolve();
        });
      } else {
        resolve();
      }
    });
  }
}
