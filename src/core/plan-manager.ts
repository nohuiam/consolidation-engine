/**
 * Plan Manager
 *
 * Handles merge plan creation and validation from BBB analysis.
 */

import { v4 as uuidv4 } from 'uuid';
import { readFileSync, existsSync } from 'fs';
import { getDatabase, type MergePlan } from '../database/schema.js';

export interface Cluster {
  files: string[];
  similarity_score: number;
  recommended_action: 'merge' | 'review' | 'skip';
}

export interface CreatePlanInput {
  bbb_report_path: string;
  strategy: 'aggressive' | 'conservative' | 'interactive';
}

export interface CreatePlanOutput {
  plan_id: string;
  clusters: Cluster[];
  estimated_savings: string;
}

export interface ValidatePlanOutput {
  valid: boolean;
  issues: Array<{ file: string; issue: string }>;
  files_exist: boolean;
  conflicts_detected: number;
}

export class PlanManager {
  private db = getDatabase();

  /**
   * Create a merge plan from BBB redundancy analysis
   */
  async createPlan(input: CreatePlanInput): Promise<CreatePlanOutput> {
    // Verify BBB report exists
    if (!existsSync(input.bbb_report_path)) {
      throw new Error(`BBB report not found: ${input.bbb_report_path}`);
    }

    // Read and parse BBB report
    const reportContent = readFileSync(input.bbb_report_path, 'utf-8');
    let report: {
      clusters?: Array<{
        files: string[];
        similarity: number;
      }>;
      duplicates?: Array<{
        files: string[];
        similarity: number;
      }>;
      total_redundancy_bytes?: number;
    };

    try {
      report = JSON.parse(reportContent);
    } catch {
      // Try to parse as text-based report
      report = this.parseTextReport(reportContent);
    }

    // Extract clusters from report
    const rawClusters = report.clusters || report.duplicates || [];

    // Apply strategy to determine recommended actions
    const clusters: Cluster[] = rawClusters.map(cluster => ({
      files: cluster.files,
      similarity_score: cluster.similarity || 0,
      recommended_action: this.getRecommendedAction(cluster.similarity || 0, input.strategy)
    }));

    // Calculate estimated savings
    const estimatedBytes = report.total_redundancy_bytes || this.estimateSavings(clusters);
    const estimatedSavings = this.formatBytes(estimatedBytes);

    // Generate plan ID and store in database
    const planId = uuidv4();
    const plan: MergePlan = {
      id: planId,
      bbb_report_path: input.bbb_report_path,
      strategy: input.strategy,
      clusters: JSON.stringify(clusters),
      estimated_savings: estimatedSavings,
      status: 'pending',
      created_at: Date.now()
    };

    this.db.insertPlan(plan);

    return {
      plan_id: planId,
      clusters,
      estimated_savings: estimatedSavings
    };
  }

  /**
   * Validate merge plan before execution
   */
  async validatePlan(planId: string): Promise<ValidatePlanOutput> {
    const plan = this.db.getPlan(planId);
    if (!plan) {
      throw new Error(`Plan not found: ${planId}`);
    }

    const clusters: Cluster[] = JSON.parse(plan.clusters);
    const issues: Array<{ file: string; issue: string }> = [];
    let allFilesExist = true;
    let conflictsDetected = 0;

    // Check each cluster
    for (const cluster of clusters) {
      if (cluster.recommended_action === 'skip') {
        continue;
      }

      // Check file existence
      for (const file of cluster.files) {
        if (!existsSync(file)) {
          issues.push({ file, issue: 'File does not exist' });
          allFilesExist = false;
        }
      }

      // Check for potential conflicts (files modified recently)
      const fileConflicts = await this.checkFileConflicts(cluster.files);
      if (fileConflicts.length > 0) {
        conflictsDetected += fileConflicts.length;
        for (const conflict of fileConflicts) {
          issues.push({ file: conflict.file, issue: conflict.reason });
        }
      }
    }

    // Update plan status
    if (issues.length === 0) {
      this.db.updatePlanStatus(planId, 'validated');
    }

    return {
      valid: issues.length === 0,
      issues,
      files_exist: allFilesExist,
      conflicts_detected: conflictsDetected
    };
  }

  /**
   * Get plan by ID
   */
  getPlan(planId: string): MergePlan | undefined {
    return this.db.getPlan(planId);
  }

  /**
   * List all plans
   */
  listPlans(status?: string): MergePlan[] {
    return this.db.listPlans(status);
  }

  /**
   * Determine recommended action based on similarity and strategy
   */
  private getRecommendedAction(
    similarity: number,
    strategy: 'aggressive' | 'conservative' | 'interactive'
  ): 'merge' | 'review' | 'skip' {
    switch (strategy) {
      case 'aggressive':
        if (similarity >= 0.7) return 'merge';
        if (similarity >= 0.5) return 'review';
        return 'skip';

      case 'conservative':
        if (similarity >= 0.95) return 'merge';
        if (similarity >= 0.8) return 'review';
        return 'skip';

      case 'interactive':
      default:
        // Always review in interactive mode
        if (similarity >= 0.6) return 'review';
        return 'skip';
    }
  }

  /**
   * Parse text-based BBB report
   */
  private parseTextReport(content: string): {
    clusters: Array<{ files: string[]; similarity: number }>;
    total_redundancy_bytes: number;
  } {
    // Simple text parsing - look for file groups
    const clusters: Array<{ files: string[]; similarity: number }> = [];
    const lines = content.split('\n');
    let currentCluster: string[] = [];
    let currentSimilarity = 0;

    for (const line of lines) {
      // Look for file paths
      if (line.includes('/') && line.trim().endsWith('.md')) {
        currentCluster.push(line.trim());
      }
      // Look for similarity percentage
      const similarityMatch = line.match(/(\d+(?:\.\d+)?)\s*%/);
      if (similarityMatch) {
        currentSimilarity = parseFloat(similarityMatch[1]) / 100;
      }
      // Empty line signals end of cluster
      if (line.trim() === '' && currentCluster.length > 1) {
        clusters.push({
          files: [...currentCluster],
          similarity: currentSimilarity || 0.8
        });
        currentCluster = [];
        currentSimilarity = 0;
      }
    }

    // Don't forget last cluster
    if (currentCluster.length > 1) {
      clusters.push({
        files: currentCluster,
        similarity: currentSimilarity || 0.8
      });
    }

    return {
      clusters,
      total_redundancy_bytes: 0
    };
  }

  /**
   * Estimate savings from clusters
   */
  private estimateSavings(clusters: Cluster[]): number {
    // Rough estimate: assume each file is ~5KB and we save 50% of duplicates
    let totalFiles = 0;
    for (const cluster of clusters) {
      if (cluster.recommended_action !== 'skip') {
        totalFiles += cluster.files.length - 1; // Keep one file per cluster
      }
    }
    return totalFiles * 5000 * 0.5;
  }

  /**
   * Format bytes to human-readable string
   */
  private formatBytes(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }

  /**
   * Check for potential file conflicts
   */
  private async checkFileConflicts(files: string[]): Promise<Array<{ file: string; reason: string }>> {
    const conflicts: Array<{ file: string; reason: string }> = [];

    for (const file of files) {
      try {
        if (!existsSync(file)) {
          conflicts.push({ file, reason: 'File does not exist' });
          continue;
        }

        // Check if file is readable
        try {
          readFileSync(file, 'utf-8');
        } catch {
          conflicts.push({ file, reason: 'File is not readable' });
        }
      } catch (error) {
        conflicts.push({ file, reason: `Error checking file: ${(error as Error).message}` });
      }
    }

    return conflicts;
  }
}

// Singleton instance
let planManagerInstance: PlanManager | null = null;

export function getPlanManager(): PlanManager {
  if (!planManagerInstance) {
    planManagerInstance = new PlanManager();
  }
  return planManagerInstance;
}
