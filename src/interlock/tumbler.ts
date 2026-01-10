/**
 * Tumbler - Signal Whitelist Filter
 *
 * Filters incoming signals based on a configured whitelist.
 */

export class Tumbler {
  private allowedSignals: Set<number>;
  private stats: {
    allowed: number;
    blocked: number;
    byType: Map<number, number>;
  };

  constructor(allowedSignals: string[]) {
    this.allowedSignals = new Set();
    this.stats = {
      allowed: 0,
      blocked: 0,
      byType: new Map()
    };

    // Parse hex strings to numbers
    for (const signal of allowedSignals) {
      if (signal.startsWith('0x')) {
        this.allowedSignals.add(parseInt(signal, 16));
      } else {
        this.allowedSignals.add(parseInt(signal, 10));
      }
    }
  }

  /**
   * Check if a signal type is allowed
   */
  isAllowed(type: number): boolean {
    // If no whitelist configured, allow all
    if (this.allowedSignals.size === 0) {
      this.stats.allowed++;
      this.incrementType(type);
      return true;
    }

    if (this.allowedSignals.has(type)) {
      this.stats.allowed++;
      this.incrementType(type);
      return true;
    }

    this.stats.blocked++;
    return false;
  }

  private incrementType(type: number): void {
    const count = this.stats.byType.get(type) || 0;
    this.stats.byType.set(type, count + 1);
  }

  /**
   * Get tumbler statistics
   */
  getStats(): {
    allowed: number;
    blocked: number;
    byType: Record<string, number>;
  } {
    const byType: Record<string, number> = {};
    for (const [type, count] of this.stats.byType) {
      byType[`0x${type.toString(16).toUpperCase()}`] = count;
    }

    return {
      allowed: this.stats.allowed,
      blocked: this.stats.blocked,
      byType
    };
  }

  /**
   * Reset statistics
   */
  resetStats(): void {
    this.stats = {
      allowed: 0,
      blocked: 0,
      byType: new Map()
    };
  }
}
