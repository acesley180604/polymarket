'use client';

import React, { useState } from 'react';
import {
  Activity,
  AlertTriangle,
  TrendingUp,
  Users,
  Zap,
  Eye,
  RefreshCw,
} from 'lucide-react';
import { SignalCard, SignalCardSkeleton } from '@/components/dashboard/signal-card';
import { cn, formatUSD } from '@/lib/utils';
import type { Signal, Whale, DashboardStats } from '@/types';

/**
 * PolySpy Dashboard
 *
 * Bloomberg Terminal inspired layout:
 * - Header with stats
 * - Left: Signal feed
 * - Right: Top whales leaderboard
 * - Bottom: Dump alerts (critical)
 */

// Mock data for demo (would come from API in production)
const MOCK_SIGNALS: Signal[] = [
  {
    id: 'signal-1',
    type: 'INSIDER_MOVE',
    wallet_address: '0xAAA1234567890123456789012345678901234567',
    wallet_short: '0xAAA1...4567',
    whale_grade: 'AAA',
    whale_persona: 'The Insider',
    whale_persona_info: null,
    whale_win_rate: 78.5,
    market_slug: 'will-ai-take-jobs-2025',
    market_question: 'Will AI replace >1M US jobs by 2025?',
    outcome: 'YES',
    amount_usd: 35000,
    price: 0.45,
    copy_ev_percent: 122.2,
    timestamp: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(),
    polymarket_url: 'https://polymarket.com/event/will-ai-take-jobs-2025',
    is_dump: false,
    is_inverse_signal: false,
  },
  {
    id: 'signal-2',
    type: 'CONTRARIAN_BET',
    wallet_address: '0xBBB1234567890123456789012345678901234567',
    wallet_short: '0xBBB1...4567',
    whale_grade: 'A',
    whale_persona: 'The Contrarian',
    whale_persona_info: null,
    whale_win_rate: 62.3,
    market_slug: 'will-recession-2024',
    market_question: 'Will there be a US recession in 2024?',
    outcome: 'NO',
    amount_usd: 20000,
    price: 0.85,
    copy_ev_percent: 17.6,
    timestamp: new Date(Date.now() - 5 * 60 * 60 * 1000).toISOString(),
    polymarket_url: 'https://polymarket.com/event/will-recession-2024',
    is_dump: false,
    is_inverse_signal: false,
  },
  {
    id: 'signal-3',
    type: 'WHALE_BUY',
    wallet_address: '0xCCC1234567890123456789012345678901234567',
    wallet_short: '0xCCC1...4567',
    whale_grade: 'AA',
    whale_persona: null,
    whale_persona_info: null,
    whale_win_rate: 71.2,
    market_slug: 'will-btc-150k-2025',
    market_question: 'Will Bitcoin reach $150,000 by end of 2025?',
    outcome: 'YES',
    amount_usd: 45000,
    price: 0.32,
    copy_ev_percent: 212.5,
    timestamp: new Date(Date.now() - 8 * 60 * 60 * 1000).toISOString(),
    polymarket_url: 'https://polymarket.com/event/will-btc-150k-2025',
    is_dump: false,
    is_inverse_signal: false,
  },
];

const MOCK_DUMP_ALERTS: Signal[] = [
  {
    id: 'dump-1',
    type: 'DUMP_ALERT',
    wallet_address: '0xAAA1234567890123456789012345678901234567',
    wallet_short: '0xAAA1...4567',
    whale_grade: 'AAA',
    whale_persona: 'The Insider',
    whale_persona_info: null,
    whale_win_rate: 78.5,
    market_slug: 'will-trump-win-2024',
    market_question: 'Will Trump win the 2024 US Presidential Election?',
    outcome: 'YES',
    amount_usd: 45000,
    price: 0.45,
    copy_ev_percent: 0,
    timestamp: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
    polymarket_url: 'https://polymarket.com/event/will-trump-win-2024',
    is_dump: true,
    is_inverse_signal: false,
  },
];

const MOCK_WHALES: Whale[] = [
  {
    address: '0xAAA1234567890123456789012345678901234567',
    address_short: '0xAAA1...4567',
    credit_grade: 'AAA',
    credit_score: 92,
    persona_tag: 'The Insider',
    persona_info: null,
    win_rate: 78.5,
    pnl_ratio: 3.2,
    total_trades: 45,
    total_profit_usd: 211666,
    total_volume_usd: 280000,
  },
  {
    address: '0xCCC1234567890123456789012345678901234567',
    address_short: '0xCCC1...4567',
    credit_grade: 'AA',
    credit_score: 81,
    persona_tag: null,
    persona_info: null,
    win_rate: 71.2,
    pnl_ratio: 2.4,
    total_trades: 38,
    total_profit_usd: 156000,
    total_volume_usd: 320000,
  },
  {
    address: '0xBBB1234567890123456789012345678901234567',
    address_short: '0xBBB1...4567',
    credit_grade: 'A',
    credit_score: 68,
    persona_tag: 'The Contrarian',
    persona_info: null,
    win_rate: 62.3,
    pnl_ratio: 1.8,
    total_trades: 52,
    total_profit_usd: 87222,
    total_volume_usd: 185000,
  },
];

const MOCK_STATS: DashboardStats = {
  total_whales_tracked: 247,
  aaa_whales: 12,
  signals_24h: 34,
  dump_alerts_24h: 3,
};

/**
 * Stats Card Component
 */
function StatsCard({
  icon: Icon,
  label,
  value,
  trend,
  highlight,
}: {
  icon: React.ElementType;
  label: string;
  value: string | number;
  trend?: string;
  highlight?: boolean;
}) {
  return (
    <div
      className={cn(
        'bg-terminal-card border border-terminal-border rounded-lg p-4',
        highlight && 'border-neon-green'
      )}
    >
      <div className="flex items-center gap-2 mb-2">
        <Icon className={cn('w-4 h-4', highlight ? 'text-neon-green' : 'text-terminal-muted')} />
        <span className="text-xs text-terminal-muted uppercase">{label}</span>
      </div>
      <div className="flex items-baseline gap-2">
        <span className={cn('text-2xl font-mono font-bold', highlight && 'text-neon-green')}>
          {value}
        </span>
        {trend && (
          <span className="text-xs text-neon-green">{trend}</span>
        )}
      </div>
    </div>
  );
}

/**
 * Whale Leaderboard Item
 */
function WhaleRow({ whale, rank }: { whale: Whale; rank: number }) {
  const gradeColors: Record<string, string> = {
    AAA: 'text-grade-aaa',
    AA: 'text-grade-aa',
    A: 'text-grade-a',
    B: 'text-grade-b',
    C: 'text-grade-c',
    F: 'text-grade-f',
  };

  return (
    <div className="flex items-center gap-3 py-2 px-3 hover:bg-terminal-border/30 rounded transition-colors">
      <span className="text-terminal-muted font-mono text-sm w-6">#{rank}</span>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className={cn('font-mono font-bold text-sm', gradeColors[whale.credit_grade])}>
            {whale.credit_grade}
          </span>
          <span className="font-mono text-sm text-terminal-text truncate">
            {whale.address_short}
          </span>
        </div>
        <div className="flex items-center gap-2 text-xs text-terminal-muted">
          <span>{whale.win_rate.toFixed(1)}% WR</span>
          <span>|</span>
          <span>{whale.pnl_ratio.toFixed(1)}x PnL</span>
          {whale.persona_tag && (
            <>
              <span>|</span>
              <span className="text-neon-purple">{whale.persona_tag}</span>
            </>
          )}
        </div>
      </div>
      <span className="font-mono text-sm text-neon-green">
        {formatUSD(whale.total_profit_usd)}
      </span>
    </div>
  );
}

/**
 * Main Dashboard Page
 */
export default function Dashboard() {
  const [isLoading, setIsLoading] = useState(false);

  const handleRefresh = () => {
    setIsLoading(true);
    setTimeout(() => setIsLoading(false), 1000);
  };

  return (
    <div className="min-h-screen p-6">
      {/* Header */}
      <header className="mb-8">
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-neon-green/20 rounded-lg flex items-center justify-center">
              <Eye className="w-6 h-6 text-neon-green" />
            </div>
            <div>
              <h1 className="text-2xl font-bold text-terminal-text">
                Poly<span className="text-neon-green">Spy</span>
              </h1>
              <p className="text-xs text-terminal-muted">Audited Whale Intelligence</p>
            </div>
          </div>

          <button
            onClick={handleRefresh}
            className={cn(
              'flex items-center gap-2 px-4 py-2 rounded-lg',
              'bg-terminal-card border border-terminal-border',
              'hover:border-neon-green hover:text-neon-green transition-colors'
            )}
          >
            <RefreshCw className={cn('w-4 h-4', isLoading && 'animate-spin')} />
            <span className="text-sm">Refresh</span>
          </button>
        </div>

        {/* Stats Row */}
        <div className="grid grid-cols-4 gap-4">
          <StatsCard
            icon={Users}
            label="Whales Tracked"
            value={MOCK_STATS.total_whales_tracked}
          />
          <StatsCard
            icon={Zap}
            label="AAA Whales"
            value={MOCK_STATS.aaa_whales}
            highlight
          />
          <StatsCard
            icon={TrendingUp}
            label="Signals (24h)"
            value={MOCK_STATS.signals_24h}
          />
          <StatsCard
            icon={AlertTriangle}
            label="Dump Alerts"
            value={MOCK_STATS.dump_alerts_24h}
            trend={MOCK_STATS.dump_alerts_24h > 0 ? 'ACTIVE' : undefined}
          />
        </div>
      </header>

      {/* Main Content Grid */}
      <div className="grid grid-cols-3 gap-6">
        {/* Signal Feed (2 columns) */}
        <div className="col-span-2 space-y-4">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold flex items-center gap-2">
              <Activity className="w-5 h-5 text-neon-green" />
              Live Signals
            </h2>
            <span className="text-xs text-terminal-muted font-mono">
              Updated in real-time
            </span>
          </div>

          {/* Dump Alerts Section */}
          {MOCK_DUMP_ALERTS.length > 0 && (
            <div className="mb-6">
              <h3 className="text-sm font-medium text-neon-red flex items-center gap-2 mb-3">
                <AlertTriangle className="w-4 h-4" />
                DUMP ALERTS
              </h3>
              <div className="space-y-3">
                {MOCK_DUMP_ALERTS.map((signal) => (
                  <SignalCard key={signal.id} signal={signal} />
                ))}
              </div>
            </div>
          )}

          {/* Regular Signals */}
          <div className="space-y-3">
            {isLoading ? (
              <>
                <SignalCardSkeleton />
                <SignalCardSkeleton />
                <SignalCardSkeleton />
              </>
            ) : (
              MOCK_SIGNALS.map((signal) => (
                <SignalCard key={signal.id} signal={signal} />
              ))
            )}
          </div>
        </div>

        {/* Right Sidebar: Whale Leaderboard */}
        <div className="col-span-1">
          <div className="bg-terminal-card border border-terminal-border rounded-lg p-4">
            <h2 className="text-lg font-semibold flex items-center gap-2 mb-4">
              <Users className="w-5 h-5 text-grade-aaa" />
              Top Whales
            </h2>

            <div className="space-y-1">
              {MOCK_WHALES.map((whale, index) => (
                <WhaleRow key={whale.address} whale={whale} rank={index + 1} />
              ))}
            </div>

            <div className="mt-4 pt-4 border-t border-terminal-border">
              <button className="w-full py-2 text-sm text-terminal-muted hover:text-neon-green transition-colors">
                View All Whales →
              </button>
            </div>
          </div>

          {/* Legend */}
          <div className="mt-4 bg-terminal-card border border-terminal-border rounded-lg p-4">
            <h3 className="text-sm font-medium text-terminal-muted mb-3">Grade Legend</h3>
            <div className="space-y-2 text-xs">
              <div className="flex items-center gap-2">
                <span className="text-grade-aaa font-mono font-bold">AAA</span>
                <span className="text-terminal-muted">God Whale (&gt;85 score)</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-grade-aa font-mono font-bold">AA</span>
                <span className="text-terminal-muted">Elite (&gt;75 score)</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-grade-a font-mono font-bold">A</span>
                <span className="text-terminal-muted">Smart Money (&gt;60)</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-grade-f font-mono font-bold">F</span>
                <span className="text-terminal-muted">Whale Trap (counter-signal!)</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Footer */}
      <footer className="mt-8 pt-4 border-t border-terminal-border text-center">
        <p className="text-xs text-terminal-muted">
          PolySpy • Audited Intelligence for Polymarket • Not Financial Advice
        </p>
      </footer>
    </div>
  );
}
