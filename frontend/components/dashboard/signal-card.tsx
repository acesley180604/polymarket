'use client';

import React from 'react';
import {
  Eye,
  TrendingDown,
  AlertTriangle,
  Gem,
  Zap,
  Target,
  Layers,
  ExternalLink,
  Copy,
  TrendingUp,
} from 'lucide-react';
import { cn, formatUSD, formatPercent, formatTimeAgo } from '@/lib/utils';
import type { Signal, CreditGrade, PersonaTag } from '@/types';
import { GRADE_STYLES, PERSONA_STYLES } from '@/types';

/**
 * SignalCard Component
 *
 * The main UI card displaying whale trading signals.
 * Features:
 * - Credit Grade Badge (AAA with gold glow, etc.)
 * - Persona Tag Pill (The Insider, etc.)
 * - Copy EV Calculation display
 * - Direct trade link to Polymarket
 *
 * Style: Bloomberg Terminal aesthetic - dark mode, neon accents, monospace numbers
 */

interface SignalCardProps {
  signal: Signal;
  onCopyTrade?: (signal: Signal) => void;
}

// Icon mapping for personas
const PERSONA_ICONS: Record<PersonaTag, React.ReactNode> = {
  'The Insider': <Eye className="w-3 h-3" />,
  'The Contrarian': <TrendingDown className="w-3 h-3" />,
  'The Whale Trap': <AlertTriangle className="w-3 h-3" />,
  'Diamond Hands': <Gem className="w-3 h-3" />,
  'The Degen': <Zap className="w-3 h-3" />,
  'The Sniper': <Target className="w-3 h-3" />,
  'The Accumulator': <Layers className="w-3 h-3" />,
};

/**
 * Grade Badge Component
 * Displays the whale's credit rating with appropriate styling
 */
function GradeBadge({ grade }: { grade: CreditGrade }) {
  const style = GRADE_STYLES[grade];

  return (
    <div
      className={cn(
        'inline-flex items-center px-2 py-0.5 rounded text-xs font-bold font-mono',
        style.bg,
        style.text,
        style.glow && 'shadow-lg animate-glow'
      )}
      style={style.glow ? { '--tw-shadow-color': style.border } as React.CSSProperties : {}}
    >
      {grade}
    </div>
  );
}

/**
 * Persona Pill Component
 * Displays the whale's behavioral persona tag
 */
function PersonaPill({ persona }: { persona: PersonaTag }) {
  const style = PERSONA_STYLES[persona];
  const Icon = PERSONA_ICONS[persona];

  return (
    <div
      className={cn(
        'inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium',
        style.bg,
        style.text
      )}
    >
      {Icon}
      <span>{persona}</span>
    </div>
  );
}

/**
 * Copy EV Display Component
 * Shows the potential ROI if copying the trade at current price
 */
function CopyEVDisplay({ ev, isDump }: { ev: number; isDump: boolean }) {
  if (isDump) {
    return (
      <div className="flex flex-col items-end">
        <span className="text-xs text-terminal-muted uppercase">Signal</span>
        <span className="text-lg font-mono font-bold text-neon-red animate-pulse">
          DUMP
        </span>
      </div>
    );
  }

  const isPositive = ev > 0;

  return (
    <div className="flex flex-col items-end">
      <span className="text-xs text-terminal-muted uppercase">Copy EV</span>
      <span
        className={cn(
          'text-lg font-mono font-bold',
          isPositive ? 'text-neon-green' : 'text-neon-red'
        )}
      >
        {formatPercent(ev, 0)}
      </span>
    </div>
  );
}

/**
 * Main Signal Card Component
 */
export function SignalCard({ signal, onCopyTrade }: SignalCardProps) {
  const {
    whale_grade,
    whale_persona,
    whale_win_rate,
    market_question,
    outcome,
    amount_usd,
    price,
    copy_ev_percent,
    timestamp,
    polymarket_url,
    is_dump,
    is_inverse_signal,
    wallet_short,
  } = signal;

  // Card border color based on signal type
  const borderColor = is_dump
    ? 'border-neon-red'
    : is_inverse_signal
    ? 'border-neon-orange'
    : whale_grade === 'AAA'
    ? 'border-grade-aaa'
    : 'border-terminal-border';

  return (
    <div
      className={cn(
        'relative bg-terminal-card rounded-lg border p-4',
        'hover:bg-terminal-card/80 transition-all duration-200',
        borderColor,
        is_dump && 'border-2 animate-pulse-neon'
      )}
    >
      {/* Dump Alert Banner */}
      {is_dump && (
        <div className="absolute -top-3 left-4 px-2 py-0.5 bg-neon-red text-black text-xs font-bold rounded">
          🚨 DUMP ALERT
        </div>
      )}

      {/* Inverse Signal Warning */}
      {is_inverse_signal && !is_dump && (
        <div className="absolute -top-3 left-4 px-2 py-0.5 bg-neon-orange text-black text-xs font-bold rounded">
          ⚠️ COUNTER-SIGNAL
        </div>
      )}

      {/* Header Row: Grade + Persona + Time */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <GradeBadge grade={whale_grade} />
          {whale_persona && <PersonaPill persona={whale_persona} />}
        </div>
        <span className="text-xs text-terminal-muted font-mono">
          {formatTimeAgo(timestamp)}
        </span>
      </div>

      {/* Wallet Info */}
      <div className="flex items-center gap-2 mb-3">
        <span className="text-xs text-terminal-muted">Wallet:</span>
        <span className="font-mono text-sm text-terminal-text">{wallet_short}</span>
        <span className="text-xs text-terminal-muted">|</span>
        <span className="text-xs text-neon-green font-mono">
          {whale_win_rate.toFixed(1)}% Win Rate
        </span>
      </div>

      {/* Market Question */}
      <div className="mb-4">
        <p className="text-sm text-terminal-text line-clamp-2">
          {market_question || signal.market_slug}
        </p>
      </div>

      {/* Trade Details Row */}
      <div className="flex items-end justify-between">
        {/* Left: Position Details */}
        <div className="flex flex-col gap-1">
          {/* Outcome Badge */}
          <div className="flex items-center gap-2">
            <span
              className={cn(
                'px-2 py-1 rounded text-sm font-bold font-mono',
                outcome === 'YES'
                  ? 'bg-neon-green/20 text-neon-green border border-neon-green/50'
                  : 'bg-neon-red/20 text-neon-red border border-neon-red/50'
              )}
            >
              {outcome}
            </span>
            <span className="text-terminal-muted text-xs">
              {is_dump ? 'SOLD' : 'BOUGHT'}
            </span>
          </div>

          {/* Amount and Price */}
          <div className="flex items-baseline gap-3">
            <span className="text-xl font-mono font-bold text-terminal-text">
              {formatUSD(amount_usd)}
            </span>
            <span className="text-sm text-terminal-muted font-mono">
              @ {(price * 100).toFixed(0)}¢
            </span>
          </div>
        </div>

        {/* Right: Copy EV */}
        <CopyEVDisplay ev={copy_ev_percent} isDump={is_dump} />
      </div>

      {/* Action Row */}
      <div className="flex items-center gap-2 mt-4 pt-3 border-t border-terminal-border">
        {/* Trade Button */}
        <a
          href={polymarket_url}
          target="_blank"
          rel="noopener noreferrer"
          className={cn(
            'flex-1 flex items-center justify-center gap-2 px-4 py-2 rounded font-medium text-sm',
            'transition-all duration-200',
            is_dump
              ? 'bg-neon-red/20 text-neon-red hover:bg-neon-red/30 border border-neon-red/50'
              : 'bg-neon-green/20 text-neon-green hover:bg-neon-green/30 border border-neon-green/50'
          )}
        >
          {is_dump ? (
            <>
              <TrendingDown className="w-4 h-4" />
              View Exit
            </>
          ) : (
            <>
              <TrendingUp className="w-4 h-4" />
              Copy Trade
            </>
          )}
          <ExternalLink className="w-3 h-3" />
        </a>

        {/* Copy Signal Button */}
        <button
          onClick={() => onCopyTrade?.(signal)}
          className={cn(
            'p-2 rounded border border-terminal-border',
            'text-terminal-muted hover:text-terminal-text hover:border-terminal-text',
            'transition-all duration-200'
          )}
          title="Copy signal data"
        >
          <Copy className="w-4 h-4" />
        </button>
      </div>
    </div>
  );
}

/**
 * Signal Card Skeleton for loading state
 */
export function SignalCardSkeleton() {
  return (
    <div className="bg-terminal-card rounded-lg border border-terminal-border p-4 animate-pulse">
      <div className="flex items-center gap-2 mb-3">
        <div className="h-5 w-12 bg-terminal-border rounded" />
        <div className="h-5 w-24 bg-terminal-border rounded-full" />
      </div>
      <div className="h-4 w-3/4 bg-terminal-border rounded mb-4" />
      <div className="flex justify-between">
        <div className="h-8 w-24 bg-terminal-border rounded" />
        <div className="h-8 w-16 bg-terminal-border rounded" />
      </div>
    </div>
  );
}

export default SignalCard;
