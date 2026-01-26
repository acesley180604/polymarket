/**
 * PolySpy Frontend Types
 * Matches the backend schema for type safety
 */

export type CreditGrade = 'AAA' | 'AA' | 'A' | 'B' | 'C' | 'F';

export type PersonaTag =
  | 'The Insider'
  | 'The Contrarian'
  | 'The Whale Trap'
  | 'Diamond Hands'
  | 'The Degen'
  | 'The Sniper'
  | 'The Accumulator';

export type SignalType =
  | 'WHALE_BUY'
  | 'WHALE_SELL'
  | 'DUMP_ALERT'
  | 'INSIDER_MOVE'
  | 'CONTRARIAN_BET';

export interface PersonaInfo {
  label: string;
  color: string;
  bg: string;
  border: string;
  icon: string;
  description: string;
  signal_strength: number;
}

export interface Signal {
  id: string;
  type: SignalType;
  wallet_address: string;
  wallet_short: string;
  whale_grade: CreditGrade;
  whale_persona: PersonaTag | null;
  whale_persona_info: PersonaInfo | null;
  whale_win_rate: number;
  market_slug: string;
  market_question: string | null;
  outcome: 'YES' | 'NO';
  amount_usd: number;
  price: number;
  copy_ev_percent: number;
  timestamp: string;
  polymarket_url: string;
  is_dump: boolean;
  is_inverse_signal: boolean;
}

export interface Whale {
  address: string;
  address_short: string;
  credit_grade: CreditGrade;
  credit_score: number;
  persona_tag: PersonaTag | null;
  persona_info: PersonaInfo | null;
  win_rate: number;
  pnl_ratio: number;
  total_trades: number;
  total_profit_usd: number;
  total_volume_usd: number;
  last_active: string | null;
}

export interface DashboardStats {
  total_whales_tracked: number;
  aaa_whales: number;
  signals_24h: number;
  dump_alerts_24h: number;
}

export interface DashboardData {
  signals: Signal[];
  dump_alerts: Signal[];
  top_whales: Whale[];
  stats: DashboardStats;
}

// Grade styling configuration
export const GRADE_STYLES: Record<CreditGrade, {
  bg: string;
  text: string;
  border: string;
  glow: boolean;
  label: string;
}> = {
  AAA: {
    bg: 'bg-gradient-to-r from-yellow-400 to-amber-500',
    text: 'text-black',
    border: 'border-yellow-400',
    glow: true,
    label: 'GOD WHALE',
  },
  AA: {
    bg: 'bg-gradient-to-r from-gray-300 to-gray-400',
    text: 'text-black',
    border: 'border-gray-300',
    glow: true,
    label: 'ELITE',
  },
  A: {
    bg: 'bg-emerald-600',
    text: 'text-white',
    border: 'border-emerald-500',
    glow: false,
    label: 'SMART MONEY',
  },
  B: {
    bg: 'bg-blue-600',
    text: 'text-white',
    border: 'border-blue-500',
    glow: false,
    label: 'AVERAGE',
  },
  C: {
    bg: 'bg-gray-600',
    text: 'text-white',
    border: 'border-gray-500',
    glow: false,
    label: 'GAMBLER',
  },
  F: {
    bg: 'bg-red-600',
    text: 'text-white',
    border: 'border-red-500',
    glow: true,
    label: 'WHALE TRAP',
  },
};

// Persona styling configuration
export const PERSONA_STYLES: Record<PersonaTag, {
  bg: string;
  text: string;
  icon: string;
}> = {
  'The Insider': {
    bg: 'bg-purple-600',
    text: 'text-purple-100',
    icon: 'eye',
  },
  'The Contrarian': {
    bg: 'bg-cyan-600',
    text: 'text-cyan-100',
    icon: 'trending-down',
  },
  'The Whale Trap': {
    bg: 'bg-red-600',
    text: 'text-red-100',
    icon: 'alert-triangle',
  },
  'Diamond Hands': {
    bg: 'bg-blue-600',
    text: 'text-blue-100',
    icon: 'gem',
  },
  'The Degen': {
    bg: 'bg-orange-600',
    text: 'text-orange-100',
    icon: 'zap',
  },
  'The Sniper': {
    bg: 'bg-emerald-600',
    text: 'text-emerald-100',
    icon: 'target',
  },
  'The Accumulator': {
    bg: 'bg-yellow-600',
    text: 'text-yellow-100',
    icon: 'layers',
  },
};
