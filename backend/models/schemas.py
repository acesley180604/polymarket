"""
PolySpy Data Models
Pydantic schemas for type-safe data handling
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field


class CreditGrade(str, Enum):
    """Whale Credit Rating Grades"""
    AAA = "AAA"  # The God Whale - Top performers
    AA = "AA"    # Elite Smart Money
    A = "A"      # Smart Money
    B = "B"      # Average Trader
    C = "C"      # Gambler / Unproven
    F = "F"      # The Whale Trap (counter-signal)


class PersonaTag(str, Enum):
    """Auto-assigned persona tags based on behavior patterns"""
    INSIDER = "The Insider"           # Buys big before market moves
    CONTRARIAN = "The Contrarian"     # Bets against the crowd and wins
    WHALE_TRAP = "The Whale Trap"     # High volume, low win rate (counter-signal)
    DIAMOND_HANDS = "Diamond Hands"   # Long holding periods
    DEGEN = "The Degen"               # Short-term, high frequency
    SNIPER = "The Sniper"             # Small positions, high win rate
    ACCUMULATOR = "The Accumulator"   # Gradually builds positions


class TransactionType(str, Enum):
    """Types of transactions"""
    BUY = "BUY"
    SELL = "SELL"
    EXIT = "EXIT"      # Full position exit
    REDEEM = "REDEEM"  # Token redemption after resolution


class SignalType(str, Enum):
    """Types of signals for the dashboard"""
    WHALE_BUY = "WHALE_BUY"
    WHALE_SELL = "WHALE_SELL"
    DUMP_ALERT = "DUMP_ALERT"
    INSIDER_MOVE = "INSIDER_MOVE"
    CONTRARIAN_BET = "CONTRARIAN_BET"


# ============================================
# Wallet Models
# ============================================

class WalletMetrics(BaseModel):
    """Calculated metrics for a wallet"""
    win_rate: float = Field(ge=0, le=100, description="Win percentage")
    pnl_ratio: float = Field(ge=0, description="Avg Win / Avg Loss")
    consistency_score: float = Field(ge=0, le=100, description="Lower std dev = higher score")
    avg_holding_hours: float = Field(ge=0, description="Average position duration")
    total_trades: int = Field(ge=0)
    total_wins: int = Field(ge=0)
    total_losses: int = Field(ge=0)
    avg_win_amount: float = Field(ge=0)
    avg_loss_amount: float = Field(ge=0)


class CreditRating(BaseModel):
    """Credit rating result"""
    score: int = Field(ge=0, le=100, description="Numeric score 0-100")
    grade: CreditGrade
    reasoning: str = Field(description="Explanation for the rating")


class Wallet(BaseModel):
    """Full wallet profile"""
    address: str
    alias: Optional[str] = None
    credit_grade: CreditGrade = CreditGrade.C
    credit_score: int = 0
    persona_tag: Optional[PersonaTag] = None
    win_rate: float = 0.0
    pnl_ratio: float = 0.0
    consistency_score: float = 0.0
    avg_holding_hours: float = 0.0
    total_trades: int = 0
    total_wins: int = 0
    total_losses: int = 0
    total_profit_usd: Decimal = Decimal("0.00")
    total_loss_usd: Decimal = Decimal("0.00")
    total_volume_usd: Decimal = Decimal("0.00")
    first_seen_at: Optional[datetime] = None
    last_active_at: Optional[datetime] = None


# ============================================
# Transaction Models
# ============================================

class Transaction(BaseModel):
    """A single trade transaction"""
    id: Optional[str] = None
    wallet_address: str
    market_slug: str
    market_question: Optional[str] = None
    outcome: str  # 'YES' or 'NO'
    type: TransactionType
    amount_usd: Decimal
    shares: Decimal
    price: Decimal = Field(ge=0, le=1, description="Price between 0 and 1")
    entry_price: Optional[Decimal] = None
    potential_roi: Optional[float] = None
    is_dump_alert: bool = False
    is_insider_signal: bool = False
    is_profitable: Optional[bool] = None
    realized_pnl: Optional[Decimal] = None
    tx_hash: Optional[str] = None
    block_number: Optional[int] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class TradeHistory(BaseModel):
    """Trade history for analysis"""
    wallet_address: str
    trades: List[Transaction]
    resolved_trades: List[Transaction] = Field(default_factory=list)


# ============================================
# Signal Models (for Dashboard)
# ============================================

class Signal(BaseModel):
    """A trading signal for the dashboard"""
    id: Optional[str] = None
    transaction_id: Optional[str] = None
    wallet_address: str
    market_slug: str
    market_question: Optional[str] = None
    signal_type: SignalType
    whale_grade: CreditGrade
    whale_persona: Optional[PersonaTag] = None
    whale_win_rate: float = 0.0
    outcome: str
    amount_usd: Decimal
    price: Decimal
    copy_ev_percent: float = Field(description="((1 - price) / price) * 100")
    is_read: bool = False
    is_dismissed: bool = False
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class SignalCard(BaseModel):
    """Data for rendering a signal card in the UI"""
    signal: Signal
    wallet: Wallet
    market_question: str
    time_ago: str  # "2 min ago", "1 hour ago"
    polymarket_url: str  # Direct link to trade
