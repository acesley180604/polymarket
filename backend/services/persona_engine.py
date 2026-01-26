"""
PolySpy Persona Engine
"The Narrative" Strategy - Auto-tag whales with memorable personas

Each whale gets a "character" that tells a story:
- "The Insider" - Knows something we don't
- "The Contrarian" - Bets against the crowd (and wins)
- "The Whale Trap" - Don't follow this guy

This creates emotional hooks and makes the data memorable.
"""

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from decimal import Decimal
from collections import defaultdict

from backend.models.schemas import (
    PersonaTag,
    Transaction,
    TradeHistory,
    TransactionType,
)


class PersonaEngine:
    """
    Auto-assigns persona tags to wallets based on trading behavior patterns.

    Each persona has specific detection criteria based on:
    - Timing patterns (when they trade relative to market moves)
    - Position patterns (what side they take)
    - Outcome patterns (do they win?)
    - Size patterns (how much they bet)
    """

    # Thresholds for persona detection
    INSIDER_MIN_AMOUNT = 5000       # $5k minimum for "Insider" detection
    INSIDER_TIME_WINDOW = 2         # Hours before significant price move
    CONTRARIAN_PRICE_THRESHOLD = 0.20  # Buys when price < 20 cents
    CONTRARIAN_HIGH_THRESHOLD = 0.80   # Or sells when price > 80 cents
    WHALE_TRAP_MIN_VOLUME = 10000   # $10k total volume for "Whale Trap"
    WHALE_TRAP_MAX_WIN_RATE = 30    # Less than 30% win rate

    def __init__(self):
        pass

    def assign_persona(
        self,
        trade_history: TradeHistory,
        market_data: Optional[Dict[str, Any]] = None
    ) -> Optional[PersonaTag]:
        """
        CRITICAL FUNCTION: Analyze trading patterns and assign the best-fit persona.

        Priority Order (first match wins):
        1. The Whale Trap (counter-signal - most important to identify)
        2. The Insider (rare but valuable)
        3. The Contrarian (strong signal)
        4. Diamond Hands / Degen (holding pattern)
        5. The Sniper / Accumulator (position sizing)

        Args:
            trade_history: Complete trade history
            market_data: Optional market context (price history, resolution times)

        Returns:
            PersonaTag if pattern detected, None otherwise
        """
        resolved_trades = trade_history.resolved_trades
        all_trades = trade_history.trades

        # Calculate base metrics
        metrics = self._calculate_behavior_metrics(resolved_trades, all_trades)

        # Check personas in priority order
        # Priority 1: Whale Trap (CRITICAL - counter-signal detection)
        if self._is_whale_trap(metrics):
            return PersonaTag.WHALE_TRAP

        # Priority 2: The Insider
        if self._is_insider(all_trades, market_data):
            return PersonaTag.INSIDER

        # Priority 3: The Contrarian
        if self._is_contrarian(resolved_trades):
            return PersonaTag.CONTRARIAN

        # Priority 4: Holding pattern personas
        holding_persona = self._detect_holding_pattern(all_trades)
        if holding_persona:
            return holding_persona

        # Priority 5: Position sizing personas
        sizing_persona = self._detect_sizing_pattern(resolved_trades)
        if sizing_persona:
            return sizing_persona

        return None

    def _calculate_behavior_metrics(
        self,
        resolved_trades: List[Transaction],
        all_trades: List[Transaction]
    ) -> Dict[str, Any]:
        """Calculate behavioral metrics for persona detection."""

        wins = [t for t in resolved_trades if t.is_profitable]
        losses = [t for t in resolved_trades if t.is_profitable is False]

        total_volume = sum(float(t.amount_usd) for t in all_trades)
        win_rate = (len(wins) / len(resolved_trades) * 100) if resolved_trades else 0

        # Avg position size
        avg_position = total_volume / len(all_trades) if all_trades else 0

        # Buy vs Sell ratio
        buys = [t for t in all_trades if t.type == TransactionType.BUY]
        sells = [t for t in all_trades if t.type in [TransactionType.SELL, TransactionType.EXIT]]

        # Low price buys (contrarian indicator)
        low_price_buys = [t for t in buys if float(t.price) < self.CONTRARIAN_PRICE_THRESHOLD]

        # High price sells (contrarian indicator)
        high_price_sells = [t for t in sells if float(t.price) > self.CONTRARIAN_HIGH_THRESHOLD]

        return {
            "total_volume": total_volume,
            "total_trades": len(all_trades),
            "win_rate": win_rate,
            "avg_position": avg_position,
            "buys_count": len(buys),
            "sells_count": len(sells),
            "low_price_buys": len(low_price_buys),
            "high_price_sells": len(high_price_sells),
            "wins_count": len(wins),
            "losses_count": len(losses),
        }

    def _is_whale_trap(self, metrics: Dict[str, Any]) -> bool:
        """
        Detect "The Whale Trap" - high volume but terrible performance.

        Criteria:
        - Total volume > $10,000
        - Win rate < 30%
        - At least 10 resolved trades

        These wallets are COUNTER-SIGNALS. When they buy, consider selling.
        """
        return (
            metrics["total_volume"] >= self.WHALE_TRAP_MIN_VOLUME and
            metrics["win_rate"] < self.WHALE_TRAP_MAX_WIN_RATE and
            metrics["wins_count"] + metrics["losses_count"] >= 10
        )

    def _is_insider(
        self,
        trades: List[Transaction],
        market_data: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Detect "The Insider" - buys big right before market moves.

        Criteria:
        - Buys > $5k within 2 hours of significant price movement
        - High success rate on these "timed" trades

        This is the holy grail - someone who consistently knows before others.
        """
        if not market_data:
            # Without market data, use simplified heuristic
            # Look for large buys that were immediately profitable
            large_buys = [
                t for t in trades
                if t.type == TransactionType.BUY and float(t.amount_usd) >= self.INSIDER_MIN_AMOUNT
            ]

            if len(large_buys) < 3:
                return False

            # Check if large buys have high win rate
            profitable_large_buys = [t for t in large_buys if t.is_profitable]
            large_buy_win_rate = len(profitable_large_buys) / len(large_buys)

            return large_buy_win_rate >= 0.80  # 80%+ win rate on large buys

        # With market data, check timing relative to price movements
        # This would require price history - placeholder for full implementation
        return False

    def _is_contrarian(self, resolved_trades: List[Transaction]) -> bool:
        """
        Detect "The Contrarian" - bets against the crowd and wins.

        Criteria:
        - Frequently buys when price < 20 cents (everyone thinks it won't happen)
        - OR sells/shorts when price > 80 cents (everyone thinks it will happen)
        - AND wins on these contrarian bets

        These are the smart money - they see what others don't.
        """
        contrarian_trades = []

        for trade in resolved_trades:
            price = float(trade.price)

            # Contrarian BUY: buying cheap "NO" positions or cheap "YES"
            if trade.type == TransactionType.BUY and price < self.CONTRARIAN_PRICE_THRESHOLD:
                contrarian_trades.append(trade)

            # Contrarian SELL: selling expensive positions (taking profit against crowd)
            elif trade.type in [TransactionType.SELL, TransactionType.EXIT]:
                if price > self.CONTRARIAN_HIGH_THRESHOLD:
                    contrarian_trades.append(trade)

        if len(contrarian_trades) < 3:
            return False

        # Check win rate on contrarian trades
        wins = [t for t in contrarian_trades if t.is_profitable]
        contrarian_win_rate = len(wins) / len(contrarian_trades)

        return contrarian_win_rate >= 0.60  # 60%+ win rate on contrarian bets

    def _detect_holding_pattern(
        self,
        trades: List[Transaction]
    ) -> Optional[PersonaTag]:
        """
        Detect holding-based personas.

        - Diamond Hands: Long avg holding period, rarely sells early
        - The Degen: Short holding, high frequency trading
        """
        if not trades:
            return None

        # Group by market to analyze holding patterns
        markets = defaultdict(list)
        for t in trades:
            markets[t.market_slug].append(t)

        # Analyze trade frequency
        if len(trades) >= 2:
            trades_sorted = sorted(trades, key=lambda t: t.timestamp)
            time_diffs = []
            for i in range(1, len(trades_sorted)):
                diff = (trades_sorted[i].timestamp - trades_sorted[i-1].timestamp).total_seconds() / 3600
                time_diffs.append(diff)

            avg_time_between_trades = sum(time_diffs) / len(time_diffs) if time_diffs else 0

            # Degen: trades very frequently (< 6 hours between trades on average)
            if avg_time_between_trades < 6 and len(trades) >= 20:
                return PersonaTag.DEGEN

        # Diamond Hands: Look for markets where they held through resolution
        # (bought early, never sold before resolution)
        for market_slug, market_trades in markets.items():
            buys = [t for t in market_trades if t.type == TransactionType.BUY]
            sells = [t for t in market_trades if t.type in [TransactionType.SELL, TransactionType.EXIT]]

            # If they have many markets with buys but few early exits, they're diamond hands
            if len(buys) > 0 and len(sells) == 0:
                # Held through resolution
                pass

        # For now, return None - would need more data for reliable detection
        return None

    def _detect_sizing_pattern(
        self,
        resolved_trades: List[Transaction]
    ) -> Optional[PersonaTag]:
        """
        Detect position-sizing based personas.

        - The Sniper: Small positions, very high win rate
        - The Accumulator: Gradually builds positions over time
        """
        if len(resolved_trades) < 5:
            return None

        wins = [t for t in resolved_trades if t.is_profitable]
        win_rate = len(wins) / len(resolved_trades)

        avg_size = sum(float(t.amount_usd) for t in resolved_trades) / len(resolved_trades)

        # The Sniper: Small but deadly accurate
        if avg_size < 500 and win_rate >= 0.75 and len(resolved_trades) >= 10:
            return PersonaTag.SNIPER

        # The Accumulator: Multiple buys in same market
        markets = defaultdict(list)
        for t in resolved_trades:
            if t.type == TransactionType.BUY:
                markets[t.market_slug].append(t)

        # Check if they frequently make multiple buys per market
        multi_buy_markets = [m for m, trades in markets.items() if len(trades) >= 3]
        if len(multi_buy_markets) >= 3:
            return PersonaTag.ACCUMULATOR

        return None


# ============================================
# Helper Functions for UI
# ============================================

def get_persona_display_info(persona: PersonaTag) -> Dict[str, Any]:
    """Return display info for persona badges in the UI."""
    persona_info = {
        PersonaTag.INSIDER: {
            "label": "The Insider",
            "color": "purple",
            "bg": "bg-purple-600",
            "border": "border-purple-400",
            "icon": "eye",  # Lucide icon name
            "description": "Knows before others. High-value signal.",
            "signal_strength": 5,  # 1-5 scale
        },
        PersonaTag.CONTRARIAN: {
            "label": "The Contrarian",
            "color": "cyan",
            "bg": "bg-cyan-600",
            "border": "border-cyan-400",
            "icon": "trending-down",
            "description": "Bets against the crowd and wins.",
            "signal_strength": 4,
        },
        PersonaTag.WHALE_TRAP: {
            "label": "Whale Trap",
            "color": "red",
            "bg": "bg-red-600",
            "border": "border-red-400",
            "icon": "alert-triangle",
            "description": "COUNTER-SIGNAL. Do the opposite.",
            "signal_strength": -3,  # Negative = inverse signal
        },
        PersonaTag.DIAMOND_HANDS: {
            "label": "Diamond Hands",
            "color": "blue",
            "bg": "bg-blue-600",
            "border": "border-blue-400",
            "icon": "gem",
            "description": "Long-term holder. Conviction player.",
            "signal_strength": 3,
        },
        PersonaTag.DEGEN: {
            "label": "The Degen",
            "color": "orange",
            "bg": "bg-orange-600",
            "border": "border-orange-400",
            "icon": "zap",
            "description": "High frequency. Noisy signal.",
            "signal_strength": 1,
        },
        PersonaTag.SNIPER: {
            "label": "The Sniper",
            "color": "green",
            "bg": "bg-emerald-600",
            "border": "border-emerald-400",
            "icon": "target",
            "description": "Small bets, deadly accuracy.",
            "signal_strength": 4,
        },
        PersonaTag.ACCUMULATOR: {
            "label": "The Accumulator",
            "color": "yellow",
            "bg": "bg-yellow-600",
            "border": "border-yellow-400",
            "icon": "layers",
            "description": "Builds positions gradually. Conviction.",
            "signal_strength": 3,
        },
    }

    return persona_info.get(persona, {
        "label": str(persona.value) if persona else "Unknown",
        "color": "gray",
        "bg": "bg-gray-600",
        "border": "border-gray-400",
        "icon": "user",
        "description": "No pattern detected.",
        "signal_strength": 0,
    })


def should_inverse_signal(persona: Optional[PersonaTag]) -> bool:
    """Check if this persona's trades should be used as counter-signals."""
    return persona == PersonaTag.WHALE_TRAP
