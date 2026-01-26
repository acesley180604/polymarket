"""
PolySpy Credit Rating Service
"The Audited Whale" Strategy - Apply accounting metrics to whale analysis

This is the CORE differentiator - treating whale analysis like credit rating agencies
treat bonds. We audit performance, not just track volume.
"""

import statistics
from typing import List, Tuple
from decimal import Decimal

from backend.models.schemas import (
    CreditGrade,
    CreditRating,
    WalletMetrics,
    Transaction,
    TradeHistory,
)


class CreditRatingService:
    """
    Calculates "Whale Credit Scores" using accounting-style metrics.

    Metrics Used:
    1. Win Rate: % of profitable trades (consistency)
    2. PnL Ratio: Avg Win / Avg Loss (risk management)
    3. Consistency: Std deviation of returns (predictability)
    4. Volume-Adjusted Score: Bonus for high-volume performers

    Grade Thresholds (Inspired by S&P/Moody's):
    - AAA: Score >= 85 (The God Whale)
    - AA:  Score >= 75 (Elite Smart Money)
    - A:   Score >= 60 (Smart Money)
    - B:   Score >= 40 (Average Trader)
    - C:   Score >= 20 (Gambler)
    - F:   Score < 20  (The Whale Trap - use as counter-signal)
    """

    # Grade thresholds
    GRADE_THRESHOLDS = {
        CreditGrade.AAA: 85,
        CreditGrade.AA: 75,
        CreditGrade.A: 60,
        CreditGrade.B: 40,
        CreditGrade.C: 20,
        CreditGrade.F: 0,
    }

    # Minimum trades required for reliable rating
    MIN_TRADES_FOR_RATING = 5

    def __init__(self):
        pass

    def calculate_credit_grade(self, trade_history: TradeHistory) -> CreditRating:
        """
        CRITICAL FUNCTION: Calculate credit grade from trade history.

        This is the "secret sauce" - applying rigorous accounting analysis
        to whale trading behavior.

        Args:
            trade_history: Complete trade history for a wallet

        Returns:
            CreditRating with score (0-100), grade (AAA-F), and reasoning
        """
        resolved_trades = trade_history.resolved_trades

        # Edge case: Not enough data
        if len(resolved_trades) < self.MIN_TRADES_FOR_RATING:
            return CreditRating(
                score=0,
                grade=CreditGrade.C,
                reasoning=f"Insufficient data: {len(resolved_trades)} trades "
                          f"(minimum {self.MIN_TRADES_FOR_RATING} required)"
            )

        # Calculate core metrics
        metrics = self._calculate_metrics(resolved_trades)

        # Calculate composite score (0-100)
        score, score_breakdown = self._calculate_composite_score(metrics, resolved_trades)

        # Determine grade from score
        grade = self._score_to_grade(score)

        # Generate reasoning
        reasoning = self._generate_reasoning(metrics, score_breakdown, grade)

        return CreditRating(
            score=score,
            grade=grade,
            reasoning=reasoning
        )

    def _calculate_metrics(self, trades: List[Transaction]) -> WalletMetrics:
        """Calculate all performance metrics from resolved trades."""

        wins = [t for t in trades if t.is_profitable]
        losses = [t for t in trades if t.is_profitable is False]

        # Win Rate (0-100%)
        win_rate = (len(wins) / len(trades)) * 100 if trades else 0

        # PnL Calculations
        win_amounts = [float(t.realized_pnl or 0) for t in wins if t.realized_pnl]
        loss_amounts = [abs(float(t.realized_pnl or 0)) for t in losses if t.realized_pnl]

        avg_win = statistics.mean(win_amounts) if win_amounts else 0
        avg_loss = statistics.mean(loss_amounts) if loss_amounts else 0

        # PnL Ratio (Risk/Reward)
        # Higher is better: means your wins are bigger than losses
        pnl_ratio = avg_win / avg_loss if avg_loss > 0 else (10.0 if avg_win > 0 else 0)

        # Consistency Score (based on return std dev)
        # Lower variance = more predictable = higher score
        all_returns = [float(t.realized_pnl or 0) for t in trades if t.realized_pnl]
        if len(all_returns) >= 2:
            returns_std = statistics.stdev(all_returns)
            avg_return = statistics.mean(all_returns)
            # Coefficient of variation - normalize by mean
            cv = returns_std / abs(avg_return) if avg_return != 0 else float('inf')
            # Convert to 0-100 score (lower CV = higher score)
            consistency_score = max(0, min(100, 100 - (cv * 20)))
        else:
            consistency_score = 50  # Neutral if insufficient data

        # Holding duration (placeholder - would come from position tracking)
        avg_holding_hours = 0.0  # TODO: Calculate from entry/exit timestamps

        return WalletMetrics(
            win_rate=win_rate,
            pnl_ratio=pnl_ratio,
            consistency_score=consistency_score,
            avg_holding_hours=avg_holding_hours,
            total_trades=len(trades),
            total_wins=len(wins),
            total_losses=len(losses),
            avg_win_amount=avg_win,
            avg_loss_amount=avg_loss,
        )

    def _calculate_composite_score(
        self,
        metrics: WalletMetrics,
        trades: List[Transaction]
    ) -> Tuple[int, dict]:
        """
        Calculate weighted composite score (0-100).

        Weighting Philosophy:
        - Win Rate: 35% (most important - are they right?)
        - PnL Ratio: 30% (are they managing risk well?)
        - Consistency: 20% (are they predictable?)
        - Volume Bonus: 15% (high volume validates the pattern)
        """
        breakdown = {}

        # 1. Win Rate Component (35 points max)
        # Scale: 50% = 0 points, 100% = 35 points
        win_rate_score = max(0, (metrics.win_rate - 50) * 0.7)  # 50%+ gives points
        breakdown['win_rate'] = min(35, win_rate_score)

        # 2. PnL Ratio Component (30 points max)
        # Scale: 1.0 = 0 points, 3.0+ = 30 points
        pnl_score = min(30, max(0, (metrics.pnl_ratio - 1.0) * 15))
        breakdown['pnl_ratio'] = pnl_score

        # 3. Consistency Component (20 points max)
        # Direct from consistency score
        breakdown['consistency'] = metrics.consistency_score * 0.2

        # 4. Volume Confidence Bonus (15 points max)
        # More trades = more confidence in the rating
        total_volume = sum(float(t.amount_usd) for t in trades)
        trade_count = len(trades)

        # Volume tiers
        if total_volume >= 100000 and trade_count >= 50:
            volume_bonus = 15  # Maximum confidence
        elif total_volume >= 50000 and trade_count >= 25:
            volume_bonus = 12
        elif total_volume >= 10000 and trade_count >= 10:
            volume_bonus = 8
        elif trade_count >= 5:
            volume_bonus = 4
        else:
            volume_bonus = 0
        breakdown['volume_bonus'] = volume_bonus

        # Calculate total score
        total_score = sum(breakdown.values())

        return int(min(100, max(0, total_score))), breakdown

    def _score_to_grade(self, score: int) -> CreditGrade:
        """Convert numeric score to letter grade."""
        if score >= self.GRADE_THRESHOLDS[CreditGrade.AAA]:
            return CreditGrade.AAA
        elif score >= self.GRADE_THRESHOLDS[CreditGrade.AA]:
            return CreditGrade.AA
        elif score >= self.GRADE_THRESHOLDS[CreditGrade.A]:
            return CreditGrade.A
        elif score >= self.GRADE_THRESHOLDS[CreditGrade.B]:
            return CreditGrade.B
        elif score >= self.GRADE_THRESHOLDS[CreditGrade.C]:
            return CreditGrade.C
        else:
            return CreditGrade.F

    def _generate_reasoning(
        self,
        metrics: WalletMetrics,
        breakdown: dict,
        grade: CreditGrade
    ) -> str:
        """Generate human-readable reasoning for the rating."""

        grade_descriptions = {
            CreditGrade.AAA: "God Whale - Elite performer with exceptional consistency",
            CreditGrade.AA: "Elite Smart Money - Top-tier performance",
            CreditGrade.A: "Smart Money - Proven profitable trader",
            CreditGrade.B: "Average Trader - Mixed results",
            CreditGrade.C: "Gambler - Unproven or inconsistent",
            CreditGrade.F: "Whale Trap - Use as counter-signal",
        }

        reasoning_parts = [grade_descriptions[grade]]

        # Add metric highlights
        if metrics.win_rate >= 70:
            reasoning_parts.append(f"Strong {metrics.win_rate:.1f}% win rate")
        elif metrics.win_rate < 40:
            reasoning_parts.append(f"Low {metrics.win_rate:.1f}% win rate")

        if metrics.pnl_ratio >= 2.0:
            reasoning_parts.append(f"Excellent {metrics.pnl_ratio:.1f}x PnL ratio")
        elif metrics.pnl_ratio < 1.0:
            reasoning_parts.append(f"Poor {metrics.pnl_ratio:.1f}x PnL ratio (losses > wins)")

        reasoning_parts.append(f"Based on {metrics.total_trades} trades")

        return " | ".join(reasoning_parts)

    def quick_grade(
        self,
        win_rate: float,
        pnl_ratio: float,
        total_trades: int = 10
    ) -> CreditGrade:
        """
        Quick grading based on key metrics only.
        Used for fast filtering before full analysis.

        The PRD Logic:
        - IF (Win Rate > 70% AND PnL Ratio > 2.0) -> Grade "AAA"
        - IF (Win Rate > 50% AND PnL Ratio > 1.5) -> Grade "A"
        - ELSE -> Grade "B" or "C"
        """
        if total_trades < self.MIN_TRADES_FOR_RATING:
            return CreditGrade.C

        if win_rate > 70 and pnl_ratio > 2.0:
            return CreditGrade.AAA
        elif win_rate > 65 and pnl_ratio > 1.8:
            return CreditGrade.AA
        elif win_rate > 50 and pnl_ratio > 1.5:
            return CreditGrade.A
        elif win_rate > 40 and pnl_ratio > 1.0:
            return CreditGrade.B
        elif win_rate > 30:
            return CreditGrade.C
        else:
            return CreditGrade.F  # The Whale Trap


# ============================================
# Helper Functions
# ============================================

def calculate_potential_roi(current_price: float) -> float:
    """
    Calculate potential ROI if copying a trade at current price.

    Formula: ((1.00 - EntryPrice) / EntryPrice) * 100

    Example: Price = 0.40 (40 cents)
    ROI = ((1.00 - 0.40) / 0.40) * 100 = 150%

    This means if the market resolves YES, you get 2.5x your money.
    """
    if current_price <= 0 or current_price >= 1:
        return 0.0

    return ((1.0 - current_price) / current_price) * 100


def format_grade_badge(grade: CreditGrade) -> dict:
    """Return styling info for grade badges in the UI."""
    badge_styles = {
        CreditGrade.AAA: {
            "color": "gold",
            "bg": "bg-gradient-to-r from-yellow-400 to-amber-500",
            "text": "text-black",
            "label": "AAA",
            "glow": True,
        },
        CreditGrade.AA: {
            "color": "silver",
            "bg": "bg-gradient-to-r from-gray-300 to-gray-400",
            "text": "text-black",
            "label": "AA",
            "glow": True,
        },
        CreditGrade.A: {
            "color": "green",
            "bg": "bg-emerald-500",
            "text": "text-white",
            "label": "A",
            "glow": False,
        },
        CreditGrade.B: {
            "color": "blue",
            "bg": "bg-blue-500",
            "text": "text-white",
            "label": "B",
            "glow": False,
        },
        CreditGrade.C: {
            "color": "gray",
            "bg": "bg-gray-500",
            "text": "text-white",
            "label": "C",
            "glow": False,
        },
        CreditGrade.F: {
            "color": "red",
            "bg": "bg-red-600",
            "text": "text-white",
            "label": "F",
            "glow": True,  # Glow to warn users this is a counter-signal
        },
    }
    return badge_styles.get(grade, badge_styles[CreditGrade.C])
