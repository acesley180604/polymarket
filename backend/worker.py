"""
PolySpy Worker
The main data processing engine that:
1. Fetches whale transactions
2. Calculates credit ratings
3. Assigns personas
4. Detects exit events
5. Generates signals for the dashboard

Run this as a background worker to keep data fresh.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Optional
import os

# Services
from backend.services.credit_rating import (
    CreditRatingService,
    calculate_potential_roi,
)
from backend.services.persona_engine import (
    PersonaEngine,
    get_persona_display_info,
    should_inverse_signal,
)
from backend.services.exit_detector import (
    ExitDetector,
    format_dump_alert_for_ui,
)
from backend.models.schemas import (
    CreditGrade,
    PersonaTag,
    Transaction,
    TransactionType,
    TradeHistory,
    Wallet,
    Signal,
    SignalType,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("polyspy.worker")


# ============================================
# Configuration
# ============================================

class Config:
    """Worker configuration"""
    # Supabase
    SUPABASE_URL = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

    # Polymarket API
    POLYMARKET_API_URL = "https://clob.polymarket.com"
    GAMMA_API_URL = "https://gamma-api.polymarket.com"

    # Minimum thresholds
    MIN_WHALE_VOLUME = 10000  # $10k minimum to track
    MIN_SIGNAL_AMOUNT = 1000  # $1k minimum for signals

    # Worker settings
    POLL_INTERVAL_SECONDS = 30
    BATCH_SIZE = 100


# ============================================
# Data Fetching (Mock for MVP)
# ============================================

def fetch_mock_transactions() -> List[Transaction]:
    """
    Fetch mock transaction data for testing.

    In production, this would:
    1. Connect to Polymarket's CLOB API
    2. Listen to CTFExchange events on Polygon
    3. Parse and normalize trade data
    """
    # Mock data representing whale activity
    now = datetime.utcnow()

    mock_data = [
        # AAA Whale - "The Insider"
        {
            "wallet_address": "0xAAA1234567890123456789012345678901234567",
            "market_slug": "will-trump-win-2024",
            "market_question": "Will Trump win the 2024 US Presidential Election?",
            "outcome": "YES",
            "type": "BUY",
            "amount_usd": 50000,
            "shares": 125000,
            "price": 0.40,
            "is_profitable": True,
            "realized_pnl": 75000,
            "timestamp": now - timedelta(days=30),
        },
        {
            "wallet_address": "0xAAA1234567890123456789012345678901234567",
            "market_slug": "will-btc-hit-100k",
            "market_question": "Will Bitcoin hit $100,000 in 2024?",
            "outcome": "YES",
            "type": "BUY",
            "amount_usd": 25000,
            "shares": 41666,
            "price": 0.60,
            "is_profitable": True,
            "realized_pnl": 16666,
            "timestamp": now - timedelta(days=20),
        },
        {
            "wallet_address": "0xAAA1234567890123456789012345678901234567",
            "market_slug": "will-fed-cut-rates",
            "market_question": "Will the Fed cut interest rates by December 2024?",
            "outcome": "NO",
            "type": "BUY",
            "amount_usd": 30000,
            "shares": 150000,
            "price": 0.20,
            "is_profitable": True,
            "realized_pnl": 120000,
            "timestamp": now - timedelta(days=15),
        },

        # Grade A Whale - "The Contrarian"
        {
            "wallet_address": "0xBBB1234567890123456789012345678901234567",
            "market_slug": "will-recession-2024",
            "market_question": "Will there be a US recession in 2024?",
            "outcome": "NO",
            "type": "BUY",
            "amount_usd": 20000,
            "shares": 22222,
            "price": 0.90,  # Betting against crowd at 90%
            "is_profitable": True,
            "realized_pnl": 2222,
            "timestamp": now - timedelta(days=25),
        },
        {
            "wallet_address": "0xBBB1234567890123456789012345678901234567",
            "market_slug": "will-eth-flip-btc",
            "market_question": "Will ETH market cap exceed BTC in 2024?",
            "outcome": "YES",
            "type": "BUY",
            "amount_usd": 15000,
            "shares": 150000,
            "price": 0.10,  # Contrarian bet at 10 cents
            "is_profitable": False,
            "realized_pnl": -15000,
            "timestamp": now - timedelta(days=18),
        },

        # Grade F Whale - "The Whale Trap"
        {
            "wallet_address": "0xFFF1234567890123456789012345678901234567",
            "market_slug": "will-trump-win-2024",
            "market_question": "Will Trump win the 2024 US Presidential Election?",
            "outcome": "NO",
            "type": "BUY",
            "amount_usd": 100000,
            "shares": 166666,
            "price": 0.60,
            "is_profitable": False,
            "realized_pnl": -100000,
            "timestamp": now - timedelta(days=35),
        },
        {
            "wallet_address": "0xFFF1234567890123456789012345678901234567",
            "market_slug": "will-btc-hit-100k",
            "market_question": "Will Bitcoin hit $100,000 in 2024?",
            "outcome": "NO",
            "type": "BUY",
            "amount_usd": 50000,
            "shares": 125000,
            "price": 0.40,
            "is_profitable": False,
            "realized_pnl": -50000,
            "timestamp": now - timedelta(days=22),
        },

        # Recent activity (for signals)
        {
            "wallet_address": "0xAAA1234567890123456789012345678901234567",
            "market_slug": "will-ai-take-jobs-2025",
            "market_question": "Will AI replace >1M US jobs by 2025?",
            "outcome": "YES",
            "type": "BUY",
            "amount_usd": 35000,
            "shares": 77777,
            "price": 0.45,
            "is_profitable": None,  # Not resolved yet
            "realized_pnl": None,
            "timestamp": now - timedelta(hours=2),
        },

        # DUMP ALERT - AAA whale selling
        {
            "wallet_address": "0xAAA1234567890123456789012345678901234567",
            "market_slug": "will-trump-win-2024",
            "market_question": "Will Trump win the 2024 US Presidential Election?",
            "outcome": "YES",
            "type": "SELL",
            "amount_usd": 45000,  # Selling 90% of position
            "shares": 112500,
            "price": 0.45,
            "is_profitable": None,
            "realized_pnl": None,
            "timestamp": now - timedelta(minutes=30),
        },
    ]

    transactions = []
    for data in mock_data:
        tx = Transaction(
            wallet_address=data["wallet_address"],
            market_slug=data["market_slug"],
            market_question=data.get("market_question"),
            outcome=data["outcome"],
            type=TransactionType(data["type"]),
            amount_usd=Decimal(str(data["amount_usd"])),
            shares=Decimal(str(data["shares"])),
            price=Decimal(str(data["price"])),
            is_profitable=data.get("is_profitable"),
            realized_pnl=Decimal(str(data["realized_pnl"])) if data.get("realized_pnl") else None,
            timestamp=data["timestamp"],
        )
        transactions.append(tx)

    return transactions


# ============================================
# Main Processing Pipeline
# ============================================

class PolySpyWorker:
    """Main worker that orchestrates all processing."""

    def __init__(self):
        self.credit_service = CreditRatingService()
        self.persona_engine = PersonaEngine()
        self.exit_detector = ExitDetector()

        # Cache
        self.wallets: Dict[str, Wallet] = {}
        self.signals: List[Signal] = []

    def process_transactions(
        self,
        transactions: List[Transaction]
    ) -> Dict[str, Any]:
        """
        Main processing pipeline.

        1. Group transactions by wallet
        2. Calculate credit ratings
        3. Assign personas
        4. Check for exit events
        5. Generate signals
        """
        results = {
            "wallets_processed": 0,
            "signals_generated": 0,
            "dump_alerts": 0,
        }

        # Group by wallet
        wallet_txs: Dict[str, List[Transaction]] = {}
        for tx in transactions:
            if tx.wallet_address not in wallet_txs:
                wallet_txs[tx.wallet_address] = []
            wallet_txs[tx.wallet_address].append(tx)

        # Process each wallet
        for wallet_address, txs in wallet_txs.items():
            wallet = self._process_wallet(wallet_address, txs)
            self.wallets[wallet_address] = wallet
            results["wallets_processed"] += 1

        # Generate signals from recent activity
        recent_txs = [
            tx for tx in transactions
            if tx.timestamp > datetime.utcnow() - timedelta(hours=24)
        ]

        for tx in recent_txs:
            wallet = self.wallets.get(tx.wallet_address)
            if not wallet:
                continue

            # Check for exit events
            if tx.type in [TransactionType.SELL, TransactionType.EXIT]:
                dump_alert = self.exit_detector.process_transaction(tx, wallet)
                if dump_alert:
                    results["dump_alerts"] += 1
                    # Generate dump signal
                    signal = self._create_signal(tx, wallet, SignalType.DUMP_ALERT)
                    signal.copy_ev_percent = 0  # No EV for dumps
                    self.signals.append(signal)
                    continue

            # Generate buy signals for smart money
            if tx.type == TransactionType.BUY:
                if wallet.credit_grade in [CreditGrade.AAA, CreditGrade.AA, CreditGrade.A]:
                    signal_type = SignalType.WHALE_BUY
                    if wallet.persona_tag == PersonaTag.INSIDER:
                        signal_type = SignalType.INSIDER_MOVE
                    elif wallet.persona_tag == PersonaTag.CONTRARIAN:
                        signal_type = SignalType.CONTRARIAN_BET

                    signal = self._create_signal(tx, wallet, signal_type)
                    self.signals.append(signal)
                    results["signals_generated"] += 1

        return results

    def _process_wallet(
        self,
        wallet_address: str,
        transactions: List[Transaction]
    ) -> Wallet:
        """Process a single wallet's data."""

        # Separate resolved and unresolved
        resolved = [tx for tx in transactions if tx.is_profitable is not None]
        all_txs = transactions

        # Create trade history
        trade_history = TradeHistory(
            wallet_address=wallet_address,
            trades=all_txs,
            resolved_trades=resolved,
        )

        # Calculate credit rating
        rating = self.credit_service.calculate_credit_grade(trade_history)

        # Assign persona
        persona = self.persona_engine.assign_persona(trade_history)

        # Calculate aggregates
        total_volume = sum(float(tx.amount_usd) for tx in all_txs)
        wins = [tx for tx in resolved if tx.is_profitable]
        losses = [tx for tx in resolved if not tx.is_profitable]
        total_profit = sum(float(tx.realized_pnl or 0) for tx in wins)
        total_loss = sum(abs(float(tx.realized_pnl or 0)) for tx in losses)
        win_rate = (len(wins) / len(resolved) * 100) if resolved else 0

        return Wallet(
            address=wallet_address,
            credit_grade=rating.grade,
            credit_score=rating.score,
            persona_tag=persona,
            win_rate=win_rate,
            pnl_ratio=total_profit / total_loss if total_loss > 0 else 0,
            total_trades=len(all_txs),
            total_wins=len(wins),
            total_losses=len(losses),
            total_profit_usd=Decimal(str(total_profit)),
            total_loss_usd=Decimal(str(total_loss)),
            total_volume_usd=Decimal(str(total_volume)),
            last_active_at=max(tx.timestamp for tx in all_txs),
        )

    def _create_signal(
        self,
        transaction: Transaction,
        wallet: Wallet,
        signal_type: SignalType
    ) -> Signal:
        """Create a signal from a transaction."""

        # Calculate EV
        copy_ev = calculate_potential_roi(float(transaction.price))

        return Signal(
            wallet_address=wallet.address,
            market_slug=transaction.market_slug,
            market_question=transaction.market_question,
            signal_type=signal_type,
            whale_grade=wallet.credit_grade,
            whale_persona=wallet.persona_tag,
            whale_win_rate=wallet.win_rate,
            outcome=transaction.outcome,
            amount_usd=transaction.amount_usd,
            price=transaction.price,
            copy_ev_percent=copy_ev,
            timestamp=transaction.timestamp,
        )

    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get data formatted for the dashboard."""

        # Sort signals by timestamp (newest first)
        sorted_signals = sorted(self.signals, key=lambda s: s.timestamp, reverse=True)

        # Get top whales
        top_whales = sorted(
            [w for w in self.wallets.values() if w.credit_grade in [CreditGrade.AAA, CreditGrade.AA, CreditGrade.A]],
            key=lambda w: w.credit_score,
            reverse=True
        )[:10]

        # Separate dump alerts
        dump_alerts = [s for s in sorted_signals if s.signal_type == SignalType.DUMP_ALERT]
        buy_signals = [s for s in sorted_signals if s.signal_type != SignalType.DUMP_ALERT]

        return {
            "signals": [self._format_signal_for_ui(s) for s in buy_signals[:20]],
            "dump_alerts": [self._format_signal_for_ui(s) for s in dump_alerts[:10]],
            "top_whales": [self._format_wallet_for_ui(w) for w in top_whales],
            "stats": {
                "total_whales_tracked": len(self.wallets),
                "aaa_whales": len([w for w in self.wallets.values() if w.credit_grade == CreditGrade.AAA]),
                "signals_24h": len(sorted_signals),
                "dump_alerts_24h": len(dump_alerts),
            }
        }

    def _format_signal_for_ui(self, signal: Signal) -> Dict[str, Any]:
        """Format signal for frontend consumption."""
        persona_info = get_persona_display_info(signal.whale_persona) if signal.whale_persona else None

        return {
            "id": f"signal-{signal.wallet_address[:8]}-{signal.timestamp.timestamp()}",
            "type": signal.signal_type.value,
            "wallet_address": signal.wallet_address,
            "wallet_short": f"{signal.wallet_address[:6]}...{signal.wallet_address[-4:]}",
            "whale_grade": signal.whale_grade.value,
            "whale_persona": signal.whale_persona.value if signal.whale_persona else None,
            "whale_persona_info": persona_info,
            "whale_win_rate": signal.whale_win_rate,
            "market_slug": signal.market_slug,
            "market_question": signal.market_question,
            "outcome": signal.outcome,
            "amount_usd": float(signal.amount_usd),
            "price": float(signal.price),
            "copy_ev_percent": signal.copy_ev_percent,
            "timestamp": signal.timestamp.isoformat(),
            "polymarket_url": f"https://polymarket.com/event/{signal.market_slug}",
            "is_dump": signal.signal_type == SignalType.DUMP_ALERT,
            "is_inverse_signal": should_inverse_signal(signal.whale_persona),
        }

    def _format_wallet_for_ui(self, wallet: Wallet) -> Dict[str, Any]:
        """Format wallet for frontend consumption."""
        persona_info = get_persona_display_info(wallet.persona_tag) if wallet.persona_tag else None

        return {
            "address": wallet.address,
            "address_short": f"{wallet.address[:6]}...{wallet.address[-4:]}",
            "credit_grade": wallet.credit_grade.value,
            "credit_score": wallet.credit_score,
            "persona_tag": wallet.persona_tag.value if wallet.persona_tag else None,
            "persona_info": persona_info,
            "win_rate": wallet.win_rate,
            "pnl_ratio": wallet.pnl_ratio,
            "total_trades": wallet.total_trades,
            "total_profit_usd": float(wallet.total_profit_usd),
            "total_volume_usd": float(wallet.total_volume_usd),
            "last_active": wallet.last_active_at.isoformat() if wallet.last_active_at else None,
        }


# ============================================
# Entry Point
# ============================================

def main():
    """Main entry point for the worker."""
    logger.info("🚀 PolySpy Worker starting...")

    worker = PolySpyWorker()

    # Fetch mock data (replace with real API in production)
    logger.info("📥 Fetching transaction data...")
    transactions = fetch_mock_transactions()
    logger.info(f"   Found {len(transactions)} transactions")

    # Process transactions
    logger.info("🔄 Processing transactions...")
    results = worker.process_transactions(transactions)
    logger.info(f"   Processed {results['wallets_processed']} wallets")
    logger.info(f"   Generated {results['signals_generated']} signals")
    logger.info(f"   Found {results['dump_alerts']} dump alerts")

    # Get dashboard data
    dashboard_data = worker.get_dashboard_data()

    # Print summary
    logger.info("\n" + "="*60)
    logger.info("📊 DASHBOARD SUMMARY")
    logger.info("="*60)

    logger.info(f"\n🐋 TOP WHALES:")
    for whale in dashboard_data["top_whales"][:5]:
        logger.info(
            f"   [{whale['credit_grade']}] {whale['address_short']} | "
            f"Win Rate: {whale['win_rate']:.1f}% | "
            f"Persona: {whale['persona_tag'] or 'None'}"
        )

    logger.info(f"\n📡 RECENT SIGNALS:")
    for signal in dashboard_data["signals"][:5]:
        ev_display = f"+{signal['copy_ev_percent']:.0f}%" if signal['copy_ev_percent'] > 0 else "N/A"
        logger.info(
            f"   [{signal['whale_grade']}] {signal['type']} | "
            f"${signal['amount_usd']:,.0f} on {signal['outcome']} | "
            f"Copy EV: {ev_display}"
        )

    if dashboard_data["dump_alerts"]:
        logger.info(f"\n🚨 DUMP ALERTS:")
        for alert in dashboard_data["dump_alerts"][:3]:
            logger.info(
                f"   [{alert['whale_grade']}] DUMP! "
                f"${alert['amount_usd']:,.0f} sold | "
                f"{alert['market_slug']}"
            )

    logger.info("\n" + "="*60)
    logger.info("✅ Worker complete!")

    return dashboard_data


if __name__ == "__main__":
    main()
