"""
PolySpy Exit Detector
"The Exit Liquidity Savior" Strategy - Detect when smart money dumps

When a Grade AAA or A whale sells >50% of their position, you don't want
to be the one buying it. This service detects these "dump" events and
flags them as critical alerts.
"""

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple
from decimal import Decimal
from dataclasses import dataclass
from collections import defaultdict

from backend.models.schemas import (
    CreditGrade,
    PersonaTag,
    Transaction,
    TransactionType,
    SignalType,
    Wallet,
)


@dataclass
class DumpAlert:
    """A dump alert event"""
    wallet_address: str
    wallet_grade: CreditGrade
    wallet_persona: Optional[PersonaTag]
    market_slug: str
    outcome: str
    position_sold_pct: float  # % of position sold
    amount_usd: Decimal
    price: Decimal
    transaction: Transaction
    alert_severity: str  # "CRITICAL", "HIGH", "MEDIUM"
    alert_message: str
    timestamp: datetime


class ExitDetector:
    """
    Detects exit events from smart money wallets.

    Types of exits detected:
    1. Position Dumps: Selling >50% of position
    2. Full Exits: Selling entire position
    3. Redemptions: Redeeming tokens after resolution
    4. Rapid Sells: Multiple sells in short time (panic selling)
    """

    # Alert thresholds
    DUMP_THRESHOLD_PCT = 50        # >50% position sell = dump alert
    CRITICAL_DUMP_PCT = 80         # >80% = critical alert
    RAPID_SELL_WINDOW_HOURS = 2    # Multiple sells within 2 hours
    MIN_ALERT_AMOUNT = 1000        # Minimum $1k for alerts

    # Grades that trigger dump alerts (smart money only)
    SMART_MONEY_GRADES = [CreditGrade.AAA, CreditGrade.AA, CreditGrade.A]

    def __init__(self):
        # Position tracking: {wallet_address: {market_slug: {outcome: shares}}}
        self.positions: Dict[str, Dict[str, Dict[str, Decimal]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(Decimal))
        )

    def process_transaction(
        self,
        transaction: Transaction,
        wallet: Wallet
    ) -> Optional[DumpAlert]:
        """
        Process a transaction and check if it triggers a dump alert.

        Args:
            transaction: The transaction to process
            wallet: The wallet's profile (with grade)

        Returns:
            DumpAlert if this is a dump event, None otherwise
        """
        # Update position tracking first
        self._update_position(transaction)

        # Only check for dumps on SELL/EXIT/REDEEM transactions
        if transaction.type not in [TransactionType.SELL, TransactionType.EXIT, TransactionType.REDEEM]:
            return None

        # Only alert on smart money wallets
        if wallet.credit_grade not in self.SMART_MONEY_GRADES:
            return None

        # Check if this is significant enough to alert
        if float(transaction.amount_usd) < self.MIN_ALERT_AMOUNT:
            return None

        # Calculate position sold percentage
        position_sold_pct = self._calculate_position_sold_pct(transaction)

        # Determine if this triggers an alert
        if position_sold_pct >= self.DUMP_THRESHOLD_PCT:
            return self._create_dump_alert(
                transaction=transaction,
                wallet=wallet,
                position_sold_pct=position_sold_pct
            )

        return None

    def _update_position(self, transaction: Transaction) -> None:
        """Update position tracking based on transaction."""
        addr = transaction.wallet_address
        market = transaction.market_slug
        outcome = transaction.outcome

        if transaction.type == TransactionType.BUY:
            self.positions[addr][market][outcome] += transaction.shares
        elif transaction.type in [TransactionType.SELL, TransactionType.EXIT, TransactionType.REDEEM]:
            self.positions[addr][market][outcome] -= transaction.shares
            # Floor at 0
            if self.positions[addr][market][outcome] < 0:
                self.positions[addr][market][outcome] = Decimal("0")

    def _calculate_position_sold_pct(self, transaction: Transaction) -> float:
        """Calculate what percentage of position was sold in this transaction."""
        addr = transaction.wallet_address
        market = transaction.market_slug
        outcome = transaction.outcome

        # Get position BEFORE this sell (add back the sold shares)
        current_position = self.positions[addr][market][outcome]
        position_before = current_position + transaction.shares

        if position_before <= 0:
            return 100.0  # Full exit

        sold_pct = (float(transaction.shares) / float(position_before)) * 100
        return min(100.0, sold_pct)

    def _create_dump_alert(
        self,
        transaction: Transaction,
        wallet: Wallet,
        position_sold_pct: float
    ) -> DumpAlert:
        """Create a dump alert object."""

        # Determine severity
        if position_sold_pct >= self.CRITICAL_DUMP_PCT:
            severity = "CRITICAL"
        elif position_sold_pct >= 70:
            severity = "HIGH"
        else:
            severity = "MEDIUM"

        # Generate alert message
        grade_label = wallet.credit_grade.value
        persona_label = wallet.persona_tag.value if wallet.persona_tag else "Smart Money"

        if position_sold_pct >= 95:
            action = "FULL EXIT"
        else:
            action = f"{position_sold_pct:.0f}% DUMP"

        message = f"🚨 {action}: {grade_label} {persona_label} sold ${float(transaction.amount_usd):,.0f} of {transaction.outcome}"

        return DumpAlert(
            wallet_address=wallet.address,
            wallet_grade=wallet.credit_grade,
            wallet_persona=wallet.persona_tag,
            market_slug=transaction.market_slug,
            outcome=transaction.outcome,
            position_sold_pct=position_sold_pct,
            amount_usd=transaction.amount_usd,
            price=transaction.price,
            transaction=transaction,
            alert_severity=severity,
            alert_message=message,
            timestamp=transaction.timestamp,
        )

    def detect_rapid_selling(
        self,
        transactions: List[Transaction],
        wallet: Wallet
    ) -> Optional[DumpAlert]:
        """
        Detect rapid selling pattern - multiple sells in short timeframe.

        This could indicate panic selling or insider knowledge of bad news.
        """
        if wallet.credit_grade not in self.SMART_MONEY_GRADES:
            return None

        # Filter to recent sells
        now = datetime.utcnow()
        window_start = now - timedelta(hours=self.RAPID_SELL_WINDOW_HOURS)

        recent_sells = [
            t for t in transactions
            if t.type in [TransactionType.SELL, TransactionType.EXIT]
            and t.timestamp >= window_start
            and t.wallet_address == wallet.address
        ]

        if len(recent_sells) < 3:  # Need at least 3 sells for "rapid"
            return None

        # Calculate total sold
        total_sold = sum(float(t.amount_usd) for t in recent_sells)

        if total_sold < self.MIN_ALERT_AMOUNT * 3:  # Higher threshold for rapid sell
            return None

        # Create alert for the most recent transaction
        latest = max(recent_sells, key=lambda t: t.timestamp)

        return DumpAlert(
            wallet_address=wallet.address,
            wallet_grade=wallet.credit_grade,
            wallet_persona=wallet.persona_tag,
            market_slug=latest.market_slug,
            outcome=latest.outcome,
            position_sold_pct=0,  # N/A for rapid sell
            amount_usd=Decimal(str(total_sold)),
            price=latest.price,
            transaction=latest,
            alert_severity="HIGH",
            alert_message=f"🚨 RAPID SELL: {wallet.credit_grade.value} whale sold {len(recent_sells)} times in {self.RAPID_SELL_WINDOW_HOURS}h (${total_sold:,.0f} total)",
            timestamp=latest.timestamp,
        )

    def detect_redemption(
        self,
        transaction: Transaction,
        wallet: Wallet
    ) -> Optional[DumpAlert]:
        """
        Detect redemption events - wallet redeeming tokens after market resolution.

        This is less urgent than dumps but still valuable info.
        """
        if transaction.type != TransactionType.REDEEM:
            return None

        if wallet.credit_grade not in self.SMART_MONEY_GRADES:
            return None

        if float(transaction.amount_usd) < self.MIN_ALERT_AMOUNT:
            return None

        return DumpAlert(
            wallet_address=wallet.address,
            wallet_grade=wallet.credit_grade,
            wallet_persona=wallet.persona_tag,
            market_slug=transaction.market_slug,
            outcome=transaction.outcome,
            position_sold_pct=100,  # Redemptions are always full position
            amount_usd=transaction.amount_usd,
            price=transaction.price,
            transaction=transaction,
            alert_severity="MEDIUM",  # Lower severity for redemptions
            alert_message=f"💰 REDEMPTION: {wallet.credit_grade.value} whale redeemed ${float(transaction.amount_usd):,.0f} from {transaction.outcome}",
            timestamp=transaction.timestamp,
        )

    def get_position(
        self,
        wallet_address: str,
        market_slug: str,
        outcome: str
    ) -> Decimal:
        """Get current tracked position for a wallet."""
        return self.positions[wallet_address][market_slug][outcome]

    def load_positions_from_history(
        self,
        transactions: List[Transaction]
    ) -> None:
        """
        Load position tracking from historical transactions.

        Call this on startup to initialize positions.
        """
        # Sort by timestamp
        sorted_txs = sorted(transactions, key=lambda t: t.timestamp)

        for tx in sorted_txs:
            self._update_position(tx)


# ============================================
# Event Stream Processor
# ============================================

class ExitEventProcessor:
    """
    Processes blockchain events to detect exits in real-time.

    Listens for:
    - CTFExchange Sell events
    - CTFExchange Redemption events
    """

    # CTFExchange event signatures
    SELL_EVENT_SIG = "OrderFilled"
    REDEEM_EVENT_SIG = "TokenRedeemed"

    def __init__(self, exit_detector: ExitDetector):
        self.exit_detector = exit_detector

    def process_event(
        self,
        event_type: str,
        event_data: Dict[str, Any],
        wallet_lookup: callable
    ) -> Optional[DumpAlert]:
        """
        Process a blockchain event and check for exit signals.

        Args:
            event_type: Type of event ("OrderFilled", "TokenRedeemed")
            event_data: Decoded event data
            wallet_lookup: Function to get wallet profile by address

        Returns:
            DumpAlert if this is a dump event
        """
        # Parse event into transaction
        transaction = self._parse_event_to_transaction(event_type, event_data)

        if not transaction:
            return None

        # Look up wallet
        wallet = wallet_lookup(transaction.wallet_address)
        if not wallet:
            return None

        # Check for dump
        if event_type == self.SELL_EVENT_SIG:
            return self.exit_detector.process_transaction(transaction, wallet)
        elif event_type == self.REDEEM_EVENT_SIG:
            return self.exit_detector.detect_redemption(transaction, wallet)

        return None

    def _parse_event_to_transaction(
        self,
        event_type: str,
        event_data: Dict[str, Any]
    ) -> Optional[Transaction]:
        """Parse blockchain event into Transaction object."""

        try:
            if event_type == self.SELL_EVENT_SIG:
                tx_type = TransactionType.SELL

                # Determine if this is a full exit
                # (Would need position context - simplified here)
                pass

            elif event_type == self.REDEEM_EVENT_SIG:
                tx_type = TransactionType.REDEEM
            else:
                return None

            return Transaction(
                wallet_address=event_data.get("maker", event_data.get("user")),
                market_slug=event_data.get("market_id", ""),
                outcome=event_data.get("outcome", "YES"),
                type=tx_type,
                amount_usd=Decimal(str(event_data.get("amount_usd", 0))),
                shares=Decimal(str(event_data.get("shares", 0))),
                price=Decimal(str(event_data.get("price", 0))),
                tx_hash=event_data.get("transaction_hash"),
                block_number=event_data.get("block_number"),
                timestamp=datetime.utcnow(),
            )
        except Exception:
            return None


# ============================================
# Alert Formatting
# ============================================

def format_dump_alert_for_ui(alert: DumpAlert) -> Dict[str, Any]:
    """Format a dump alert for the frontend."""
    severity_colors = {
        "CRITICAL": {"bg": "bg-red-900", "border": "border-red-500", "text": "text-red-300"},
        "HIGH": {"bg": "bg-orange-900", "border": "border-orange-500", "text": "text-orange-300"},
        "MEDIUM": {"bg": "bg-yellow-900", "border": "border-yellow-500", "text": "text-yellow-300"},
    }

    colors = severity_colors.get(alert.alert_severity, severity_colors["MEDIUM"])

    return {
        "id": f"dump-{alert.wallet_address[:8]}-{alert.timestamp.timestamp()}",
        "type": "DUMP_ALERT",
        "severity": alert.alert_severity,
        "message": alert.alert_message,
        "wallet_address": alert.wallet_address,
        "wallet_grade": alert.wallet_grade.value,
        "wallet_persona": alert.wallet_persona.value if alert.wallet_persona else None,
        "market_slug": alert.market_slug,
        "outcome": alert.outcome,
        "amount_usd": float(alert.amount_usd),
        "price": float(alert.price),
        "position_sold_pct": alert.position_sold_pct,
        "timestamp": alert.timestamp.isoformat(),
        "colors": colors,
    }
