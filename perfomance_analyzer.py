from database_manager import SessionLocal
from database_models import TradeJournal
import numpy as np


class PerformanceAnalyzer:

    def __init__(self):
        self.db = SessionLocal()

    def get_recent_trades(self, limit=200):

        return (
            self.db.query(TradeJournal)
            .order_by(TradeJournal.created_at.desc())
            .limit(limit)
            .all()
        )

    def calculate_winrate(self):

        trades = self.get_recent_trades()

        wins = [t for t in trades if t.trade_result == "win"]
        losses = [t for t in trades if t.trade_result == "loss"]

        total = len(wins) + len(losses)

        if total == 0:
            return 0

        return len(wins) / total

    def total_profit(self):

        trades = self.get_recent_trades()

        return sum(t.profit_loss or 0 for t in trades)

    def average_rr(self):

        trades = self.get_recent_trades()

        rr_values = [t.rr for t in trades if t.rr is not None]

        if not rr_values:
            return 0

        return float(np.mean(rr_values))

    def pair_performance(self):

        trades = self.get_recent_trades()

        pairs = {}

        for trade in trades:

            pairs.setdefault(trade.symbol, [])
            pairs[trade.symbol].append(trade)

        result = {}

        for pair, pair_trades in pairs.items():

            wins = len([t for t in pair_trades if t.trade_result == "win"])
            total = len(pair_trades)

            result[pair] = wins / total if total else 0

        return result