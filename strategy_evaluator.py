from database_manager import SessionLocal
from database_models import TradeJournal
from collections import defaultdict


class StrategyEvaluator:

    def __init__(self):
        self.db = SessionLocal()

    def get_recent_trades(self, limit=300):

        return (
            self.db.query(TradeJournal)
            .order_by(TradeJournal.created_at.desc())
            .limit(limit)
            .all()
        )

    def best_setups(self):

        trades = self.get_recent_trades()

        profitable = [t for t in trades if (t.profit_loss or 0) > 0]

        profitable.sort(key=lambda x: x.profit_loss, reverse=True)

        return profitable[:10]

    def worst_setups(self):

        trades = self.get_recent_trades()

        losing = [t for t in trades if (t.profit_loss or 0) < 0]

        losing.sort(key=lambda x: x.profit_loss)

        return losing[:10]

    def indicator_performance(self):

        trades = self.get_recent_trades()

        indicator_stats = defaultdict(list)

        for trade in trades:

            indicators = trade.indicator_snapshot or {}

            for name, value in indicators.items():

                indicator_stats[name].append(trade.profit_loss or 0)

        result = {}

        for indicator, profits in indicator_stats.items():

            if profits:
                result[indicator] = sum(profits) / len(profits)

        return result

    def pair_performance(self):

        trades = self.get_recent_trades()

        pairs = defaultdict(list)

        for trade in trades:
            pairs[trade.symbol].append(trade)

        result = {}

        for pair, pair_trades in pairs.items():

            wins = len([t for t in pair_trades if t.trade_result == "win"])
            total = len(pair_trades)

            result[pair] = wins / total if total else 0

        return result