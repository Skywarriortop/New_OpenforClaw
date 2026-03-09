from sqlalchemy.orm import Session
from database_manager import SessionLocal
from database_models import TradeJournal
from datetime import datetime


class TradeJournalService:

    def __init__(self):
        self.db: Session = SessionLocal()

    def log_trade(
        self,
        symbol,
        timeframe,
        entry_price,
        stop_loss,
        take_profit,
        indicator_snapshot=None,
        news_sentiment=None,
        ai_reasoning=None,
        ai_confidence=None,
    ):

        trade = TradeJournal(
            symbol=symbol,
            timeframe=timeframe,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            indicator_snapshot=indicator_snapshot,
            news_sentiment=news_sentiment,
            ai_reasoning=ai_reasoning,
            ai_confidence=ai_confidence,
            created_at=datetime.utcnow(),
        )

        self.db.add(trade)
        self.db.commit()
        self.db.refresh(trade)

        return trade.id

    def update_trade_result(
        self,
        trade_id,
        profit_loss,
        rr,
        drawdown,
        trade_result,
    ):

        trade = (
            self.db.query(TradeJournal)
            .filter(TradeJournal.id == trade_id)
            .first()
        )

        if not trade:
            return None

        trade.profit_loss = profit_loss
        trade.rr = rr
        trade.drawdown = drawdown
        trade.trade_result = trade_result

        self.db.commit()

        return trade

    def get_recent_trades(self, limit=100):

        return (
            self.db.query(TradeJournal)
            .order_by(TradeJournal.created_at.desc())
            .limit(limit)
            .all()
        )