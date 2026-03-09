from performance_analyzer import PerformanceAnalyzer
from strategy_evaluator import StrategyEvaluator


class AIImprovementLoop:

    def __init__(self):

        self.performance = PerformanceAnalyzer()
        self.strategy = StrategyEvaluator()

    def generate_report(self):

        winrate = self.performance.calculate_winrate()
        total_profit = self.performance.total_profit()
        avg_rr = self.performance.average_rr()

        pair_perf = self.performance.pair_performance()
        indicator_perf = self.strategy.indicator_performance()

        best_setups = self.strategy.best_setups()
        worst_setups = self.strategy.worst_setups()

        report = {
            "winrate": winrate,
            "total_profit": total_profit,
            "average_rr": avg_rr,
            "pair_performance": pair_perf,
            "indicator_performance": indicator_perf,
            "best_setups": best_setups,
            "worst_setups": worst_setups
        }

        return report

    def detect_problems(self):

        report = self.generate_report()

        problems = []

        if report["winrate"] < 0.45:
            problems.append("overall strategy winrate terlalu rendah")

        for pair, winrate in report["pair_performance"].items():

            if winrate < 0.40:
                problems.append(f"pair {pair} memiliki winrate rendah")

        for indicator, perf in report["indicator_performance"].items():

            if perf < 0:
                problems.append(f"indicator {indicator} menghasilkan loss rata-rata")

        return problems

    def generate_improvements(self):

        problems = self.detect_problems()

        improvements = []

        for p in problems:

            if "pair" in p:
                improvements.append("pertimbangkan mengurangi trading pada pair tersebut")

            if "indicator" in p:
                improvements.append("pertimbangkan menonaktifkan atau mengurangi bobot indikator tersebut")

            if "winrate terlalu rendah" in p:
                improvements.append("tingkatkan confidence threshold AI")

        return improvements