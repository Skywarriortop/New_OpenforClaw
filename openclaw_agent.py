from ai_improvement_loop import AIImprovementLoop
from qwen_connector import QwenConnector


class OpenClawAgent:

    def __init__(self):

        self.loop = AIImprovementLoop()
        self.llm = QwenConnector()

    def analyze_system(self):

        report = self.loop.generate_report()
        problems = self.loop.detect_problems()
        improvements = self.loop.generate_improvements()

        prompt = f"""
You are a quantitative trading research AI.

Analyze this trading system performance.

REPORT:
{report}

PROBLEMS DETECTED:
{problems}

CURRENT RECOMMENDATIONS:
{improvements}

Tasks:
1. Identify best trading strategies
2. Identify worst strategies
3. Suggest improvements
4. Suggest risk management changes
"""

        response = self.llm.generate(prompt)

        return response