import json
import logging

from ai_improvement_loop import AIImprovementLoop

logger = logging.getLogger(__name__)


class StrategyController:

    def __init__(self, config_path="strategy_config.json"):

        self.loop = AIImprovementLoop()
        self.config_path = config_path

        self.config = self.load_config()

    def load_config(self):

        try:

            with open(self.config_path, "r") as f:
                return json.load(f)

        except Exception:

            default = {
                "confidence_threshold": 0.6,
                "enabled_pairs": ["XAUUSD", "EURUSD", "GBPUSD"],
                "indicator_weights": {
                    "RSI": 1.0,
                    "MACD": 1.0,
                    "Bollinger": 1.0
                }
            }

            with open(self.config_path, "w") as f:
                json.dump(default, f, indent=4)

            return default

    def save_config(self):

        with open(self.config_path, "w") as f:
            json.dump(self.config, f, indent=4)

    def apply_improvements(self):

        improvements = self.loop.generate_improvements()

        logger.info("AI Improvements detected:")
        logger.info(improvements)

        for imp in improvements:

            if "confidence threshold" in imp:

                self.config["confidence_threshold"] += 0.05
                logger.info("Increasing AI confidence threshold")

            if "pair" in imp:

                for pair in self.config["enabled_pairs"]:
                    if pair in imp:
                        self.config["enabled_pairs"].remove(pair)
                        logger.info(f"Disabling pair {pair}")

            if "indicator" in imp:

                for indicator in self.config["indicator_weights"]:

                    self.config["indicator_weights"][indicator] *= 0.9
                    logger.info(f"Reducing weight for {indicator}")

        self.save_config()

        return self.config