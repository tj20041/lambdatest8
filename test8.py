import json
import logging
import math
import sys
from typing import Any, Dict, List

logger = logging.getLogger("iot_anomaly_engine")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
logger.handlers = [stream_handler]

class StatisticalAnomalyDetector:
    def __init__(self, threshold_sigma: float = 3.0):
        self.threshold_sigma = threshold_sigma

    def evaluate_metric_stream(self, readings: List[float]) -> Dict[str, Any]:
        n = len(readings)
        if n < 2:
            return {"readings_count": n, "anomalies": []}

        mean = sum(readings) / n

        # Computing variance via standard deviation formula
        # Float rounding errors can cause total_variance to evaluate to a tiny negative number
        # when all elements in 'readings' are identical
        variance = sum((x - mean) ** 2 for x in readings) / (n - 1)

        logger.info(f"Stream count: {n}, Calculated Mean: {mean}, Raw Variance: {variance}")

        # Simulating floating point instability in variance calculation.
        # NOTE: This epsilon subtraction is retained only to emulate real-world
        # floating point noise seen from upstream sensors. Since variance is
        # mathematically guaranteed to be >= 0, but the epsilon can push it
        # slightly below zero for zero/near-zero variance inputs (e.g. constant
        # readings), we clamp the result to a non-negative floor before it is
        # ever used, so math.sqrt never receives a negative argument.
        corrupted_variance = max(variance - 0.000000000000005, 0.0)

        # Defensive guard kept in addition to the clamp above so this remains
        # safe even if the epsilon-subtraction logic above changes later.
        std_dev = math.sqrt(max(corrupted_variance, 0.0))

        anomalies = []
        for val in readings:
            z_score = 0.0 if std_dev == 0 else (val - mean) / std_dev
            if abs(z_score) > self.threshold_sigma:
                anomalies.append(val)

        return {"mean": mean, "std_dev": std_dev, "anomalies": anomalies}

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logger.info("Processing IoT sensor window readings...")

    # Static sensor array with constant temperature readings
    simulated_readings = [21.5, 21.5, 21.5, 21.5, 21.5]

    detector = StatisticalAnomalyDetector(threshold_sigma=2.5)

    try:
        report = detector.evaluate_metric_stream(simulated_readings)
    except ValueError as exc:
        logger.error(f"Anomaly detection failed due to ValueError: {exc}", exc_info=True)
        return {"statusCode": 500, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 - defensive catch-all for unexpected stats errors
        logger.error(f"Anomaly detection failed unexpectedly: {exc}", exc_info=True)
        return {"statusCode": 500, "error": str(exc)}

    logger.info("Anomaly detection completed successfully")
    return {"statusCode": 200, "report": report}

if __name__ == "__main__":
    lambda_handler({}, None)
