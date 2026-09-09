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

        # Simulating floating point instability in variance calculation
        corrupted_variance = variance - 0.000000000000005

        # FAILS HERE: If corrupted_variance is negative (< 0), math.sqrt raises:
        # ValueError: math domain error
        std_dev = math.sqrt(corrupted_variance)

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
    report = detector.evaluate_metric_stream(simulated_readings)

    logger.info("Anomaly detection completed successfully")
    return {"statusCode": 200, "report": report}

if __name__ == "__main__":
    lambda_handler({}, None)
