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
        # Float rounding errors can cause variance to evaluate to a tiny negative number
        # when all elements in 'readings' are identical; clamp to 0.0 before sqrt.
        variance = sum((x - mean) ** 2 for x in readings) / (n - 1)

        logger.info(f"Stream count: {n}, Calculated Mean: {mean}, Raw Variance: {variance}")

        # Defence-in-depth clamp: ensure any floating-point underflow never produces
        # a negative value passed to math.sqrt, which would raise ValueError.
        safe_variance = max(0.0, variance)

        std_dev = math.sqrt(safe_variance)

        if std_dev == 0:
            logger.warning(
                "std_dev is zero — all readings are identical; "
                "skipping z-score anomaly detection"
            )

        anomalies = []
        for val in readings:
            z_score = 0.0 if std_dev == 0 else (val - mean) / std_dev
            if abs(z_score) > self.threshold_sigma:
                anomalies.append(val)

        return {"mean": mean, "std_dev": std_dev, "anomalies": anomalies}


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logger.info("Processing IoT sensor window readings...")

    # Read readings from the Lambda event payload so the function processes
    # real sensor data rather than a hardcoded constant array.
    readings = event.get("readings", [])

    # Input validation: reject empty or non-list input with a 400 response.
    if not isinstance(readings, list) or len(readings) < 2:
        logger.error(
            "Invalid input: 'readings' must be a list with at least 2 numeric values. "
            "Received: %s", readings
        )
        return {
            "statusCode": 400,
            "error": "invalid_input",
            "detail": "'readings' must be a list of at least 2 numeric values.",
        }

    # Validate that every element is a real number.
    if not all(isinstance(v, (int, float)) for v in readings):
        logger.error("Invalid input: all values in 'readings' must be numeric.")
        return {
            "statusCode": 400,
            "error": "invalid_input",
            "detail": "All values in 'readings' must be numeric (int or float).",
        }

    detector = StatisticalAnomalyDetector(threshold_sigma=2.5)

    try:
        report = detector.evaluate_metric_stream(readings)
    except ValueError as exc:
        logger.error("Anomaly detection failed: %s", exc, exc_info=True)
        return {
            "statusCode": 500,
            "error": "anomaly_detection_failed",
            "detail": str(exc),
        }

    logger.info("Anomaly detection completed successfully")
    return {"statusCode": 200, "report": report}


if __name__ == "__main__":
    # Example invocation with non-constant readings for local testing.
    lambda_handler({"readings": [21.5, 22.0, 21.8, 23.1, 21.5]}, None)
