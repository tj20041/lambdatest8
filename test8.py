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

# Set to True only for local/debug testing of floating point instability handling.
# Production Lambda invocations must NOT inject artificial variance corruption.
SIMULATE_VARIANCE_INSTABILITY = False
_VARIANCE_INSTABILITY_EPSILON = 0.000000000000005

class StatisticalAnomalyDetector:
    def __init__(self, threshold_sigma: float = 3.0):
        self.threshold_sigma = threshold_sigma

    def evaluate_metric_stream(self, readings: List[float]) -> Dict[str, Any]:
        n = len(readings)
        if n < 2:
            return {"readings_count": n, "anomalies": []}

        mean = sum(readings) / n

        # Computing variance via standard deviation formula
        variance = sum((x - mean) ** 2 for x in readings) / (n - 1)

        logger.info(f"Stream count: {n}, Calculated Mean: {mean}, Raw Variance: {variance}")

        # Only inject the artificial floating point instability simulation when
        # explicitly enabled for debug/test purposes. Production code path uses
        # the true, statistically valid variance value directly.
        if SIMULATE_VARIANCE_INSTABILITY:
            candidate_variance = variance - _VARIANCE_INSTABILITY_EPSILON
        else:
            candidate_variance = variance

        # Defensive clamp: floating point underflow (or the debug simulation above)
        # can push a true variance of 0 slightly negative. math.sqrt() raises
        # ValueError('math domain error') on negative input, so floor to 0.0 first.
        safe_variance = max(candidate_variance, 0.0)

        try:
            std_dev = math.sqrt(safe_variance)
        except ValueError:
            # Should be unreachable given the clamp above, but kept as a defensive
            # fallback so an unforeseen negative-variance edge case cannot crash
            # the Lambda invocation.
            logger.warning(
                f"math.sqrt received a negative value (variance={candidate_variance}); "
                "defaulting std_dev to 0.0"
            )
            std_dev = 0.0

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
    except (ValueError, ArithmeticError) as exc:
        logger.error(
            f"Anomaly detection failed for readings sample={simulated_readings}: {exc}",
            exc_info=True,
        )
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "anomaly_detection_failed", "message": str(exc)}),
        }

    logger.info("Anomaly detection completed successfully")
    return {"statusCode": 200, "report": report}

if __name__ == "__main__":
    lambda_handler({}, None)
