import unittest

import pandas as pd

from dags.palantir_etl_transformations import (
    transform_decision_rows,
    transform_detection_rows,
    transform_telemetry_rows,
)


class DetectionTransformationTests(unittest.TestCase):
    def test_detected_at_is_converted_to_date_key(self):
        before = pd.DataFrame({"detected_at": ["2026-10-07 12:34:56"]})

        actual = transform_detection_rows(before)["detected_date_key"].tolist()

        self.assertEqual(actual, [20261007])

    def test_each_detection_gets_detection_count_one(self):
        before = pd.DataFrame({"detected_at": ["2026-10-07 12:34:56"]})

        actual = transform_detection_rows(before)["detection_count"].tolist()

        self.assertEqual(actual, [1])


class DecisionTransformationTests(unittest.TestCase):
    def test_decision_types_set_authorized_and_overridden_flags(self):
        before = pd.DataFrame(
            {
                "decided_at": ["2026-10-07 12:34:56"] * 3,
                "decision_type": ["AUTHORIZE", "OVERRIDE", "REJECT"],
            }
        )

        actual = transform_decision_rows(before)[
            ["is_authorized", "is_overridden"]
        ].values.tolist()

        self.assertEqual(actual, [[1, 0], [0, 1], [0, 0]])

    def test_decided_at_is_converted_to_date_key(self):
        before = pd.DataFrame(
            {
                "decided_at": ["2026-10-07 12:34:56"],
                "decision_type": ["AUTHORIZE"],
            }
        )

        actual = transform_decision_rows(before)["decided_date_key"].tolist()

        self.assertEqual(actual, [20261007])


class TelemetryTransformationTests(unittest.TestCase):
    def test_timestamp_is_converted_to_date_and_time_keys(self):
        before = pd.DataFrame(
            {
                "timestamp": ["2026-10-07 12:34:56"],
                "altitude_meters": [1250.5],
                "battery_bandwidth_pct": [87.25],
            }
        )

        actual = transform_telemetry_rows(before)[
            ["snapshot_date_key", "snapshot_time_key"]
        ].values.tolist()

        self.assertEqual(actual, [[20261007, 123456]])

    def test_telemetry_measures_are_mapped_and_event_count_is_one(self):
        before = pd.DataFrame(
            {
                "timestamp": ["2026-10-07 12:34:56"],
                "altitude_meters": [1250.5],
                "battery_bandwidth_pct": [87.25],
            }
        )

        actual = transform_telemetry_rows(before)[
            [
                "avg_altitude_meters",
                "min_battery_bandwidth_pct",
                "telemetry_event_count",
            ]
        ].values.tolist()

        self.assertEqual(actual, [[1250.5, 87.25, 1]])


if __name__ == "__main__":
    unittest.main()
