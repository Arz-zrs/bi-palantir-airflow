import pandas as pd


def transform_detection_rows(detections: pd.DataFrame) -> pd.DataFrame:
    transformed = detections.copy()
    transformed["detected_date_key"] = (
        pd.to_datetime(transformed["detected_at"]).dt.strftime("%Y%m%d").astype(int)
    )
    transformed["detection_count"] = 1
    return transformed


def transform_decision_rows(decisions: pd.DataFrame) -> pd.DataFrame:
    transformed = decisions.copy()
    transformed["decided_date_key"] = (
        pd.to_datetime(transformed["decided_at"]).dt.strftime("%Y%m%d").astype(int)
    )
    transformed["is_authorized"] = transformed["decision_type"].eq("AUTHORIZE").astype(int)
    transformed["is_overridden"] = transformed["decision_type"].eq("OVERRIDE").astype(int)
    return transformed


def transform_telemetry_rows(telemetry: pd.DataFrame) -> pd.DataFrame:
    transformed = telemetry.copy()
    timestamps = pd.to_datetime(transformed["timestamp"])
    transformed["snapshot_date_key"] = timestamps.dt.strftime("%Y%m%d").astype(int)
    transformed["snapshot_time_key"] = timestamps.dt.strftime("%H%M%S").astype(int)
    transformed["avg_altitude_meters"] = transformed["altitude_meters"]
    transformed["min_battery_bandwidth_pct"] = transformed["battery_bandwidth_pct"]
    transformed["telemetry_event_count"] = 1
    return transformed
