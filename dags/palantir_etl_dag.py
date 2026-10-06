from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def get_db_engines():
    oltp_hook = PostgresHook(postgres_conn_id="oltp_conn")
    dw_hook = PostgresHook(postgres_conn_id="dw_conn")
    return oltp_hook.get_sqlalchemy_engine(), dw_hook.get_sqlalchemy_engine()

# -------------------------------------------------------------
# DIMENSION TASKS
# -------------------------------------------------------------
def load_dim_date():
    _, dw_engine = get_db_engines()
    dates = pd.date_range(start="2025-01-01", end="2027-12-31", freq="D")
    df = pd.DataFrame({
        "date_key": dates.strftime("%Y%m%d").astype(int),
        "full_date": dates.date,
        "year": dates.year,
        "quarter": dates.quarter,
        "month": dates.month,
        "day_of_week": dates.dayofweek + 1,
    })
    with dw_engine.begin() as conn:
        df.to_sql("dim_date", conn, if_exists="append", index=False, method="multi")

def load_dim_time():
    _, dw_engine = get_db_engines()
    times = pd.date_range(start="00:00:00", end="23:59:59", freq="s")
    df = pd.DataFrame({
        "time_key": times.strftime("%H%M%S").astype(int),
        "hour": times.hour,
        "minute": times.minute,
        "second": times.second,
    })
    with dw_engine.begin() as conn:
        df.to_sql("dim_time", conn, if_exists="append", index=False, chunksize=10000, method="multi")

def load_dim_sensor():
    oltp_engine, dw_engine = get_db_engines()
    query = """
        SELECT 
            ROW_NUMBER() OVER (ORDER BY sensor_id) AS sensor_key,
            sensor_id,
            sensor_name,
            sensor_type,
            status,
            TRUE AS is_current,
            CURRENT_DATE AS valid_from,
            NULL::date AS valid_to
        FROM edge_sensors;
    """
    df = pd.read_sql(query, oltp_engine)
    with dw_engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE dim_sensor CASCADE;")
        df.to_sql("dim_sensor", conn, if_exists="append", index=False)

def load_dim_mission():
    oltp_engine, dw_engine = get_db_engines()
    query = """
        SELECT 
            mission_id AS mission_key,
            mission_id,
            mission_code,
            mission_status,
            priority
        FROM mission_deployments;
    """
    df = pd.read_sql(query, oltp_engine)
    with dw_engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE dim_mission CASCADE;")
        df.to_sql("dim_mission", conn, if_exists="append", index=False)

def load_dim_target_category():
    oltp_engine, dw_engine = get_db_engines()
    query = """
        SELECT 
            category_id AS category_key,
            category_id,
            category_name,
            threat_level
        FROM target_categories;
    """
    df = pd.read_sql(query, oltp_engine)
    with dw_engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE dim_target_category CASCADE;")
        df.to_sql("dim_target_category", conn, if_exists="append", index=False)

def load_dim_effector():
    oltp_engine, dw_engine = get_db_engines()
    query = """
        SELECT 
            ROW_NUMBER() OVER (ORDER BY effector_id) AS effector_key,
            effector_id,
            effector_name,
            effector_type,
            max_range_km,
            TRUE AS is_current
        FROM effectors;
    """
    df = pd.read_sql(query, oltp_engine)
    with dw_engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE dim_effector CASCADE;")
        df.to_sql("dim_effector", conn, if_exists="append", index=False)

def load_dim_operator():
    oltp_engine, dw_engine = get_db_engines()
    query = """
        SELECT 
            ROW_NUMBER() OVER (ORDER BY operator_id) AS operator_key,
            operator_id,
            'Central Operations' AS command_post_unit,
            'TOP_SECRET' AS clearance_level
        FROM (SELECT DISTINCT operator_id FROM operator_decision_logs) ops;
    """
    df = pd.read_sql(query, oltp_engine)
    with dw_engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE dim_operator CASCADE;")
        df.to_sql("dim_operator", conn, if_exists="append", index=False)

def load_dim_geography():
    oltp_engine, dw_engine = get_db_engines()
    # Extract distinct locations across detections, effectors, and operators
    query = """
        WITH points AS (
            SELECT latitude, longitude FROM threat_detections WHERE latitude IS NOT NULL
            UNION
            SELECT target_latitude, target_longitude FROM targeting_effector_pairings WHERE target_latitude IS NOT NULL
            UNION
            SELECT command_post_latitude, command_post_longitude FROM operator_decision_logs WHERE command_post_latitude IS NOT NULL
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY latitude, longitude) AS geography_key,
            NULL::varchar AS h3_index_r7,
            NULL::varchar AS geohash_6,
            latitude,
            longitude,
            'THEATER_GLOBAL' AS region_name
        FROM points;
    """
    df = pd.read_sql(query, oltp_engine)
    with dw_engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE dim_geography CASCADE;")
        df.to_sql("dim_geography", conn, if_exists="append", index=False)

# -------------------------------------------------------------
# FACT TASKS
# -------------------------------------------------------------
def load_fact_threat_detections():
    oltp_engine, dw_engine = get_db_engines()
    # Lookup keys from loaded dimensions
    query = """
        SELECT 
            td.detection_id AS detection_fact_id,
            s.sensor_key,
            m.mission_key,
            c.category_key,
            g.geography_key,
            TO_CHAR(td.detected_at, 'YYYYMMDD')::INT AS detected_date_key,
            td.edge_model_version,
            td.detection_id::VARCHAR(255) AS detection_id,
            td.confidence_score,
            1 AS detection_count
        FROM threat_detections td
        LEFT JOIN dim_sensor s ON td.sensor_id = s.sensor_id
        LEFT JOIN dim_mission m ON td.mission_id = m.mission_id
        LEFT JOIN dim_target_category c ON td.category_id = c.category_id
        LEFT JOIN dim_geography g ON td.latitude = g.latitude AND td.longitude = g.longitude;
    """
    # Fetch data using DW context for surrogate joins
    # First, transfer raw detections into a staging mechanism or execute direct cross-query
    detections = pd.read_sql("SELECT * FROM threat_detections", oltp_engine)
    dim_s = pd.read_sql("SELECT sensor_key, sensor_id FROM dim_sensor", dw_engine)
    dim_m = pd.read_sql("SELECT mission_key, mission_id FROM dim_mission", dw_engine)
    dim_c = pd.read_sql("SELECT category_key, category_id FROM dim_target_category", dw_engine)
    dim_g = pd.read_sql("SELECT geography_key, latitude, longitude FROM dim_geography", dw_engine)

    merged = detections.merge(dim_s, on="sensor_id", how="left") \
                       .merge(dim_m, on="mission_id", how="left") \
                       .merge(dim_c, on="category_id", how="left") \
                       .merge(dim_g, on=["latitude", "longitude"], how="left")

    merged["detection_fact_id"] = merged["detection_id"]
    merged["detected_date_key"] = pd.to_datetime(merged["detected_at"]).dt.strftime("%Y%m%d").astype(int)
    merged["detection_count"] = 1

    fact_df = merged[[
        "detection_fact_id", "sensor_key", "mission_key", "category_key",
        "geography_key", "detected_date_key", "edge_model_version",
        "detection_id", "confidence_score", "detection_count"
    ]]

    with dw_engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE fact_threat_detections CASCADE;")
        fact_df.to_sql("fact_threat_detections", conn, if_exists="append", index=False)

def load_fact_killchain_decisions():
    oltp_engine, dw_engine = get_db_engines()
    
    query = """
        SELECT 
            odl.decision_id AS decision_fact_id,
            tep.detection_id,
            tep.effector_id,
            odl.operator_id,
            odl.decided_at,
            odl.decision_type,
            odl.operational_context,
            tep.pairing_status,
            tep.kill_chain_latency_ms,
            EXTRACT(EPOCH FROM (odl.decided_at - tep.paired_at)) * 1000 AS operator_response_latency_ms,
            tep.kill_chain_latency_ms + (EXTRACT(EPOCH FROM (odl.decided_at - tep.paired_at)) * 1000) AS total_end_to_end_latency_ms,
            CASE WHEN odl.decision_type = 'AUTHORIZE' THEN 1 ELSE 0 END AS is_authorized,
            CASE WHEN odl.decision_type = 'OVERRIDE' THEN 1 ELSE 0 END AS is_overridden
        FROM operator_decision_logs odl
        JOIN targeting_effector_pairings tep ON odl.pairing_id = tep.pairing_id;
    """
    decisions = pd.read_sql(query, oltp_engine)
    detections = pd.read_sql("SELECT detection_id, sensor_id, mission_id, category_id FROM threat_detections", oltp_engine)
    
    dim_s = pd.read_sql("SELECT sensor_key, sensor_id FROM dim_sensor", dw_engine)
    dim_m = pd.read_sql("SELECT mission_key, mission_id FROM dim_mission", dw_engine)
    dim_c = pd.read_sql("SELECT category_key, category_id FROM dim_target_category", dw_engine)
    dim_e = pd.read_sql("SELECT effector_key, effector_id FROM dim_effector", dw_engine)
    dim_o = pd.read_sql("SELECT operator_key, operator_id FROM dim_operator", dw_engine)

    merged = decisions.merge(detections, on="detection_id", how="left") \
                      .merge(dim_s, on="sensor_id", how="left") \
                      .merge(dim_m, on="mission_id", how="left") \
                      .merge(dim_c, on="category_id", how="left") \
                      .merge(dim_e, on="effector_id", how="left") \
                      .merge(dim_o, on="operator_id", how="left")

    merged["decided_date_key"] = pd.to_datetime(merged["decided_at"]).dt.strftime("%Y%m%d").astype(int)

    fact_df = merged[[
        "decision_fact_id", "sensor_key", "mission_key", "category_key",
        "effector_key", "operator_key", "decided_date_key", "pairing_status",
        "decision_type", "operational_context", "kill_chain_latency_ms",
        "operator_response_latency_ms", "total_end_to_end_latency_ms",
        "is_authorized", "is_overridden"
    ]]

    with dw_engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE fact_killchain_decisions CASCADE;")
        fact_df.to_sql("fact_killchain_decisions", conn, if_exists="append", index=False)

def load_fact_telemetry_snapshot():
    oltp_engine, dw_engine = get_db_engines()
    
    query = """
        SELECT 
            ROW_NUMBER() OVER () AS telemetry_snapshot_id,
            t.sensor_id,
            ms.mission_id,
            t.latitude,
            t.longitude,
            t.timestamp,
            t.altitude_meters,
            t.battery_bandwidth_pct,
            CASE WHEN t.network_connected THEN 60 ELSE 0 END AS network_uptime_seconds
        FROM sensor_telemetry_logs t
        LEFT JOIN mission_sensors ms ON t.sensor_id = ms.sensor_id;
    """
    telemetry = pd.read_sql(query, oltp_engine)

    dim_s = pd.read_sql("SELECT sensor_key, sensor_id FROM dim_sensor", dw_engine)
    dim_m = pd.read_sql("SELECT mission_key, mission_id FROM dim_mission", dw_engine)
    dim_g = pd.read_sql("SELECT geography_key, latitude, longitude FROM dim_geography", dw_engine)

    merged = telemetry.merge(dim_s, on="sensor_id", how="left") \
                      .merge(dim_m, on="mission_id", how="left") \
                      .merge(dim_g, on=["latitude", "longitude"], how="left")

    merged["snapshot_date_key"] = pd.to_datetime(merged["timestamp"]).dt.strftime("%Y%m%d").astype(int)
    merged["snapshot_time_key"] = pd.to_datetime(merged["timestamp"]).dt.strftime("%H%M%S").astype(int)
    merged["avg_altitude_meters"] = merged["altitude_meters"]
    merged["min_battery_bandwidth_pct"] = merged["battery_bandwidth_pct"]
    merged["telemetry_event_count"] = 1

    fact_df = merged[[
        "telemetry_snapshot_id", "sensor_key", "mission_key", "geography_key",
        "snapshot_date_key", "snapshot_time_key", "avg_altitude_meters",
        "min_battery_bandwidth_pct", "network_uptime_seconds", "telemetry_event_count"
    ]]

    with dw_engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE fact_sensor_telemetry_snapshot CASCADE;")
        fact_df.to_sql("fact_sensor_telemetry_snapshot", conn, if_exists="append", index=False)


# -------------------------------------------------------------
# DAG DEFINITION & DEPENDENCIES
# -------------------------------------------------------------
with DAG(
    dag_id="palantir_star_schema_etl",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["palantir", "etl"],
) as dag:

    # Date/Time Dimensions (can run independently)
    t_dim_date = PythonOperator(task_id="dim_date", python_callable=load_dim_date)
    t_dim_time = PythonOperator(task_id="dim_time", python_callable=load_dim_time)

    # Entity Dimensions
    t_dim_sensor = PythonOperator(task_id="dim_sensor", python_callable=load_dim_sensor)
    t_dim_mission = PythonOperator(task_id="dim_mission", python_callable=load_dim_mission)
    t_dim_category = PythonOperator(task_id="dim_target_category", python_callable=load_dim_target_category)
    t_dim_effector = PythonOperator(task_id="dim_effector", python_callable=load_dim_effector)
    t_dim_operator = PythonOperator(task_id="dim_operator", python_callable=load_dim_operator)
    t_dim_geography = PythonOperator(task_id="dim_geography", python_callable=load_dim_geography)

    dim_tasks = [
        t_dim_date,
        t_dim_time,
        t_dim_sensor,
        t_dim_mission,
        t_dim_category,
        t_dim_effector,
        t_dim_operator,
        t_dim_geography,
    ]

    # Fact Tables
    t_fact_detections = PythonOperator(task_id="fact_threat_detections", python_callable=load_fact_threat_detections)
    t_fact_decisions = PythonOperator(task_id="fact_killchain_decisions", python_callable=load_fact_killchain_decisions)
    t_fact_telemetry = PythonOperator(task_id="fact_sensor_telemetry_snapshot", python_callable=load_fact_telemetry_snapshot)

    # Dependency: All dimensions must finish before any facts start
    dim_tasks >> t_fact_detections
    dim_tasks >> t_fact_decisions
    dim_tasks >> t_fact_telemetry