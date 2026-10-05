-- Dimensions

CREATE TABLE dim_sensor (
    sensor_key BIGINT PRIMARY KEY,
    sensor_id VARCHAR(255) NOT NULL,
    sensor_name VARCHAR(255),
    sensor_type VARCHAR(100),
    status VARCHAR(50),
    is_current BOOLEAN DEFAULT TRUE,
    valid_from DATE,
    valid_to DATE
);

CREATE TABLE dim_mission (
    mission_key BIGINT PRIMARY KEY,
    mission_id BIGINT NOT NULL,
    mission_code VARCHAR(100),
    mission_status VARCHAR(50),
    priority VARCHAR(50)
);

CREATE TABLE dim_target_category (
    category_key INT PRIMARY KEY,
    category_id INT NOT NULL,
    category_name VARCHAR(100),
    threat_level VARCHAR(50)
);

CREATE TABLE dim_effector (
    effector_key BIGINT PRIMARY KEY,
    effector_id VARCHAR(255) NOT NULL,
    effector_name VARCHAR(255),
    effector_type VARCHAR(100),
    max_range_km NUMERIC(10, 2),
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE dim_operator (
    operator_key BIGINT PRIMARY KEY,
    operator_id VARCHAR(255) NOT NULL,
    command_post_unit VARCHAR(255),
    clearance_level VARCHAR(50)
);

CREATE TABLE dim_geography (
    geography_key BIGINT PRIMARY KEY,
    h3_index_r7 VARCHAR(50),
    geohash_6 VARCHAR(50),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    region_name VARCHAR(100)
);

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY, -- YYYYMMDD
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day_of_week INT NOT NULL
);

CREATE TABLE dim_time (
    time_key INT PRIMARY KEY, -- HHMMSS
    hour INT NOT NULL,
    minute INT NOT NULL,
    second INT NOT NULL
);


-- Facts

CREATE TABLE fact_threat_detections (
    detection_fact_id BIGINT PRIMARY KEY,
    sensor_key BIGINT REFERENCES dim_sensor(sensor_key),
    mission_key BIGINT REFERENCES dim_mission(mission_key),
    category_key INT REFERENCES dim_target_category(category_key),
    geography_key BIGINT REFERENCES dim_geography(geography_key),
    detected_date_key INT REFERENCES dim_date(date_key),
    edge_model_version VARCHAR(100),
    detection_id VARCHAR(255),
    confidence_score NUMERIC(5, 4),
    detection_count INT DEFAULT 1
);

CREATE TABLE fact_killchain_decisions (
    decision_fact_id BIGINT PRIMARY KEY,
    sensor_key BIGINT REFERENCES dim_sensor(sensor_key),
    mission_key BIGINT REFERENCES dim_mission(mission_key),
    category_key INT REFERENCES dim_target_category(category_key),
    effector_key BIGINT REFERENCES dim_effector(effector_key),
    operator_key BIGINT REFERENCES dim_operator(operator_key),
    decided_date_key INT REFERENCES dim_date(date_key),
    pairing_status VARCHAR(50),
    decision_type VARCHAR(100),
    operational_context TEXT,
    kill_chain_latency_ms INT,
    operator_response_latency_ms INT,
    total_end_to_end_latency_ms INT,
    is_authorized INT CHECK (is_authorized IN (0, 1)),
    is_overridden INT CHECK (is_overridden IN (0, 1))
);

CREATE TABLE fact_sensor_telemetry_snapshot (
    telemetry_snapshot_id BIGINT PRIMARY KEY,
    sensor_key BIGINT REFERENCES dim_sensor(sensor_key),
    mission_key BIGINT REFERENCES dim_mission(mission_key),
    geography_key BIGINT REFERENCES dim_geography(geography_key),
    snapshot_date_key INT REFERENCES dim_date(date_key),
    snapshot_time_key INT REFERENCES dim_time(time_key),
    avg_altitude_meters NUMERIC(10, 2),
    min_battery_bandwidth_pct NUMERIC(5, 2),
    network_uptime_seconds INT,
    telemetry_event_count INT DEFAULT 1
);