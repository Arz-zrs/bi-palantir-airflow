-- PostGIS extension not available in postgres:16-alpine image
-- CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE edge_sensors (
    sensor_id VARCHAR(255) PRIMARY KEY,
    sensor_name VARCHAR(255),
    sensor_type VARCHAR(50) CHECK (sensor_type IN ('SAT', 'DRONE', 'TITAN')),
    status VARCHAR(50) CHECK (status IN ('ACTIVE', 'OFFLINE')),
    last_known_latitude DOUBLE PRECISION,
    last_known_longitude DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE mission_deployments (
    mission_id BIGSERIAL PRIMARY KEY,
    mission_code VARCHAR(255) UNIQUE NOT NULL,
    area_of_responsibility_geojson JSONB,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    mission_status VARCHAR(50) CHECK (mission_status IN ('ONGOING', 'COMPLETED')),
    priority VARCHAR(50)
);

CREATE TABLE target_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(255) UNIQUE NOT NULL,
    threat_level VARCHAR(50) CHECK (threat_level IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    description TEXT
);

CREATE TABLE effectors (
    effector_id VARCHAR(255) PRIMARY KEY,
    effector_name VARCHAR(255),
    effector_type VARCHAR(50) CHECK (effector_type IN ('ARTILLERY', 'DRONE', 'JAMMING')),
    max_range_km DOUBLE PRECISION,
    status VARCHAR(50) CHECK (status IN ('READY', 'ENGAGED', 'OFFLINE'))
);

CREATE TABLE mission_sensors (
    mission_sensor_id BIGSERIAL PRIMARY KEY,
    mission_id BIGINT REFERENCES mission_deployments(mission_id) ON DELETE CASCADE,
    sensor_id VARCHAR(255) REFERENCES edge_sensors(sensor_id) ON DELETE CASCADE,
    assigned_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    unassigned_at TIMESTAMPTZ,
    assignment_role VARCHAR(50) CHECK (assignment_role IN ('PRIMARY', 'RECON', 'BACKUP'))
);

CREATE TABLE sensor_telemetry_logs (
    telemetry_id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(255) REFERENCES edge_sensors(sensor_id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    altitude_meters NUMERIC,
    battery_bandwidth_pct NUMERIC,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    network_connected BOOLEAN
);

CREATE TABLE threat_detections (
    detection_id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(255) REFERENCES edge_sensors(sensor_id) ON DELETE SET NULL,
    mission_id BIGINT REFERENCES mission_deployments(mission_id) ON DELETE SET NULL,
    category_id INT REFERENCES target_categories(category_id) ON DELETE SET NULL,
    detected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    confidence_score NUMERIC CHECK (confidence_score BETWEEN 0 AND 1),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    bounding_box_json JSONB,
    edge_model_version VARCHAR(100)
);

CREATE TABLE targeting_effector_pairings (
    pairing_id BIGSERIAL PRIMARY KEY,
    detection_id BIGINT REFERENCES threat_detections(detection_id) ON DELETE CASCADE,
    effector_id VARCHAR(255) REFERENCES effectors(effector_id) ON DELETE CASCADE,
    paired_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    kill_chain_latency_ms INT,
    target_latitude DOUBLE PRECISION,
    target_longitude DOUBLE PRECISION,
    pairing_status VARCHAR(50) CHECK (pairing_status IN ('RECOMMENDED', 'EXECUTED'))
);

CREATE TABLE operator_decision_logs (
    decision_id BIGSERIAL PRIMARY KEY,
    pairing_id BIGINT REFERENCES targeting_effector_pairings(pairing_id) ON DELETE CASCADE,
    operator_id VARCHAR(255) NOT NULL,
    decision_type VARCHAR(50) CHECK (decision_type IN ('AUTHORIZE', 'OVERRIDE', 'REJECT')),
    decided_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    operational_context TEXT,
    command_post_latitude DOUBLE PRECISION,
    command_post_longitude DOUBLE PRECISION
);

CREATE INDEX idx_telemetry_sensor_time ON sensor_telemetry_logs (sensor_id, timestamp DESC);
CREATE INDEX idx_detections_mission_time ON threat_detections (mission_id, detected_at DESC);
CREATE INDEX idx_pairings_detection ON targeting_effector_pairings (detection_id);
