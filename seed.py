import datetime
import json
import os
import random
import sys
import time
import faker
import numpy as np
import psycopg2
from psycopg2.extras import execute_values

# Setup configuration from environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "palantir_oltp_pg")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASSWORD", "password")

fake = faker.Faker()
np.random.seed(42)
random.seed(42)


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def wait_for_tables(conn, timeout_seconds=60):
    """Wait for all required tables to exist in the database."""
    required_tables = [
        "edge_sensors",
        "mission_deployments",
        "target_categories",
        "effectors",
        "mission_sensors",
        "sensor_telemetry_logs",
        "threat_detections",
        "targeting_effector_pairings",
        "operator_decision_logs",
    ]
    
    cur = conn.cursor()
    start_time = time.time()
    
    while time.time() - start_time < timeout_seconds:
        try:
            cur.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = ANY(%s)
            """, (required_tables,))
            existing_tables = set(row[0] for row in cur.fetchall())
            
            if len(existing_tables) == len(required_tables):
                print(f"All {len(required_tables)} tables ready.")
                return True
            
            missing = set(required_tables) - existing_tables
            print(f"Waiting for tables: {missing}")
            time.sleep(1)
        except Exception as e:
            print(f"Checking tables: {e}")
            time.sleep(1)
    
    raise TimeoutError(f"Tables not ready after {timeout_seconds}s")


def seed_database():
    conn = get_db_connection()
    cur = conn.cursor()
    print("Connected to OLTP database. Waiting for tables to initialize...")
    
    # Wait for all tables to be created by init.sql
    wait_for_tables(conn)
    
    print("Truncating existing tables...")
    # Clear old data in transactional reverse dependency order
    cur.execute("""
        TRUNCATE TABLE 
            operator_decision_logs,
            targeting_effector_pairings,
            threat_detections,
            sensor_telemetry_logs,
            mission_sensors,
            effectors,
            target_categories,
            mission_deployments,
            edge_sensors
        RESTART IDENTITY CASCADE;
    """)
    conn.commit()

    # --- 1. Populate Master Tables ---
    print("Seeding master lookup tables...")

    # Edge Sensors (5 rows)
    sensors = [
        ("S-1", "Radar-Alpha", "SAT", "ACTIVE", -6.1754, 106.8272),
        ("S-2", "Drone-Eye-1", "DRONE", "ACTIVE", -6.2088, 106.8456),
        ("S-3", "Titan-Scan-A", "TITAN", "OFFLINE", -6.1931, 106.8229),
        ("S-4", "Sat-Vanguard", "SAT", "ACTIVE", -6.2297, 106.8091),
        ("S-5", "Drone-Eye-2", "DRONE", "ACTIVE", -6.1214, 106.7741),
    ]
    execute_values(
        cur,
        """
        INSERT INTO edge_sensors (sensor_id, sensor_name, sensor_type, status, last_known_latitude, last_known_longitude) 
        VALUES %s
        """,
        sensors,
    )
    conn.commit()

    # Target Categories (5 rows)
    categories = [
        ("UAV", "HIGH", "Unmanned aerial threat"),
        ("ARMORED_VEHICLE", "MEDIUM", "Ground mechanized unit"),
        ("NAVAL_VESSEL", "CRITICAL", "Surface maritime target"),
        ("INFANTRY", "LOW", "Personnel movement"),
        ("MISSILE", "CRITICAL", "Ballistic or cruise trajectory"),
    ]
    cur.executemany(
        """
        INSERT INTO target_categories (category_name, threat_level, description) 
        VALUES (%s, %s, %s)
        """,
        categories,
    )
    conn.commit()
    
    # Retrieve category_ids after commit
    cur.execute("SELECT category_id FROM target_categories ORDER BY category_id")
    category_ids = [row[0] for row in cur.fetchall()]

    # Effectors (5 rows)
    effectors = [
        ("E-101", "Iron Dome Alpha", "JAMMING", 70.00, "READY"),
        ("E-102", "Thunderbolt Howitzer", "ARTILLERY", 40.00, "READY"),
        ("E-103", "Scorpion Jammer", "JAMMING", 15.00, "READY"),
        ("E-105", "Reaper Strike UAV", "DRONE", 150.00, "ENGAGED"),
        ("E-106", "Phalanx Defense System", "ARTILLERY", 5.50, "OFFLINE"),
    ]
    execute_values(
        cur,
        """
        INSERT INTO effectors (effector_id, effector_name, effector_type, max_range_km, status) 
        VALUES %s
        """,
        effectors,
    )
    conn.commit()

    # --- 2. Populate Transactional Tables (1,000+ rows each) ---
    print("Seeding transaction & log tables (>= 1,000 records each)...")

    sensor_ids = [s[0] for s in sensors]
    effector_ids = [e[0] for e in effectors]
    start_base = datetime.datetime(2026, 8, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)

    # 1. Mission Deployments (1,050 records)
    missions = []
    for i in range(1, 1051):
        prefix = random.choice(["M-", "OP-", "RECON-", "PATROL-", "TASK-"])
        start = start_base + datetime.timedelta(days=random.randint(0, 30))
        end = start + datetime.timedelta(days=random.randint(1, 15))
        
        geojson = json.dumps({
            "type": "Polygon",
            "coordinates": [[
                [106.8000, -6.2000], [106.8500, -6.2000],
                [106.8500, -6.2500], [106.8000, -6.2500],
                [106.8000, -6.2000]
            ]]
        })
        
        missions.append((
            f"{prefix}{i:05d}",
            geojson,
            start,
            end,
            random.choice(["ONGOING", "COMPLETED"]),
            random.choice(["LOW", "MEDIUM", "HIGH", "CRITICAL"]),
        ))
    
    cur.executemany(
        """
        INSERT INTO mission_deployments (mission_code, area_of_responsibility_geojson, start_time, end_time, mission_status, priority) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        missions,
    )
    conn.commit()
    
    # Retrieve mission_ids after commit
    cur.execute("SELECT mission_id FROM mission_deployments ORDER BY mission_id")
    mission_ids = [row[0] for row in cur.fetchall()]

    # 2. Mission Sensors Assignment (~2,100 records)
    mission_sensors = []
    for m_id in mission_ids:
        assigned_s = random.sample(sensor_ids, k=random.randint(1, 3))
        for s_id in assigned_s:
            mission_sensors.append((
                m_id,
                s_id,
                start_base + datetime.timedelta(hours=random.randint(1, 24)),
                random.choice(["PRIMARY", "RECON", "BACKUP"])
            ))
    execute_values(
        cur,
        """
        INSERT INTO mission_sensors (mission_id, sensor_id, assigned_at, assignment_role) 
        VALUES %s
        """,
        mission_sensors,
    )
    conn.commit()

    # 3. Sensor Telemetry Logs (1,500 records)
    telemetry_logs = []
    for _ in range(1500):
        lat = float(np.round(np.random.normal(loc=-6.2088, scale=0.15), 6))
        lon = float(np.round(np.random.normal(loc=106.8456, scale=0.15), 6))
        ts = start_base + datetime.timedelta(minutes=random.randint(1, 40000))
        telemetry_logs.append((
            random.choice(sensor_ids),
            ts,
            float(np.round(random.uniform(100.0, 12000.0), 2)),
            float(np.round(random.uniform(20.0, 100.0), 2)),
            lat,
            lon,
            random.choice([True, True, True, False]),
        ))
    execute_values(
        cur,
        """
        INSERT INTO sensor_telemetry_logs (sensor_id, timestamp, altitude_meters, battery_bandwidth_pct, latitude, longitude, network_connected) 
        VALUES %s
        """,
        telemetry_logs,
    )
    conn.commit()

    # 4. Threat Detections (1,200 records)
    confidence_scores = np.random.beta(a=5, b=2, size=1200)
    threat_detections = []
    for i in range(1200):
        lat = float(np.round(np.random.normal(loc=-6.2088, scale=0.15), 6))
        lon = float(np.round(np.random.normal(loc=106.8456, scale=0.15), 6))
        det_time = start_base + datetime.timedelta(minutes=random.randint(1, 40000))
        bbox = json.dumps({"xmin": 10, "ymin": 20, "xmax": 50, "ymax": 60})
        
        threat_detections.append((
            random.choice(sensor_ids),
            random.choice(mission_ids),
            random.choice(category_ids),
            det_time,
            float(np.round(confidence_scores[i], 4)),
            lat,
            lon,
            bbox,
            random.choice(["v1.0.0", "v1.1.2-beta", "v1.2.1", "v2.0.0"]),
        ))

    # Pass fetch=True to retrieve all 1,200 inserted rows across batches
    inserted_detections = execute_values(
        cur,
        """
        INSERT INTO threat_detections (sensor_id, mission_id, category_id, detected_at, confidence_score, latitude, longitude, bounding_box_json, edge_model_version) 
        VALUES %s RETURNING detection_id, detected_at, latitude, longitude
        """,
        threat_detections,
        fetch=True,
    )
    conn.commit()

    # 5. Targeting Effector Pairings (1,200 records)
    pairings = []
    kc_latencies = np.random.lognormal(mean=4.2, sigma=0.5, size=len(inserted_detections)).astype(int)

    for idx, (det_id, det_time, lat, lon) in enumerate(inserted_detections):
        paired_time = det_time + datetime.timedelta(milliseconds=int(kc_latencies[idx]))
        pairings.append((
            det_id,
            random.choice(effector_ids),
            paired_time,
            int(kc_latencies[idx]),
            lat,
            lon,
            random.choice(["RECOMMENDED", "EXECUTED"]),
        ))

    # Pass fetch=True to retrieve all 1,200 inserted rows across batches
    inserted_pairings = execute_values(
        cur,
        """
        INSERT INTO targeting_effector_pairings (detection_id, effector_id, paired_at, kill_chain_latency_ms, target_latitude, target_longitude, pairing_status) 
        VALUES %s RETURNING pairing_id, paired_at
        """,
        pairings,
        fetch=True,
    )
    conn.commit()

    # 6. Operator Decision Logs (1,200 records)
    decisions = []
    for pairing_id, paired_time in inserted_pairings:
        decided_time = paired_time + datetime.timedelta(seconds=random.randint(2, 45))
        cp_lat = float(np.round(np.random.normal(loc=-6.2088, scale=0.05), 6))
        cp_lon = float(np.round(np.random.normal(loc=106.8456, scale=0.05), 6))
        
        decisions.append((
            pairing_id,
            f"OP-{random.randint(1, 50):03d}",
            random.choice(["AUTHORIZE", "OVERRIDE", "REJECT"]),
            decided_time,
            f"Target verified | WX: {random.choice(['Clear', 'Rain', 'Fog'])}",
            cp_lat,
            cp_lon,
        ))

    execute_values(
        cur,
        """
        INSERT INTO operator_decision_logs (pairing_id, operator_id, decision_type, decided_at, operational_context, command_post_latitude, command_post_longitude) 
        VALUES %s
        """,
        decisions,
    )

    conn.commit()
    cur.close()
    conn.close()
    print("OLTP database seeding completed successfully!")


if __name__ == "__main__":
    try:
        seed_database()
    except Exception as e:
        print(f"Error seeding OLTP database: {e}", file=sys.stderr)
        sys.exit(1)