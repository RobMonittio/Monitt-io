
-- Monitt.io — TimescaleDB Schema
-- Run this once against your Railway PostgreSQL database
-- Requires TimescaleDB extension (add via Railway or run CREATE EXTENSION manually)

-- Enable TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ─────────────────────────────────────────
-- Core tables
-- ─────────────────────────────────────────

CREATE TABLE customers (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    email       TEXT,
    phone       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE buildings (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL REFERENCES customers(id),
    name        TEXT NOT NULL,
    address     TEXT,
    city        TEXT DEFAULT 'Santiago',
    country     TEXT DEFAULT 'CL',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE devices (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    building_id     UUID NOT NULL REFERENCES buildings(id),
    serial_number   TEXT UNIQUE NOT NULL,   -- TRB256 serial
    mqtt_username   TEXT NOT NULL,          -- MQTT auth username for this device
    mqtt_password   TEXT NOT NULL,          -- MQTT auth password (hashed in production)
    controller_brand TEXT DEFAULT 'DSE',   -- DSE, ComAp, etc.
    controller_model TEXT,                  -- e.g. DSE6020
    installed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────
-- Time-series readings (hypertable)
-- ─────────────────────────────────────────

CREATE TABLE readings (
    time                TIMESTAMPTZ     NOT NULL,
    device_id           UUID            NOT NULL REFERENCES devices(id),
    building_id         UUID            NOT NULL REFERENCES buildings(id),
    customer_id         UUID            NOT NULL REFERENCES customers(id),

    -- Engine
    engine_state        SMALLINT,       -- 0=stopped, 1=cranking, 2=warming up, 3=running, 4=cooling down
    engine_rpm          SMALLINT,       -- RPM
    coolant_temp_c      SMALLINT,       -- °C
    oil_pressure_kpa    SMALLINT,       -- kPa

    -- Fuel & battery
    fuel_level_pct      SMALLINT,       -- %
    battery_voltage_mv  INTEGER,        -- mV (e.g. 13200 = 13.2V)

    -- Generator output (L1)
    gen_frequency_hz    SMALLINT,       -- Hz * 10 (e.g. 500 = 50.0 Hz)
    gen_l1_voltage_mv   INTEGER,        -- mV
    gen_l1_current_ma   INTEGER,        -- mA
    gen_l1_watts        INTEGER,        -- W

    -- Alarms
    common_alarm        BOOLEAN DEFAULT FALSE,
    common_warning      BOOLEAN DEFAULT FALSE,

    -- AI scoring (populated by AI worker, not device)
    anomaly_score       REAL,           -- 0.0 to 1.0
    alert_tier          SMALLINT DEFAULT 0  -- 0=normal, 1=watch, 2=dispatch

);

-- Convert to TimescaleDB hypertable (partitions by time automatically)
SELECT create_hypertable('readings', 'time');

-- Indexes for fast per-device queries
CREATE INDEX ON readings (device_id, time DESC);
CREATE INDEX ON readings (building_id, time DESC);

-- Compress data older than 7 days (~10x space saving)
ALTER TABLE readings SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id'
);
SELECT add_compression_policy('readings', INTERVAL '7 days');

-- ─────────────────────────────────────────
-- Service history log
-- ─────────────────────────────────────────

CREATE TABLE service_logs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id       UUID NOT NULL REFERENCES devices(id),
    building_id     UUID NOT NULL REFERENCES buildings(id),
    technician_name TEXT,
    work_performed  TEXT,
    parts_replaced  TEXT,
    final_status    TEXT,
    serviced_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- AI scoring suppressed for 48hrs after service
    ai_suppressed_until TIMESTAMPTZ GENERATED ALWAYS AS (serviced_at + INTERVAL '48 hours') STORED
);

-- ─────────────────────────────────────────
-- Alerts
-- ─────────────────────────────────────────

CREATE TABLE alerts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id       UUID NOT NULL REFERENCES devices(id),
    building_id     UUID NOT NULL REFERENCES buildings(id),
    customer_id     UUID NOT NULL REFERENCES customers(id),
    tier            SMALLINT NOT NULL,  -- 1=watch, 2=dispatch
    parameter_name  TEXT,               -- e.g. 'oil_pressure_kpa'
    current_value   TEXT,
    normal_range    TEXT,
    fault_description TEXT,
    whatsapp_sent   BOOLEAN DEFAULT FALSE,
    acknowledged    BOOLEAN DEFAULT FALSE,
    dispatched      BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────
-- Jobs (technician dispatch)
-- ─────────────────────────────────────────

CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id        UUID REFERENCES alerts(id),
    device_id       UUID NOT NULL REFERENCES devices(id),
    building_id     UUID NOT NULL REFERENCES buildings(id),
    technician_name TEXT,
    status          TEXT NOT NULL DEFAULT 'assigned',  -- assigned, en_route, on_site, resolved
    job_description TEXT,
    parts_replaced  TEXT,
    final_status    TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);

-- ─────────────────────────────────────────
-- Row Level Security (prevents cross-customer data leaks)
-- ─────────────────────────────────────────

ALTER TABLE readings ENABLE ROW LEVEL SECURITY;
ALTER TABLE alerts ENABLE ROW LEVEL SECURITY;
ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;

-- Policies will be added when the API layer is built
-- For now, the processor uses a superuser connection that bypasses RLS
