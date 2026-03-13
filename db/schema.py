import duckdb
from config.settings import DB_PATH


def get_conn():
    return duckdb.connect(DB_PATH)


def get_read_conn():
    """Use this only in tests/scripts that need to read without the Go API.
    In production, the Go API opens DuckDB directly with access_mode=READ_ONLY."""
    return duckdb.connect(DB_PATH, read_only=True)


def initialize_schema():
    conn = get_conn()

    # Execute each statement individually — DuckDB has no executescript()
    statements = [

        # Raw ingest log — each row = one file pulled from source
        # UUID primary key is correct here: each raw pull is a unique event
        """
        CREATE TABLE IF NOT EXISTS raw_ingest (
            id               VARCHAR PRIMARY KEY,
            source_name      VARCHAR NOT NULL,
            source_type      VARCHAR NOT NULL,
            pulled_at        TIMESTAMPTZ NOT NULL,
            source_timestamp TIMESTAMPTZ,
            payload_path     VARCHAR NOT NULL,
            checksum         VARCHAR,
            status           VARCHAR NOT NULL,
            error_message    VARCHAR
        )
        """,

        # Core time series table
        # PRIMARY KEY is the natural composite key — enables correct ON CONFLICT dedup
        # Do NOT use UUID as primary key here
        """
        CREATE TABLE IF NOT EXISTS facts_time_series (
            source_name      VARCHAR NOT NULL,
            series_name      VARCHAR NOT NULL,
            region           VARCHAR NOT NULL DEFAULT 'US',
            observation_time TIMESTAMPTZ NOT NULL,
            release_time     TIMESTAMPTZ,
            ingest_time      TIMESTAMPTZ NOT NULL,
            value            DOUBLE,
            unit             VARCHAR,
            frequency        VARCHAR,
            quality_flag     VARCHAR DEFAULT 'ok',
            metadata_json    VARCHAR,
            PRIMARY KEY (source_name, series_name, region, observation_time)
        )
        """,

        # Computed features — one row per feature per day
        """
        CREATE TABLE IF NOT EXISTS features_daily (
            feature_date     DATE NOT NULL,
            feature_name     VARCHAR NOT NULL,
            region           VARCHAR NOT NULL DEFAULT 'US',
            value            DOUBLE,
            interpretation   VARCHAR,
            confidence       VARCHAR,
            computed_at      TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (feature_date, feature_name, region)
        )
        """,

        # Intraday features (for EIA-930, price)
        """
        CREATE TABLE IF NOT EXISTS features_intraday (
            ts               TIMESTAMPTZ NOT NULL,
            feature_name     VARCHAR NOT NULL,
            region           VARCHAR NOT NULL DEFAULT 'US',
            value            DOUBLE,
            delta_1h         DOUBLE,
            delta_24h        DOUBLE,
            PRIMARY KEY (ts, feature_name, region)
        )
        """,

        # Events and alerts
        """
        CREATE TABLE IF NOT EXISTS events (
            id               VARCHAR PRIMARY KEY,
            source_name      VARCHAR NOT NULL,
            event_type       VARCHAR NOT NULL,
            entity_name      VARCHAR,
            region           VARCHAR,
            event_time       TIMESTAMPTZ NOT NULL,
            severity         VARCHAR,
            description      VARCHAR,
            source_url       VARCHAR,
            raw_ref          VARCHAR,
            ingest_time      TIMESTAMPTZ NOT NULL
        )
        """,

        # Collector health — one row per source, updated on each run
        """
        CREATE TABLE IF NOT EXISTS collector_health (
            source_name          VARCHAR NOT NULL PRIMARY KEY,
            last_attempt         TIMESTAMPTZ NOT NULL,
            last_success         TIMESTAMPTZ,
            last_status          VARCHAR NOT NULL,
            consecutive_failures INTEGER DEFAULT 0,
            error_message        VARCHAR
        )
        """,

        # Catalyst calendar — mix of auto-computed and manual entries
        """
        CREATE TABLE IF NOT EXISTS catalyst_calendar (
            id               VARCHAR PRIMARY KEY,
            event_date       DATE NOT NULL,
            event_time_et    VARCHAR,
            event_type       VARCHAR NOT NULL,
            description      VARCHAR NOT NULL,
            impact           VARCHAR,
            is_auto          BOOLEAN DEFAULT FALSE,
            notes            VARCHAR
        )
        """,

        # Manual consensus inputs (storage draw estimates, etc.)
        """
        CREATE TABLE IF NOT EXISTS consensus_inputs (
            input_date       DATE NOT NULL,
            input_type       VARCHAR NOT NULL,
            value            DOUBLE,
            unit             VARCHAR,
            source_note      VARCHAR,
            entered_at       TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (input_date, input_type)
        )
        """,

        # Stored summaries
        """
        CREATE TABLE IF NOT EXISTS summary_outputs (
            summary_date     DATE NOT NULL,
            summary_type     VARCHAR NOT NULL,
            content          VARCHAR NOT NULL,
            inputs_hash      VARCHAR,
            generated_at     TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (summary_date, summary_type)
        )
        """,

        # Feature snapshots for historical analog finder (Feature 6).
        # One row per date — full feature vector as JSON.
        # Populated as a side-effect of the analog transform; backfilled by
        # scripts/backfill_history.py once enough history accumulates.
        """
        CREATE TABLE IF NOT EXISTS feature_snapshots (
            snapshot_date    DATE NOT NULL PRIMARY KEY,
            feature_vector   VARCHAR NOT NULL,
            created_at       TIMESTAMPTZ NOT NULL
        )
        """,

        # AIS vessel snapshot — one row per vessel per terminal, replaced every 5 min
        # by the Go cmd/ais binary. Enables per-vessel manifest display and EPI.
        # first_seen_at is preserved on conflict; dwell_minutes updated by the binary.
        """
        CREATE TABLE IF NOT EXISTS ais_vessels (
            mmsi          INTEGER NOT NULL,
            name          VARCHAR,
            terminal      VARCHAR NOT NULL,
            status        VARCHAR NOT NULL,
            lat           DOUBLE,
            lon           DOUBLE,
            sog           DOUBLE,
            nav_status    INTEGER,
            destination   VARCHAR,
            draught       DOUBLE,
            dwell_minutes INTEGER NOT NULL DEFAULT 0,
            first_seen_at TIMESTAMPTZ NOT NULL,
            observed_at   TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (mmsi, terminal)
        )
        """,

        # News wire — scored RSS headlines for nat-gas relevant events.
        # Written by collectors/news_wire.py every 15 minutes.
        # id = sha1(url)[:16]; tags = comma-separated matched keywords.
        """
        CREATE TABLE IF NOT EXISTS news_items (
            id           VARCHAR PRIMARY KEY,
            source       VARCHAR NOT NULL,
            title        VARCHAR NOT NULL,
            url          VARCHAR,
            published_at TIMESTAMPTZ,
            fetched_at   TIMESTAMPTZ NOT NULL,
            score        FLOAT NOT NULL DEFAULT 0,
            sentiment    VARCHAR NOT NULL DEFAULT 'neutral',
            tags         VARCHAR,
            implication  VARCHAR
        )
        """,

        # Pipeline events — OFOs, capacity constraints, maintenance windows.
        # Written by collectors/pipeline_ebb.py (Feature 1 Phase 3).
        # Also supports manual entry for known terminal maintenance.
        """
        CREATE TABLE IF NOT EXISTS pipeline_events (
            id                    VARCHAR PRIMARY KEY,
            pipeline_name         VARCHAR NOT NULL,
            event_type            VARCHAR NOT NULL,
            market_point          VARCHAR,
            effective_date        DATE NOT NULL,
            expiry_date           DATE,
            description           VARCHAR,
            capacity_impact_mmcfd DOUBLE,
            source_url            VARCHAR,
            ingest_time           TIMESTAMPTZ NOT NULL
        )
        """,

    ]

    for sql in statements:
        conn.execute(sql)

    conn.close()


if __name__ == "__main__":
    initialize_schema()
    print(f"Schema initialized at {DB_PATH}")
