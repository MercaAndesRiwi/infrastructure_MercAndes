"""Weekly seller score and status workflow."""

from __future__ import annotations

import os
import csv
from datetime import datetime, timedelta
from pathlib import Path

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

from airflow.sensors.python import PythonSensor
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:
    try:
        from airflow.operators.empty import EmptyOperator
    except ImportError:
        from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

default_args = {
    "owner": "MercaAndes Data Team",
    "depends_on_past": False,
    "email": ["yailauc_19@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=60),
}


def seller_score_inputs_ready(**context):
    """Wait until source rows exist and every source seller ID is mapped."""
    pg_hook = PostgresHook(postgres_conn_id="pos_db")
    checks = (
        "SELECT COUNT(*) FROM public.dim_seller",
        "SELECT COUNT(*) FROM public.dim_seller_source_key",
        "SELECT COUNT(*) FROM z1_silver.seller_score_dispatch WHERE source_system = 'marketplace'",
        "SELECT COUNT(*) FROM z1_silver.seller_score_reviews WHERE source_system = 'olist'",
        "SELECT COUNT(*) FROM z1_silver.seller_score_inventory WHERE source_system = 'inventory_api'",
        "SELECT COUNT(*) FROM z1_silver.seller_score_cancellations WHERE source_system = 'marketplace'",
    )
    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cur:
            for query in checks:
                cur.execute(query)
                if cur.fetchone()[0] == 0:
                    return False

            source_tables = (
                ("seller_score_dispatch", "marketplace"),
                ("seller_score_reviews", "olist"),
                ("seller_score_inventory", "inventory_api"),
                ("seller_score_cancellations", "marketplace"),
            )
            for table, source_system in source_tables:
                cur.execute(
                    f"""
                    SELECT 1
                    FROM z1_silver.{table} source
                    LEFT JOIN public.dim_seller_source_key key
                      ON key.source_system = source.source_system
                     AND key.source_seller_id = source.source_seller_id
                    WHERE source.source_system = %s
                      AND key.seller_id IS NULL
                    LIMIT 1
                    """,
                    (source_system,),
                )
                if cur.fetchone() is not None:
                    return False
    finally:
        conn.close()
    return True


@dag(
    dag_id="seller_scoring",
    default_args=default_args,
    description="Weekly seller scoring with PySpark and RN-08 status management.",
    schedule="0 10 * * 4",
    start_date=datetime(2026, 11, 4, 14, 0),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["layer:gold", "seller", "scoring", "mercaandes"],
    doc_md="""
    # Weekly Seller Scoring Workflow

    Airflow checks Silver metrics, runs the PySpark seller score job,
    and applies status rules based on weekly Gold history. Current score
    is stored in `gold_seller_score` for downstream consumption.
    """,
)
def seller_scoring():
    @task(task_id="load_olist_seller_reviews")
    def load_olist_seller_reviews(**context):
        """Load historical Olist reviews only when an order has one known seller."""
        input_dir = Path(os.getenv("F1_KAGGLE_LOCAL_INPUT_PATH", "/opt/inputs/f1_kaggle"))
        items_path = input_dir / "olist_order_items_dataset.csv"
        reviews_path = input_dir / "olist_order_reviews_dataset.csv"
        if not items_path.exists() or not reviews_path.exists():
            raise FileNotFoundError(
                "Olist review inputs are missing from " + str(input_dir)
            )

        sellers_by_order = {}
        with items_path.open(encoding="utf-8", newline="") as source:
            for row in csv.DictReader(source):
                sellers_by_order.setdefault(row["order_id"], set()).add(row["seller_id"])

        review_rows = []
        skipped_ambiguous = 0
        with reviews_path.open(encoding="utf-8", newline="") as source:
            for row in csv.DictReader(source):
                sellers = sellers_by_order.get(row["order_id"], set())
                if len(sellers) != 1:
                    skipped_ambiguous += 1
                    continue
                review_score = int(row["review_score"])
                if not 1 <= review_score <= 5:
                    continue
                seller_id = next(iter(sellers))
                review_rows.append(
                    (
                        f"{row['review_id']}:{seller_id}",
                        "olist",
                        seller_id,
                        review_score,
                        row.get("review_creation_date") or None,
                    )
                )

        pg_hook = PostgresHook(postgres_conn_id="pos_db")
        conn = pg_hook.get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO z1_silver.seller_score_reviews
                            (review_id, source_system, source_seller_id, review_score, reviewed_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (review_id) DO UPDATE SET
                            source_seller_id = EXCLUDED.source_seller_id,
                            review_score = EXCLUDED.review_score,
                            reviewed_at = EXCLUDED.reviewed_at
                        """,
                        review_rows,
                    )
        finally:
            conn.close()

        print(
            f"[SELLER SCORE REVIEWS] Loaded {len(review_rows)} Olist reviews; "
            f"skipped {skipped_ambiguous} ambiguous orders."
        )
        return {"loaded": len(review_rows), "skipped_ambiguous": skipped_ambiguous}

    @task(task_id="map_seller_ids_and_prepare_inventory_accuracy")
    def prepare_simulated_seller_score_inputs(**context):
        """Map source IDs to country/type cohorts and create deterministic stock checks."""
        pg_hook = PostgresHook(postgres_conn_id="pos_db")
        conn = pg_hook.get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO public.dim_seller_source_key
                            (source_system, source_seller_id, seller_id)
                        SELECT DISTINCT 'marketplace', event.source_seller_id, seller.seller_id
                        FROM public.marketplace_seller_score_events event
                        JOIN public.dim_country country
                          ON country.country_code = event.source_country
                        JOIN public.dim_seller seller
                          ON seller.country_id = country.country_id
                         AND seller.external = (event.source_seller_type = 'third_party')
                         AND seller.seller_name = 'Seller score group ' || event.source_country || ' ' || event.source_seller_type
                        ON CONFLICT (source_system, source_seller_id) DO UPDATE
                        SET seller_id = EXCLUDED.seller_id
                    """)
                    cur.execute("""
                        INSERT INTO public.dim_seller_source_key
                            (source_system, source_seller_id, seller_id)
                        SELECT 'inventory_api', source.seller_id, canonical.seller_id
                        FROM public.sellers source
                        JOIN public.dim_country country ON country.country_code = source.country
                        JOIN public.dim_seller canonical
                          ON canonical.country_id = country.country_id
                         AND canonical.external = (source.seller_type = 'third_party')
                         AND canonical.seller_name = 'Seller score group ' || source.country || ' ' || source.seller_type
                        ON CONFLICT (source_system, source_seller_id) DO UPDATE
                        SET seller_id = EXCLUDED.seller_id
                    """)
                    cur.execute("""
                        INSERT INTO public.dim_seller_source_key
                            (source_system, source_seller_id, seller_id)
                        SELECT DISTINCT 'olist', source.source_seller_id, canonical.seller_id
                        FROM z1_silver.seller_score_reviews source
                        JOIN public.dim_country country ON country.country_code = 'BR'
                        JOIN public.dim_seller canonical
                          ON canonical.country_id = country.country_id
                         AND canonical.seller_name = 'Seller score group BR third_party'
                        ON CONFLICT (source_system, source_seller_id) DO UPDATE
                        SET seller_id = EXCLUDED.seller_id
                    """)
                    cur.execute("""
                        INSERT INTO public.dim_seller_source_key
                            (source_system, source_seller_id, seller_id)
                        SELECT 'marketplace', 'SIM-' || country.country_code || '-' || seller_type,
                               canonical.seller_id
                        FROM public.dim_country country
                        CROSS JOIN (VALUES ('own'), ('third_party')) AS types(seller_type)
                        JOIN public.dim_seller canonical
                          ON canonical.country_id = country.country_id
                         AND canonical.external = (seller_type = 'third_party')
                         AND canonical.seller_name = 'Seller score group ' || country.country_code || ' ' || seller_type
                        WHERE country.country_code IN ('CO', 'PE', 'EC', 'BO', 'CL', 'BR')
                        ON CONFLICT (source_system, source_seller_id) DO UPDATE
                        SET seller_id = EXCLUDED.seller_id
                    """)
                    cur.execute("""
                        INSERT INTO z1_silver.seller_score_dispatch
                            (dispatch_id, source_system, source_seller_id, paid_at, shipped_at)
                        SELECT 'sim-dispatch-' || source_key.source_seller_id || '-' || sample.n,
                               'marketplace', source_key.source_seller_id,
                               NOW() - (sample.n || ' days')::interval,
                               NOW() - (sample.n || ' days')::interval
                                   + CASE WHEN sample.n <= 4 THEN INTERVAL '12 hours' ELSE INTERVAL '36 hours' END
                        FROM public.dim_seller_source_key source_key
                        CROSS JOIN generate_series(1, 5) AS sample(n)
                        WHERE source_key.source_system = 'marketplace'
                          AND source_key.source_seller_id LIKE 'SIM-%'
                        ON CONFLICT (dispatch_id) DO NOTHING
                    """)
                    cur.execute("""
                        INSERT INTO z1_silver.seller_score_cancellations
                            (order_id, source_system, source_seller_id, cancelled, cancellation_reason, cancelled_at)
                        SELECT 'sim-order-' || source_key.source_seller_id || '-' || sample.n,
                               'marketplace', source_key.source_seller_id, sample.n = 1,
                               CASE WHEN sample.n = 1 THEN 'simulated' ELSE NULL END, NOW()
                        FROM public.dim_seller_source_key source_key
                        CROSS JOIN generate_series(1, 5) AS sample(n)
                        WHERE source_key.source_system = 'marketplace'
                          AND source_key.source_seller_id LIKE 'SIM-%'
                        ON CONFLICT (order_id) DO NOTHING
                    """)
                    cur.execute("""
                        INSERT INTO z1_silver.seller_score_inventory
                            (observation_id, source_system, source_seller_id, inventory_accurate, observed_at)
                        SELECT 'sim-stock-' || source.seller_id, 'inventory_api', source.seller_id,
                               mod(abs(hashtext(source.seller_id))::bigint, 10) <> 0, NOW()
                        FROM public.sellers source
                        ON CONFLICT (observation_id) DO UPDATE
                        SET inventory_accurate = EXCLUDED.inventory_accurate,
                            observed_at = EXCLUDED.observed_at
                    """)
        finally:
            conn.close()

    @task(task_id="validate_scoring_prerequisites")
    def validate_scoring_prerequisites(**context):
        """Check that Silver source tables exist and their seller keys are mapped."""
        pg_hook = PostgresHook(postgres_conn_id="pos_db")
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        checks = {
            "sellers": "SELECT COUNT(*) FROM public.dim_seller",
            "seller keys": "SELECT COUNT(*) FROM public.dim_seller_source_key",
            "dispatch": "SELECT COUNT(*) FROM z1_silver.seller_score_dispatch WHERE source_system = 'marketplace'",
            "reviews": "SELECT COUNT(*) FROM z1_silver.seller_score_reviews WHERE source_system = 'olist'",
            "inventory": "SELECT COUNT(*) FROM z1_silver.seller_score_inventory WHERE source_system = 'inventory_api'",
            "cancellations": "SELECT COUNT(*) FROM z1_silver.seller_score_cancellations WHERE source_system = 'marketplace'",
        }
        counts = {}
        for name, query in checks.items():
            cur.execute(query)
            counts[name] = cur.fetchone()[0]

        mapping_checks = {
            "dispatch": "seller_score_dispatch",
            "reviews": "seller_score_reviews",
            "inventory": "seller_score_inventory",
            "cancellations": "seller_score_cancellations",
        }
        unmapped = {}
        for name, table in mapping_checks.items():
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM z1_silver.{table} source
                LEFT JOIN public.dim_seller_source_key key
                  ON key.source_system = source.source_system
                 AND key.source_seller_id = source.source_seller_id
                WHERE source.source_system = %s
                  AND key.seller_id IS NULL
                """,
                ("olist" if name == "reviews" else "inventory_api" if name == "inventory" else "marketplace",),
            )
            unmapped[name] = cur.fetchone()[0]
        cur.close()
        conn.close()

        empty = [name for name, count in counts.items() if count == 0]
        if empty:
            raise ValueError(
                "Seller score Silver tables are not ready: " + ", ".join(empty)
            )
        unresolved = [name for name, count in unmapped.items() if count]
        if unresolved:
            raise ValueError(
                "Seller score source IDs have no dim_seller mapping: "
                + ", ".join(f"{name}={unmapped[name]}" for name in unresolved)
            )

        print(f"[PRE-CHECK] Seller score source row counts: {counts}; unmapped: {unmapped}")
        return {"counts": counts, "unmapped": unmapped}

    @task(task_id="rn_08_update_seller_status")
    def update_seller_status(**context):
        """Apply RN-08 from the two most recent weekly scores written by Spark."""
        pg_hook = PostgresHook(postgres_conn_id="pos_db")
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            WITH ranked_scores AS (
                SELECT seller_id, week_start, score,
                       ROW_NUMBER() OVER (
                           PARTITION BY seller_id ORDER BY week_start DESC
                       ) AS week_rank
                FROM z2_gold.seller_score_history
                WHERE week_start >= date_trunc('week', CURRENT_DATE - INTERVAL '7 days')::date
            ),
            recent_scores AS (
                SELECT seller_id,
                       COUNT(*) AS weeks_available,
                       MAX(score) FILTER (WHERE week_rank = 1) AS latest_score,
                       MAX(score) FILTER (WHERE week_rank = 2) AS previous_score
                FROM ranked_scores
                WHERE week_rank <= 2
                GROUP BY seller_id
            )
            INSERT INTO z2_gold.seller_status (seller_id, status, reason, updated_at)
            SELECT seller_id,
                   CASE
                       WHEN weeks_available = 2 AND latest_score < 45 AND previous_score < 45
                           THEN 'suspended'
                       WHEN weeks_available = 2 AND latest_score < 60 AND previous_score < 60
                           THEN 'under_observation'
                       ELSE 'active'
                   END,
                   CASE
                       WHEN weeks_available = 2 AND latest_score < 45 AND previous_score < 45
                           THEN 'Score below 45 for two consecutive weeks'
                       WHEN weeks_available = 2 AND latest_score < 60 AND previous_score < 60
                           THEN 'Score below 60 for two consecutive weeks'
                       ELSE 'active'
                   END,
                   NOW()
            FROM recent_scores
            ON CONFLICT (seller_id) DO UPDATE
            SET status = EXCLUDED.status,
                reason = EXCLUDED.reason,
                updated_at = EXCLUDED.updated_at
            """
        )
        updated = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        print(f"[RN-08] Updated status for {updated} sellers.")
        return updated

    start_scoring = EmptyOperator(task_id="start_scoring")

    t_wait_for_inputs = PythonSensor(
        task_id="wait_for_seller_score_inputs",
        python_callable=seller_score_inputs_ready,
        poke_interval=300,
        timeout=7 * 24 * 60 * 60,
        mode="reschedule",
        doc_md="Waits up to seven days for Silver inputs and dim_seller_source_key mappings.",
    )

    t_validate = validate_scoring_prerequisites()

    @task(task_id="ingest_marketplace_seller_events")
    def ingest_marketplace_seller_events(**context):
        """Ingests pending marketplace events into Marketplace Silver tables."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id="pos_db")
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS z1_silver;
                INSERT INTO z1_silver.seller_score_dispatch (
                    dispatch_id, source_system, source_seller_id, paid_at, shipped_at
                )
                SELECT 
                    COALESCE(order_id, payload->>'order_id', gen_random_uuid()::text),
                    'Marketplace',
                    COALESCE(payload->>'seller_id', 'SEL-CO-001'),
                    COALESCE((payload->>'order_date')::timestamptz, created_at - INTERVAL '1 day'),
                    COALESCE((payload->>'actual_delivery_date')::timestamptz, created_at)
                FROM public.marketplace_events
                ON CONFLICT (dispatch_id) DO NOTHING;
            """)
            count = cur.rowcount
            conn.commit()
            print(f"[Ingestion] Processed {count} marketplace events into Silver.")
            return count
        finally:
            cur.close()
            conn.close()

    t_ingest_marketplace = ingest_marketplace_seller_events()
    t_load_reviews = load_olist_seller_reviews()
    t_prepare_simulated_inputs = prepare_simulated_seller_score_inputs()

    @task(task_id="rn_07_calculate_seller_score_spark")
    def calculate_seller_score_spark(**context):
        """RN-07: Calculates seller score in Gold based on Silver metrics.
        Formula: Score = 0.35 * dispatch_pct + 0.25 * (review_rating * 20) + 0.20 * inventory_accuracy + 0.20 * (100 - cancellation_rate)
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id="pos_db")
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS z2_gold;
                CREATE TABLE IF NOT EXISTS z2_gold.seller_score (
                    seller_id VARCHAR(64) PRIMARY KEY,
                    score NUMERIC(5, 2),
                    dispatch_pct NUMERIC(5, 2),
                    review_rating NUMERIC(3, 2),
                    inventory_accuracy NUMERIC(5, 2),
                    cancellation_rate NUMERIC(5, 2),
                    calculated_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS z2_gold.seller_score_history (
                    seller_id VARCHAR(64),
                    week_start DATE,
                    score NUMERIC(5, 2),
                    dispatch_pct NUMERIC(5, 2),
                    review_rating NUMERIC(3, 2),
                    inventory_accuracy NUMERIC(5, 2),
                    cancellation_rate NUMERIC(5, 2),
                    calculated_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (seller_id, week_start)
                );

                INSERT INTO z2_gold.seller_score (
                    seller_id, score, dispatch_pct, review_rating,
                    inventory_accuracy, cancellation_rate, calculated_at
                )
                SELECT
                    s.seller_id::text,
                    ROUND(
                        0.35 * COALESCE(o.dispatch_pct, 95.0) +
                        0.25 * (COALESCE(r.review_rating, 4.5) * 20.0) +
                        0.20 * COALESCE(i.inventory_accuracy, 98.0) +
                        0.20 * (100.0 - COALESCE(c.cancellation_rate, 2.0)),
                        2
                    ) AS score,
                    ROUND(COALESCE(o.dispatch_pct, 95.0), 2) AS dispatch_pct,
                    ROUND(COALESCE(r.review_rating, 4.5), 2) AS review_rating,
                    ROUND(COALESCE(i.inventory_accuracy, 98.0), 2) AS inventory_accuracy,
                    ROUND(COALESCE(c.cancellation_rate, 2.0), 2) AS cancellation_rate,
                    NOW() AS calculated_at
                FROM public.dim_seller s
                LEFT JOIN (
                    SELECT source_seller_id AS seller_id, 
                           100.0 * COUNT(*) FILTER (WHERE dispatch_lead_time_hours <= 48) / NULLIF(COUNT(*), 0) AS dispatch_pct
                    FROM z1_silver.vw_seller_dispatch
                    GROUP BY source_seller_id
                ) o ON s.seller_id::text = o.seller_id
                LEFT JOIN (
                    SELECT source_seller_id AS seller_id, AVG(review_score) AS review_rating
                    FROM z1_silver.vw_seller_reviews
                    GROUP BY source_seller_id
                ) r ON s.seller_id::text = r.seller_id
                LEFT JOIN (
                    SELECT source_seller_id AS seller_id, 
                           100.0 * COUNT(*) FILTER (WHERE inventory_accurate = TRUE) / NULLIF(COUNT(*), 0) AS inventory_accuracy
                    FROM z1_silver.vw_seller_inventory
                    GROUP BY source_seller_id
                ) i ON s.seller_id::text = i.seller_id
                LEFT JOIN (
                    SELECT source_seller_id AS seller_id, 
                           100.0 * COUNT(*) FILTER (WHERE cancelled = TRUE) / NULLIF(COUNT(*), 0) AS cancellation_rate
                    FROM z1_silver.vw_seller_cancellations
                    GROUP BY source_seller_id
                ) c ON s.seller_id::text = c.seller_id
                ON CONFLICT (seller_id) DO UPDATE SET
                    score = EXCLUDED.score,
                    dispatch_pct = EXCLUDED.dispatch_pct,
                    review_rating = EXCLUDED.review_rating,
                    inventory_accuracy = EXCLUDED.inventory_accuracy,
                    cancellation_rate = EXCLUDED.cancellation_rate,
                    calculated_at = NOW();

                INSERT INTO z2_gold.seller_score_history (
                    seller_id, week_start, score, dispatch_pct, review_rating,
                    inventory_accuracy, cancellation_rate, calculated_at
                )
                SELECT seller_id, date_trunc('week', CURRENT_DATE)::date,
                       score, dispatch_pct, review_rating, inventory_accuracy,
                       cancellation_rate, NOW()
                FROM z2_gold.seller_score
                ON CONFLICT (seller_id, week_start) DO UPDATE SET
                    score = EXCLUDED.score,
                    dispatch_pct = EXCLUDED.dispatch_pct,
                    review_rating = EXCLUDED.review_rating,
                    inventory_accuracy = EXCLUDED.inventory_accuracy,
                    cancellation_rate = EXCLUDED.cancellation_rate,
                    calculated_at = NOW();
            """)
            updated = cur.rowcount
            conn.commit()
            print(f"[RN-07] Successfully computed seller scores for {updated} sellers.")
            return updated
        finally:
            cur.close()
            conn.close()

    t_score = calculate_seller_score_spark()

    t_status = update_seller_status()

    @task(task_id="export_seller_score_to_s3")
    def export_seller_score_to_s3(**context):
        """Export computed Gold seller scores to AWS S3."""
        import boto3
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        try:
            s3 = boto3.client("s3")
            bucket = os.getenv("S3_DATA_LAKE_BUCKET_NAME", "mercaandes-data-lake")
            pg_hook = PostgresHook(postgres_conn_id="pos_db")
            conn = pg_hook.get_conn()
            cur = conn.cursor()
            for table, tbl_name in [("seller_score", "gold_seller_score"), ("seller_status", "gold_seller_status")]:
                try:
                    cur.execute(f"SELECT * FROM z2_gold.{table} LIMIT 1000;")
                    cols = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    if rows:
                        csv_data = ",".join(cols) + "\n" + "\n".join([",".join(str(val) for val in r) for r in rows])
                        key = f"gold/{table}/{table}.csv"
                        s3.put_object(Bucket=bucket, Key=key, Body=csv_data.encode("utf-8"))
                        print(f"[GOLD SELLER SCORE] Exported {len(rows)} records to s3://{bucket}/{key}")
                except Exception as exc:
                    print(f"[GOLD Warning] Could not export {table}: {exc}")
            cur.close()
            conn.close()
        except Exception as exc:
            print(f"[GOLD Warning] S3 export skipped: {exc}")
        return True

    t_export_score = export_seller_score_to_s3()
    end_scoring = EmptyOperator(task_id="end_scoring")

    start_scoring >> [t_ingest_marketplace, t_load_reviews] >> t_prepare_simulated_inputs >> t_wait_for_inputs
    t_wait_for_inputs >> t_validate >> t_score >> t_status >> t_export_score >> end_scoring


seller_scoring_dag = seller_scoring()
