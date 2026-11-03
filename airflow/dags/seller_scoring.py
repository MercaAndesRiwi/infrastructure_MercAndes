"""Weekly seller score and status workflow."""

from __future__ import annotations

import os
import csv
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.sensors.python import PythonSensor
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
        "SELECT COUNT(*) FROM public.silver_seller_score_dispatch WHERE source_system = 'marketplace'",
        "SELECT COUNT(*) FROM public.silver_seller_score_reviews WHERE source_system = 'olist'",
        "SELECT COUNT(*) FROM public.silver_seller_score_inventory WHERE source_system = 'inventory_api'",
        "SELECT COUNT(*) FROM public.silver_seller_score_cancellations WHERE source_system = 'marketplace'",
    )
    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cur:
            for query in checks:
                cur.execute(query)
                if cur.fetchone()[0] == 0:
                    return False

            source_tables = (
                ("silver_seller_score_dispatch", "marketplace"),
                ("silver_seller_score_reviews", "olist"),
                ("silver_seller_score_inventory", "inventory_api"),
                ("silver_seller_score_cancellations", "marketplace"),
            )
            for table, source_system in source_tables:
                cur.execute(
                    f"""
                    SELECT 1
                    FROM public.{table} source
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
    start_date=datetime(2026, 11, 5, 10, 0),
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
                        INSERT INTO public.silver_seller_score_reviews
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
                        FROM public.silver_seller_score_reviews source
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
                        INSERT INTO public.silver_seller_score_dispatch
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
                        INSERT INTO public.silver_seller_score_cancellations
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
                        INSERT INTO public.silver_seller_score_inventory
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
            "dispatch": "SELECT COUNT(*) FROM public.silver_seller_score_dispatch WHERE source_system = 'marketplace'",
            "reviews": "SELECT COUNT(*) FROM public.silver_seller_score_reviews WHERE source_system = 'olist'",
            "inventory": "SELECT COUNT(*) FROM public.silver_seller_score_inventory WHERE source_system = 'inventory_api'",
            "cancellations": "SELECT COUNT(*) FROM public.silver_seller_score_cancellations WHERE source_system = 'marketplace'",
        }
        counts = {}
        for name, query in checks.items():
            cur.execute(query)
            counts[name] = cur.fetchone()[0]

        mapping_checks = {
            "dispatch": "silver_seller_score_dispatch",
            "reviews": "silver_seller_score_reviews",
            "inventory": "silver_seller_score_inventory",
            "cancellations": "silver_seller_score_cancellations",
        }
        unmapped = {}
        for name, table in mapping_checks.items():
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM public.{table} source
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
                FROM public.gold_seller_score_history
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
            INSERT INTO public.gold_seller_status (seller_id, status, reason, updated_at)
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

    t_ingest_marketplace = DockerOperator(
        task_id="ingest_marketplace_seller_events",
        image=os.getenv(
            "SELLER_SCORE_INGESTION_IMAGE", "mercaandes-seller-score-ingestion:local"
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode=os.getenv("SELLER_SCORE_DOCKER_NETWORK", "mercaandes-seller-score"),
        environment={
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
            "KAFKA_TOPIC_ORDERS": os.getenv(
                "KAFKA_TOPIC_ORDERS", "mercaandes.orders.events"
            ),
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": os.getenv("POS_DB_NAME", "merca_pos"),
            "POSTGRES_USER": os.getenv("POS_DB_USER", "pos_user"),
            "POSTGRES_PASSWORD": os.getenv("POS_DB_PASSWORD", ""),
        },
        auto_remove="success",
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=15),
        doc_md="Ingests pending Kafka events into Marketplace Silver tables.",
    )

    t_load_reviews = load_olist_seller_reviews()
    t_prepare_simulated_inputs = prepare_simulated_seller_score_inputs()

    t_score = DockerOperator(
        task_id="rn_07_calculate_seller_score_spark",
        image=os.getenv("SELLER_SCORE_DOCKER_IMAGE", "mercaandes-seller-score-job:local"),
        docker_url="unix://var/run/docker.sock",
        network_mode=os.getenv("SELLER_SCORE_DOCKER_NETWORK", "mercaandes-seller-score"),
        environment={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": os.getenv("POS_DB_NAME", "merca_pos"),
            "POSTGRES_USER": os.getenv("POS_DB_USER", "pos_user"),
            "POSTGRES_PASSWORD": os.getenv("POS_DB_PASSWORD", ""),
        },
        auto_remove="success",
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=60),
        doc_md="Executes RN-07 calculation in Spark and persists result to Gold.",
    )

    t_status = update_seller_status()
    end_scoring = EmptyOperator(task_id="end_scoring")

    start_scoring >> [t_ingest_marketplace, t_load_reviews] >> t_prepare_simulated_inputs >> t_wait_for_inputs
    t_wait_for_inputs >> t_validate >> t_score >> t_status >> end_scoring


seller_scoring_dag = seller_scoring()
