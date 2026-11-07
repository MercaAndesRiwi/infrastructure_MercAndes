-- ==============================================================================
-- MercaAndes Data Platform - Purge Airflow Metadata from merca_gold Database
-- Database: PostgreSQL 16 (Amazon RDS merca_gold)
--
-- MOTIVATION & OBJECTIVE:
-- When Airflow was initialized or migrated pointing directly to 'merca_gold',
-- its internal orchestration metadata tables were written into 'public'.
-- This script safely and idempotently purges all Airflow metadata tables from
-- 'merca_gold.public' without touching ANY MercaAndes business tables,
-- dimensions, facts, country schemas (colombia, peru, ecuador, bolivia, chile),
-- or analytical schemas (silver, gold).
--
-- DEDICATED AIRFLOW DATABASE:
-- Airflow orchestration must connect strictly to its own dedicated database:
--   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:<pwd>@<host>:5432/airflow
-- ==============================================================================

-- 1. DROP AIRFLOW ACCESS CONTROL & USER MANAGEMENT TABLES
DROP TABLE IF EXISTS public.ab_permission_view_role CASCADE;
DROP TABLE IF EXISTS public.ab_permission_view CASCADE;
DROP TABLE IF EXISTS public.ab_permission CASCADE;
DROP TABLE IF EXISTS public.ab_user_role CASCADE;
DROP TABLE IF EXISTS public.ab_user CASCADE;
DROP TABLE IF EXISTS public.ab_role CASCADE;
DROP TABLE IF EXISTS public.ab_register CASCADE;

-- 2. DROP AIRFLOW TASK INSTANCES, LOGS & EXECUTION HISTORY
DROP TABLE IF EXISTS public.task_instance_history CASCADE;
DROP TABLE IF EXISTS public.task_instance_note CASCADE;
DROP TABLE IF EXISTS public.task_instance CASCADE;
DROP TABLE IF EXISTS public.task_reschedule CASCADE;
DROP TABLE IF EXISTS public.task_fail CASCADE;
DROP TABLE IF EXISTS public.task_map CASCADE;
DROP TABLE IF EXISTS public.rendered_task_instance_fields CASCADE;
DROP TABLE IF EXISTS public.xcom CASCADE;
DROP TABLE IF EXISTS public.log CASCADE;
DROP TABLE IF EXISTS public.log_template CASCADE;
DROP TABLE IF EXISTS public.job CASCADE;
DROP TABLE IF EXISTS public.trigger CASCADE;
DROP TABLE IF EXISTS public.callback_request CASCADE;
DROP TABLE IF EXISTS public.deadline CASCADE;

-- 3. DROP AIRFLOW DAG METADATA & PARSING TABLES
DROP TABLE IF EXISTS public.dag_run_note CASCADE;
DROP TABLE IF EXISTS public.backfill_dag_run CASCADE;
DROP TABLE IF EXISTS public.backfill CASCADE;
DROP TABLE IF EXISTS public.dag_run CASCADE;
DROP TABLE IF EXISTS public.dag_tag CASCADE;
DROP TABLE IF EXISTS public.dag_code CASCADE;
DROP TABLE IF EXISTS public.dag_version CASCADE;
DROP TABLE IF EXISTS public.dag_warning CASCADE;
DROP TABLE IF EXISTS public.dag_owner_attributes CASCADE;
DROP TABLE IF EXISTS public.dag_bundle CASCADE;
DROP TABLE IF EXISTS public.dag_priority_parsing_request CASCADE;
DROP TABLE IF EXISTS public.dag_schedule_asset_name_reference CASCADE;
DROP TABLE IF EXISTS public.dag_schedule_asset_reference CASCADE;
DROP TABLE IF EXISTS public.dag_schedule_dataset_alias_reference CASCADE;
DROP TABLE IF EXISTS public.dag_schedule_dataset_reference CASCADE;
DROP TABLE IF EXISTS public.import_error CASCADE;
DROP TABLE IF EXISTS public.dag CASCADE;

-- 4. DROP AIRFLOW ASSETS & DATASETS (AIRFLOW 2.x - 3.x DATA-AWARE SCHEDULING)
DROP TABLE IF EXISTS public.asset_partition_dag_run CASCADE;
DROP TABLE IF EXISTS public.asset_alias_asset CASCADE;
DROP TABLE IF EXISTS public.asset_alias CASCADE;
DROP TABLE IF EXISTS public.asset_event CASCADE;
DROP TABLE IF EXISTS public.asset_active CASCADE;
DROP TABLE IF EXISTS public.asset CASCADE;
DROP TABLE IF EXISTS public.dataset_alias_dataset CASCADE;
DROP TABLE IF EXISTS public.dataset_alias CASCADE;
DROP TABLE IF EXISTS public.dataset_event_tag CASCADE;
DROP TABLE IF EXISTS public.dataset_event CASCADE;
DROP TABLE IF EXISTS public.dataset CASCADE;

-- 5. DROP AIRFLOW CONFIGURATION, POOLS, SESSIONS & ALEMBIC REVISION
DROP TABLE IF EXISTS public.connection CASCADE;
DROP TABLE IF EXISTS public.variable CASCADE;
DROP TABLE IF EXISTS public.slot_pool CASCADE;
DROP TABLE IF EXISTS public.sla_miss CASCADE;
DROP TABLE IF EXISTS public.session CASCADE;
DROP TABLE IF EXISTS public.alembic_version CASCADE;

-- ==============================================================================
-- 6. VERIFICATION: AUDIT CLEANLINESS OF merca_gold.public SCHEMA
-- ==============================================================================
DO $$
DECLARE
    airflow_table_count INTEGER;
    business_table_count INTEGER;
BEGIN
    -- Check if any residual airflow tables remain
    SELECT COUNT(*)
    INTO airflow_table_count
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name IN (
          'ab_permission', 'ab_permission_view', 'ab_permission_view_role', 'ab_register',
          'ab_role', 'ab_user', 'ab_user_role', 'alembic_version', 'asset', 'asset_active',
          'asset_alias', 'asset_alias_asset', 'asset_event', 'asset_partition_dag_run',
          'backfill', 'backfill_dag_run', 'callback_request', 'connection', 'dag',
          'dag_bundle', 'dag_code', 'dag_owner_attributes', 'dag_priority_parsing_request',
          'dag_run', 'dag_run_note', 'dag_tag', 'dag_version', 'dag_warning', 'dataset',
          'dataset_alias', 'dataset_alias_dataset', 'dataset_event', 'deadline',
          'import_error', 'job', 'log', 'log_template', 'rendered_task_instance_fields',
          'session', 'sla_miss', 'slot_pool', 'task_fail', 'task_instance',
          'task_instance_history', 'task_instance_note', 'task_map', 'task_reschedule',
          'trigger', 'variable', 'xcom'
      );

    -- Count active business tables in public
    SELECT COUNT(*)
    INTO business_table_count
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND (
          table_name LIKE 'dim_%'
          OR table_name LIKE 'fact_%'
          OR table_name LIKE 'gold_%'
          OR table_name LIKE 'silver_%'
          OR table_name IN ('customers', 'data_quality_metrics', 'marketplace_events', 'marketplace_seller_score_events')
      );

    IF airflow_table_count > 0 THEN
        RAISE WARNING 'Purge completed with % residual Airflow tables in public schema.', airflow_table_count;
    ELSE
        RAISE NOTICE 'SUCCESS: Zero Airflow metadata tables found in public schema. Total preserved business tables: %', business_table_count;
    END IF;
END $$;
