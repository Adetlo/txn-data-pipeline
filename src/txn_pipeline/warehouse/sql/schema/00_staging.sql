-- Staging view over every curated partition. Partition directories are a storage layout;
-- the partition columns are also stored inside each file, so hive_partitioning is off.
CREATE OR REPLACE VIEW stg_curated AS
SELECT * FROM read_parquet('{curated_glob}', hive_partitioning = false, union_by_name = true);
