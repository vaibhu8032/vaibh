from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# Sets up the execution environment, which is the main entry point
# to building Flink applications.
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)  # set parallelism to 1 for simplicity, adjust as needed
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = StreamTableEnvironment.create(env, environment_settings=settings)

# Define Kafka connector properties
kafka_bootstrap_servers = "redpanda-1:29092"
kafka_group_id = "test-group"
kafka_auto_offset_reset = "earliest"

# Register Kafka tables
t1 = f"""
CREATE TABLE t1 (
    `timestamp` STRING,
    `value` INT
) WITH (
    'connector' = 'kafka',
    'topic' = 't1',
    'properties.bootstrap.servers' = '{kafka_bootstrap_servers}',
    'properties.group.id' = '{kafka_group_id}',
    'properties.auto.offset.reset' = '{kafka_auto_offset_reset}',
    'format' = 'json'
)
"""
t2 = f"""
CREATE TABLE t2 (
    `timestamp` STRING,
    `value` INT
) WITH (
    'connector' = 'kafka',
    'topic' = 't2',
    'properties.bootstrap.servers' = '{kafka_bootstrap_servers}',
    'properties.group.id' = '{kafka_group_id}',
    'properties.auto.offset.reset' = '{kafka_auto_offset_reset}',
    'format' = 'json'
)
"""
t3 = f"""
CREATE TABLE t3 (
    `timestamp` STRING,
    `value` INT
) WITH (
    'connector' = 'kafka',
    'topic' = 't3',
    'properties.bootstrap.servers' = '{kafka_bootstrap_servers}',
    'properties.group.id' = '{kafka_group_id}',
    'properties.auto.offset.reset' = '{kafka_auto_offset_reset}',
    'format' = 'json'
)
"""

table_env.execute_sql(t1)
table_env.execute_sql(t2)
table_env.execute_sql(t3)

# Register output Kafka table
recon_result_ddl = f"""
CREATE TABLE recon_result (
    `topic` STRING,
    `start_time` STRING,
    `end_time` STRING,
    `expected_counts` BIGINT,
    `received_counts` BIGINT,
    PRIMARY KEY (`topic`) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'recon_result',
    'properties.bootstrap.servers' = '{kafka_bootstrap_servers}',
    'properties.group.id' = '{kafka_group_id}',
    'properties.auto.offset.reset' = '{kafka_auto_offset_reset}',
    'key.format' = 'json',
    'value.format' = 'json'
)
"""

table_env.execute_sql(recon_result_ddl)

# Define SQL query
query = """
INSERT INTO recon_result
WITH results AS (
    SELECT 't1' AS topic,
           MIN(`timestamp`) AS start_time,
           MAX(`timestamp`) AS end_time,
           COUNT(*) AS expected_counts,
           (SELECT COUNT(*) FROM t1) AS received_counts
    FROM t1
    UNION ALL
    SELECT 't2' AS topic,
           MIN(`timestamp`) AS start_time,
           MAX(`timestamp`) AS end_time,
           COUNT(*) AS expected_counts,
           (SELECT COUNT(*) FROM t2) AS received_counts
    FROM t2
    UNION ALL
    SELECT 't3' AS topic,
           MIN(`timestamp`) AS start_time,
           MAX(`timestamp`) AS end_time,
           COUNT(*) AS expected_counts,
           (SELECT COUNT(*) FROM t3) AS received_counts
    FROM t3
)
SELECT topic, start_time, end_time, expected_counts, received_counts
FROM results
"""

# Execute the query
table_env.execute_sql(query)

# Execute the Flink job
env.execute("Flink Python API Skeleton")
