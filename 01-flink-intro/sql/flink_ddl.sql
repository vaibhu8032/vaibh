
-----------------retriving data from topics using flink sql----------------------------------------------
CREATE TABLE t1(
  `timestamp` string,
  `value` INT
) WITH (
  'connector' = 'kafka',
  'topic' = 't1',
  'properties.bootstrap.servers' = 'redpanda-1:29092',
  'properties.group.id' = 'test-group',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json'
);


CREATE TABLE t2 (
  `timestamp` string,
  `value` INT
) WITH (
  'connector' = 'kafka',
  'topic' = 't2',
  'properties.bootstrap.servers' = 'redpanda-1:29092',
  'properties.group.id' = 'test-group',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json'
);

CREATE TABLE t3 (
  `timestamp` string,
  `value` INT
) WITH (
  'connector' = 'kafka',
  'topic' = 't3',
  'properties.bootstrap.servers' = 'redpanda-1:29092',
  'properties.group.id' = 'test-group',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json'
);

CREATE TABLE recon (
   `topic` string,
  `start_time` string,
  `end_time` string,
  `counts` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'recon',
  'properties.bootstrap.servers' = 'redpanda-1:29092',
  'properties.group.id' = 'test-group',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json'
);




----------------producering data to kafka topic using flink sql--------------------------------------------------------
create table recon_result(
	`topic` STRING,
	`start_time` string,
	`end_time` string,
	`expected_counts` BIGINT,
	`recieved_counts` BIGINT,
	primary key(topic) not enforced
	
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'recon_result',
  'properties.bootstrap.servers' = 'redpanda-1:29092',
  'properties.group.id' = 'test-group',
  'properties.auto.offset.reset' = 'earliest',

  'key.format' = 'json',
  'value.format' = 'json'
);

insert into recon_result
WITH results AS (
    SELECT 
        't1' AS topic,
        MIN(`timestamp`) AS start_time,
        MAX(`timestamp`) AS end_time,
        COUNT(*) AS expected_counts,
        (SELECT COUNT(*) FROM t1) AS recieved_counts
    FROM t1
    UNION ALL
    SELECT 
        't2' AS topic,
        MIN(`timestamp`) AS start_time,
        MAX(`timestamp`) AS end_time,
        COUNT(*) AS expected_counts,
        (SELECT COUNT(*) FROM t2) AS recieved_counts
    FROM t2
    UNION ALL
    SELECT 
        't3' AS topic,
        MIN(`timestamp`) AS start_time,
        MAX(`timestamp`) AS end_time,
        COUNT(*) AS expected_counts,
        (SELECT COUNT(*) FROM t3) AS recieved_counts
    FROM t3
)
select 
 topic, start_time, end_time, expected_counts, recieved_counts 
FROM results;


-- CREATE TABLE jdbc_sink_table (
--   `topic` STRING,
--   `start_time` STRING,
--   `end_time` STRING,
--   `expected_counts` BIGINT,
--   `received_counts` BIGINT,
--   PRIMARY KEY (`topic`) NOT ENFORCED
-- ) WITH (
--   'connector' = 'jdbc',
--   'url' = 'jdbc:sqlserver://[SERVER=VAIBHAVP\SQLEXPRESS]:[1433];databaseName=[demo];user=[vaibhav];password=[enjjoyBBDVP$10253]',
--   'table-name' = 'jdbc_sink_table',
--   'username' = 'vaibhav',
--   'password' = 'enjjoyBBDVP$10253',
--   'driver' = 'com.mysql.jdbc.Driver'
-- );

-- CREATE TABLE jdbc_sink_table (
--   `topic` STRING,
--   `start_time` STRING,
--   `end_time` STRING,
--   `expected_counts` BIGINT,
--   `received_counts` BIGINT,
--   PRIMARY KEY (`topic`) NOT ENFORCED
-- ) WITH (
--   'connector' = 'jdbc',
--   'url' = 'jdbc:sqlserver://VAIBHAVP\\SQLEXPRESS:1433;databaseName=demo',
--   'table-name' = 'jdbc_sink_table',
--   'username' = 'vaibhav',
--   'password' = 'enjoyBBDVP$10253',
--   'driver' = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
-- );

-- -- INSERT INTO jdbc_sink_table
-- SELECT * FROM recon_result;

-- jdbc:sqlserver://[SERVER=VAIBHAVP\SQLEXPRESS]:[1433];databaseName=[demo];user=[vaibhav];password=[enjjoyBBDVP$10253]

-- Create tables for topics t1 to t100
-- {% for i in range(1, 101) %}
-- CREATE TABLE t{{ i }} (
--   timestamp TIMESTAMP(3),
--   value INT
-- ) WITH (
--   'connector' = 'kafka',
--   'topic' = 't{{ i }}',
--   'properties.bootstrap.servers' = 'localhost:9092',
--   'format' = 'json'
-- );
-- {% endfor %};
--  'connector.type' = 'kafka',
--             ...     'update-mode' = 'append',