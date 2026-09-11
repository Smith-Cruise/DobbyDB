CREATE DATABASE IF NOT EXISTS iceberg_rest.`iceberg_rest_db`;

DROP TABLE IF EXISTS iceberg_rest.`iceberg_rest_db`.`orders`;

CREATE TABLE iceberg_rest.`iceberg_rest_db`.`orders` (
    id INT,
    name STRING,
    amount DECIMAL(10, 2),
    dt STRING
)
USING iceberg
PARTITIONED BY (dt)
LOCATION 's3://lakelet-e2e/warehouse/iceberg_rest_db.db/orders';

INSERT INTO iceberg_rest.`iceberg_rest_db`.`orders`
VALUES
    (1, 'alice', CAST(10.50 AS DECIMAL(10, 2)), '2026-06-25'),
    (2, 'bob', CAST(20.25 AS DECIMAL(10, 2)), '2026-06-25'),
    (3, 'carol', CAST(7.00 AS DECIMAL(10, 2)), '2026-06-24'),
    (4, 'dave', CAST(12.30 AS DECIMAL(10, 2)), '2026-06-24');
