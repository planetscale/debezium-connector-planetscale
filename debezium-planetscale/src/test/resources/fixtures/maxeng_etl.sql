-- maxeng_etl dataset dump from PlanetScale (2026-03-27)
-- Stripped of MySQL conditional comments for Vitess compatibility.

SET NAMES utf8mb4;

CREATE TABLE `d1` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `d1` VALUES (100),(101);

CREATE TABLE `t1` (
  `id` int NOT NULL,
  `shape` geometry NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `t1` VALUES (2, ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'));
INSERT INTO `t1` VALUES (13, ST_GeomFromText('POLYGON((10 0,30 0,40 20,20 35,0 20,10 0))'));
INSERT INTO `t1` VALUES (42, ST_GeomFromText('POLYGON((-73.9857 40.7484,-73.9813 40.7516,-73.9788 40.7489,-73.9832 40.7457,-73.9857 40.7484))'));

CREATE TABLE `vstream_test` (
  `id` int NOT NULL AUTO_INCREMENT,
  `tiny_col` tinyint DEFAULT NULL,
  `small_col` smallint DEFAULT NULL,
  `medium_col` mediumint DEFAULT NULL,
  `int_col` int DEFAULT NULL,
  `big_col` bigint DEFAULT NULL,
  `bit_col` bit(8) DEFAULT NULL,
  `varchar_col` varchar(255) DEFAULT NULL,
  `text_col` text,
  `float_col` float DEFAULT NULL,
  `double_col` double DEFAULT NULL,
  `decimal_col` decimal(10,2) DEFAULT NULL,
  `date_col` date DEFAULT NULL,
  `datetime_col` datetime DEFAULT NULL,
  `timestamp_col` timestamp NULL DEFAULT NULL,
  `time_col` time DEFAULT NULL,
  `json_col` json DEFAULT NULL,
  `blob_col` blob,
  `bool_col` tinyint(1) DEFAULT NULL,
  `enum_col` enum('small','medium','large') DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `vstream_test` (id, tiny_col, small_col, medium_col, int_col, big_col, bit_col, varchar_col, text_col, float_col, double_col, decimal_col, date_col, datetime_col, timestamp_col, time_col, json_col, blob_col, bool_col, enum_col) VALUES (1, 127, 32000, 8000000, 2000000000, 9000000000000000000, b'11111111', 'test varchar', 'test text content', 123.456, 789.123456789, 12345.67, '2026-01-15', '2026-01-15 10:30:00', '2026-01-15 10:30:00', '10:30:00', '{"key": "value", "nested": {"num": 42}}', 'Hello', 1, 'medium');
INSERT INTO `vstream_test` (id, tiny_col, small_col, medium_col, int_col, big_col, bit_col, varchar_col, text_col, float_col, double_col, decimal_col, date_col, datetime_col, timestamp_col, time_col, json_col, blob_col, bool_col, enum_col) VALUES (2, 50, 16000, 4000000, 1000000000, 5000000000000000000, b'10101010', 'second row', 'more text', 456.789, 123.987654321, 54321.12, '2026-01-20', '2026-01-20 15:45:00', '2026-01-20 15:45:00', '15:45:00', '{"test": "data"}', 'World', 0, 'large');
