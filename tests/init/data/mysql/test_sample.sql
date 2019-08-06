-- MySQL dump 10.13  Distrib 8.0.16, for Linux (x86_64)


--
-- Table structure for table `all_types8`
--

DROP TABLE IF EXISTS `all_types`;

CREATE TABLE `all_types` (
    `boolean` BOOLEAN NOT NULL,
    `boolean_ptr` BOOLEAN,

    `tiny_int` TINYINT NOT NULL,
    `u_tiny_int` TINYINT unsigned NOT NULL,

    `small_int` SMALLINT NOT NULL,
    `u_small_int` SMALLINT unsigned NOT NULL,

    `medium_int` MEDIUMINT NOT NULL,
    `u_medium_int` MEDIUMINT unsigned NOT NULL,

    `integer` INT NOT NULL,
    `u_integer` INT unsigned NOT NULL,

    `big_int` bigint(20) NOT NULL,
    `u_big_int` bigint(20) unsigned NOT NULL,

-- ptr

    `tiny_int_ptr` TINYINT,
    `u_tiny_int_ptr` TINYINT unsigned,

    `small_int_ptr` SMALLINT,
    `u_small_int_ptr` SMALLINT unsigned,

    `medium_int_ptr` MEDIUMINT,
    `u_medium_int_ptr` MEDIUMINT unsigned,

    `integer_ptr` INT,
    `u_integer_ptr` INT unsigned,

    `big_int_ptr` bigint(20),
    `u_big_int_ptr` bigint(20) unsigned,


-- floats
    `decimal` decimal(5, 2) unsigned NOT NULL,
    `decimal_ptr` decimal(5,2),

    `numeric` numeric(5,2) NOT NULL,
    `numeric_ptr` numeric(5, 2),

    `float` float NOT NULL,
    `float_ptr` float,

    `double` double NOT NULL,
    `double_ptr` double,

    `real` real NOT NULL,
    `real_ptr` real,

-- bit values

    `bit` bit(10) NOT NULL,
    `bit_ptr` bit(10),

-- date and time
    `time` time NOT NULL,
    `time_ptr` time,

    `date` date NOT NULL,
    `date_ptr` date,

    `date_time` datetime NOT NULL,
    `date_time_ptr` datetime,

    `timestamp` timestamp NOT NULL,
    `timestamp_ptr` timestamp,

    `year` year NOT NULL,
    `year_ptr` year,

-- strings

    `char` char(20) NOT NULL,
    `char_ptr` char(20),

    `var_char` varchar(20) NOT NULL,
    `var_char_ptr` varchar(20),

    `binary` binary(20) NOT NULL,
    `binary_ptr` binary(20),

    `var_binary` varbinary(20) NOT NULL,
    `var_binary_ptr` varbinary(20),

    `blob` blob NOT NULL,
    `blob_ptr` blob,

    `text` text NOT NULL,
    `text_ptr` text,

    `enum` enum('value1', 'value2', 'value3') NOT NULL,
    `enum_ptr` enum('value1', 'value2', 'value3'),

    `set` set('s1', 's2', 's3') NOT NULL,
    `set_ptr` set('s1', 's2', 's3'),

-- json

    `json` json NOT NULL,
    `json_ptr` json

    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `all_types` VALUES
(false, true,
-3,3,14,14,-150,150,-1600,1600,5000,50000,
-3,3,14,14,-150,150,-1600,1600,50000,50000,
1.11,1.11,2.22,2.22,3.33,3.33,4.44,4.44,5.55,5.55,
_binary '\0',_binary '\0',
'10:11:12.33', '10:11:12.33', '2008-07-04','2008-07-04','2011-12-18 13:17:17','2011-12-18 13:17:17','2007-12-31 23:00:01','2007-12-31 23:00:01',2004,2004,'char','char','varchar','varchar',_binary 'binary\0\0\0\0\0\0\0\0\0\0\0\0\0\0',_binary 'binary\0\0\0\0\0\0\0\0\0\0\0\0\0\0',_binary 'varbinary',_binary 'varbinary',_binary 'blob',_binary 'blob','text','text','value1','value1','s1','s2','{\"key1\": \"value1\", \"key2\": \"value2\"}','{\"key1\": \"value1\", \"key2\": \"value2\"}'),
(false, NULL,
-3,3,14,14,-150,150,-1600,1600,5000,50000,
NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
1.11,NULL,2.22,NULL,3.33,NULL,4.44,NULL,5.55,NULL,
_binary '\0',NULL,
'10:11:12.33', NULL, '2008-07-04',NULL,'2011-12-18 13:17:17',NULL,'2007-12-31 23:00:01',NULL,2004,NULL,'char',NULL,'varchar',NULL,_binary 'binary\0\0\0\0\0\0\0\0\0\0\0\0\0\0',NULL,_binary 'varbinary',NULL,_binary 'blob',NULL,'text',NULL,'value1',NULL,'s1',NULL,'{\"key1\": \"value1\", \"key2\": \"value2\"}',NULL);



-- Link table --------------------

DROP TABLE IF EXISTS test_sample.link;

CREATE TABLE IF NOT EXISTS test_sample.link (
    id int PRIMARY KEY AUTO_INCREMENT,
    url VARCHAR (255) NOT NULL,
    name VARCHAR (255) NOT NULL,
    description VARCHAR (255)
);

INSERT INTO test_sample.link (ID, url, name, description) VALUES
(0, 'http://www.youtube.com', 'Youtube' , '');

-- Link2 table --------------------

DROP TABLE IF EXISTS test_sample.link2;

CREATE TABLE IF NOT EXISTS test_sample.link2 (
    id int PRIMARY KEY AUTO_INCREMENT,
    url VARCHAR (255) NOT NULL,
    name VARCHAR (255) NOT NULL,
    description VARCHAR (255)
);

INSERT INTO test_sample.link2 (ID, url, name, description) VALUES
(0, 'http://www.youtube.com', 'Youtube' , '');