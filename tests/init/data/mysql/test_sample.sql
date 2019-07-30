-- MySQL dump 10.13  Distrib 8.0.16, for Linux (x86_64)


--
-- Table structure for table `all_types8`
--

DROP TABLE IF EXISTS `all_types`;

CREATE TABLE `all_types` (
    `boolean` BOOLEAN NOT NULL,
    `boolean_ptr` BOOLEAN,

   `tiny_int` TINYINT NOT NULL,
   `utiny_int` TINYINT unsigned NOT NULL,

   `small_int` SMALLINT NOT NULL,
   `usmall_int` SMALLINT unsigned NOT NULL,

   `medium_int` MEDIUMINT NOT NULL,
   `umedium_int` MEDIUMINT unsigned NOT NULL,

   `int` INT NOT NULL,
   `uint` INT unsigned NOT NULL,

   `big_int` bigint(20) NOT NULL,
    `ubig_int` bigint(20) unsigned NOT NULL,

-- ptr

    `tiny_int_ptr` TINYINT,
    `utiny_int_ptr` TINYINT unsigned,

    `small_int_ptr` SMALLINT,
    `usmall_int_ptr` SMALLINT unsigned,

    `medium_int_ptr` MEDIUMINT,
    `umedium_int_ptr` MEDIUMINT unsigned,

    `int_ptr` INT,
    `uint_ptr` INT unsigned,

    `big_int_ptr` bigint(20),
    `ubig_int_ptr` bigint(20) unsigned,


-- floats
    `decimal` decimal(5, 2) unsigned NOT NULL,
    `decimal_ptr` decimal(5,2),

    `numeric` numeric(5,2) NOT NULL,
    `numeric_ptr` numeric(5, 2),

    `float` float NOT NULL,
    `float_ptr` float,

    `double` double NOT NULL,
    `double_ptr` double,

-- bit values

    `bit` bit(10) NOT NULL,
    `bit_ptr` bit(10),

-- date and time

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

    `varchar` varchar(20) NOT NULL,
    `varchar_ptr` varchar(20),

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
(false, true, -3,3,-14,14,-150,150,-1600,1600,-17000,17000,-3,3,-14,14,-150,150,-1600,1600,-17000,17000,1.11,1.11,2.22,2.22,3.33,3.33,4.44,4.44,_binary '\0',_binary '\0','2008-07-04','2008-07-04','2011-12-18 13:17:17','2011-12-18 13:17:17','2007-12-31 23:00:01','2007-12-31 23:00:01',2004,2004,'char','char','varchar','varchar',_binary 'binary\0\0\0\0\0\0\0\0\0\0\0\0\0\0',_binary 'binary\0\0\0\0\0\0\0\0\0\0\0\0\0\0',_binary 'varbinary',_binary 'varbinary',_binary 'blob',_binary 'blob','text','text','value1','value1','s1','s2','{\"key1\": \"value1\", \"key2\": \"value2\"}','{\"key1\": \"value1\", \"key2\": \"value2\"}'),
(false, NULL, -3,3,-14,14,-150,150,-1600,1600,-17000,17000,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1.11,NULL,2.22,NULL,3.33,NULL,4.44,NULL,_binary '\0',NULL,'2008-07-04',NULL,'2011-12-18 13:17:17',NULL,'2007-12-31 23:00:01',NULL,2004,NULL,'char',NULL,'varchar',NULL,_binary 'binary\0\0\0\0\0\0\0\0\0\0\0\0\0\0',NULL,_binary 'varbinary',NULL,_binary 'blob',NULL,'text',NULL,'value1',NULL,'s1',NULL,'{\"key1\": \"value1\", \"key2\": \"value2\"}',NULL);



