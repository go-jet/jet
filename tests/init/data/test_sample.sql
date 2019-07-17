
-- AllTypes table -----------------------------

create schema IF NOT EXISTS test_sample;

DROP TABLE IF EXISTS test_sample.all_types;

CREATE TABLE test_sample.ALL_TYPES
(
    -- numeric
    smallint_ptr smallint,
    smallint smallint NOT NULL,
    integer_ptr  integer,
    integer integer NOT NULL,
    bigint_ptr   bigint,
    bigint bigint NOT NULL,
    decimal_ptr decimal(10, 2),
    decimal decimal(10, 2) NOT NULL,
    numeric_ptr numeric(20, 3),
    numeric numeric(20,3) NOT NULL,
    real_ptr    real,
    real        real NOT NULL,
    double_precision_ptr double precision,
    double_precision double precision NOT NULL,
    smallserial     smallserial NOT NULL,
    serial serial NOT NULL,
    bigserial bigserial NOT NULL,

    --monetary
--     money_ptr money,
--     money     money NOT NULL,

    character_varying_ptr character varying(100),
    character_varying character varying(200) NOT NULL,
    character_ptr character(80),
    character character(80) NOT NULL,
    text_ptr text,
    text text NOT NULL,

    --binary
    bytea_ptr bytea,
    bytea bytea NOT NULL,

    --datetime
    timestampz_ptr timestamp with time zone,
    timestampz timestamp with time zone NOT NULL,
    timestamp_ptr timestamp without time zone,
    timestamp timestamp without time zone NOT NULL,
    date_ptr date,
    date date NOT NULL,
    timez_ptr time with time zone,
    timez time with time zone NOT NULL,
    time_ptr time without time zone,
    time time without time zone NOT NULL,
    interval_ptr interval,
    interval interval NOT NULL,

    --boolean
    boolean_ptr boolean,
    boolean boolean NOT NULL,

    --geometry
    point_ptr point,

    --bitstrings
    bit_ptr bit(3),
    bit bit(3) NOT NULL,
    bit_varying_ptr bit varying(20),
    bit_varying bit varying(40) NOT NULL,

    --textsearch
    tsvector_ptr tsvector,
    tsvector tsvector NOT NULL,

    --uuid
    uuid_ptr uuid,
    uuid uuid NOT NULL,

    --xml
    xml_ptr xml,
    xml xml NOT NULL,

    --json
    json_ptr json,
    json json NOT NULL,
    jsonb_ptr jsonb,
    jsonb jsonb NOT NULL,

    --array
    integer_array_ptr integer[],
    integer_array     integer[] NOT NULL,
    text_array_ptr    text[],
    text_array        text[] NOT NULL,
    jsonb_array       jsonb[] NOT NULL,
    text_multi_dim_array_ptr text[][],
    text_multi_dim_array text[][] NOT NULL
);

INSERT INTO test_sample.ALL_types(
    smallint_ptr, "smallint", integer_ptr, "integer", bigint_ptr, "bigint", decimal_ptr, "decimal", numeric_ptr, "numeric", real_ptr, "real", double_precision_ptr, double_precision, smallserial, serial, bigserial,
--     money_ptr, money,
    character_varying_ptr, character_varying, character_ptr, "character", text_ptr, text,
    bytea_ptr, bytea,
    timestampz_ptr, timestampz, timestamp_ptr, "timestamp", date_ptr, date, timez_ptr, timez, time_ptr, "time", interval_ptr, "interval",
    boolean_ptr, "boolean",
    point_ptr,
    bit_ptr, "bit", bit_varying_ptr, bit_varying,
    tsvector_ptr, tsvector,
    uuid_ptr, uuid,
    xml_ptr, xml,
    json_ptr, json, jsonb_ptr, jsonb,
    integer_array_ptr, integer_array, text_array_ptr, text_array, jsonb_array, text_multi_dim_array_ptr, text_multi_dim_array)
VALUES (1, 1, 300, 300, 50000, 5000, 11.44, 11.44, 55.77, 55.77, 99.1, 99.1, 11111111.22, 11111111.22, DEFAULT, DEFAULT, DEFAULT,
--         100000, 100000,
        'ABBA', 'ABBA', 'JOHN', 'JOHN', 'Some text', 'Some text',
        'bytea', 'bytea',
        'January 8 04:05:06 1999 PST', 'January 8 04:05:06 1999 PST', '1999-01-08 04:05:06', '1999-01-08 04:05:06', '1999-01-08', '1999-01-08', '04:05:06 -8:00', '04:05:06 -8:00', '04:05:06', '04:05:06', '3 4:05:06', '3 4:05:06',
        TRUE, FALSE,
        '(2,3)',
        B'101', B'101', B'101111', B'101111',
        to_tsvector('supernovae'), to_tsvector('supernovae'),
        'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11',
        '<Sub>abc</Sub>', '<Sub>abc</Sub>',
        '{"a": 1, "b": 3}', '{"a": 1, "b": 3}', '{"a": 1, "b": 3}', '{"a": 1, "b": 3}',
        '{1, 2, 3}', '{1, 2, 3}', '{"breakfast", "consulting"}', '{"breakfast", "consulting"}', ARRAY['{"a": 1, "b": 2}'::jsonb, '{"a":3, "b": 4}'::jsonb], '{{"meeting", "lunch"}, {"training", "presentation"}}', '{{"meeting", "lunch"}, {"training", "presentation"}}')
        ,
       (NULL, 1, NULL, 300, NULL, 5000, NULL, 11.44, NULL, 55.77, NULL, 99.1, NULL, 11111111.22, DEFAULT, DEFAULT, DEFAULT,
--         NULL, 100000,
        NULL, 'ABBA', NULL, 'JOHN', NULL, 'Some text',
        NULL, 'bytea',
        NULL, 'January 8 04:05:06 1999 PST', NULL, '1999-01-08 04:05:06', NULL, '1999-01-08', NULL, '04:05:06 -8:00', NULL, '04:05:06', NULL, '3 4:05:06',
        NULL, FALSE,
        NULL,
        NULL, B'101', NULL, B'101111',
        NULL, to_tsvector('supernovae'),
        NULL, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11',
        NULL, '<Sub>abc</Sub>',
        NULL, '{"a": 1, "b": 3}', NULL, '{"a": 1, "b": 3}',
        NULL, '{1, 2, 3}', NULL, '{"breakfast", "consulting"}', ARRAY['{"a": 1, "b": 2}'::jsonb, '{"a":3, "b": 4}'::jsonb], NULL, '{{"meeting", "lunch"}, {"training", "presentation"}}')
       ;

-- Link table --------------------

DROP TABLE IF EXISTS test_sample.link;

CREATE TABLE IF NOT EXISTS test_sample.link (
    id serial PRIMARY KEY,
    url VARCHAR (255) NOT NULL,
    name VARCHAR (255) NOT NULL,
    description VARCHAR (255)
);

INSERT INTO test_sample.link (ID, url, name, description) VALUES
 (0, 'http://www.youtube.com', 'Youtube' , '');


-- Employee table ---------------

DROP TABLE IF EXISTS test_sample.employee;

CREATE TABLE test_sample.employee (
  employee_id INT PRIMARY KEY,
  first_name VARCHAR (255) NOT NULL,
  last_name VARCHAR (255) NOT NULL,
  employment_date timestamp with time zone,
  manager_id INT,
  FOREIGN KEY (manager_id)
      REFERENCES test_sample.employee (employee_id)
      ON DELETE CASCADE
);
INSERT INTO test_sample.employee (
    employee_id,
    first_name,
    last_name,
    employment_date,
    manager_id
)
VALUES
(1, 'Windy', 'Hays', '1999-01-08 04:05:06.100 +1:00', NULL),
(2, 'Ava', 'Christensen', '1999-01-08 04:05:06 +1:00', 1),
(3, 'Hassan', 'Conner', '1999-01-08 04:05:06 +1:00', 1),
(4, 'Anna', 'Reeves', '1999-01-08 04:05:06 +1:00', 2),
(5, 'Sau', 'Norman', '1999-01-08 04:05:06 +1:00', 2),
(6, 'Kelsie', 'Hays', '1999-01-08 04:05:06 +1:00', 3),
(7, 'Tory', 'Goff', '1999-01-08 04:05:06 +1:00', 3),
(8, 'Salley', 'Lester', '1999-01-08 04:05:06 +1:00', 3);


-- Person table ------------------

DROP TYPE IF EXISTS test_sample.MOOD CASCADE;

CREATE TYPE test_sample.MOOD AS ENUM ('sad', 'ok', 'happy');

DROP TABLE IF EXISTS test_sample.person;


CREATE TABLE test_sample.person(
    person_id uuid NOT NULL PRIMARY KEY,
    first_name varchar(100),
    last_name varchar(100),
    "Mood" test_sample.mood
);

INSERT INTO test_sample.person(person_id, first_name, last_name, "Mood") VALUES
     ('b68dbff4-a87d-11e9-a7f2-98ded00c39c6', 'Sad', 'John', 'sad'),
     ('b68dbff5-a87d-11e9-a7f2-98ded00c39c7', 'Ok', 'John', 'ok'),
     ('b68dbff6-a87d-11e9-a7f2-98ded00c39c8', 'Ok', 'John', 'ok');



-- WEIRD TABLE NAMES --------------

DROP TABLE IF EXISTS test_sample."WEIRD NAMES TABLE";

CREATE TABLE test_sample."WEIRD NAMES TABLE"(
    "weird_column_name1" varchar(100) NOT NULL,
    "Weird_Column_Name2" varchar(100) NOT NULL,
    "wEiRd_cOluMn_nAmE3" varchar(100) NOT NULL,
    "WeIrd_CoLuMN_Name4" varchar(100) NOT NULL,
    "WEIRD_COLUMN_NAME5" varchar(100) NOT NULL,

    "WeirdColumnName6" varchar(100) NOT NULL,
    "weirdColumnName7" varchar(100) NOT NULL,
    "weirdcolumnname8" varchar(100),

    "weird col name9" varchar(100) NOT NULL,
    "wEiRd cOlu nAmE10" varchar(100) NOT NULL,
    "WEIRD COLU NAME11" varchar(100) NOT NULL,
    "Weird Colu Name12" varchar(100) NOT NULL,

    "weird-col-name13" varchar(100) NOT NULL,
    "wEiRd-cOlu-nAmE14" varchar(100) NOT NULL,
    "WEIRD-COLU-NAME15" varchar(100) NOT NULL,
    "Weird-Colu-Name16" varchar(100) NOT NULL
);

INSERT INTO test_sample."WEIRD NAMES TABLE"
VALUES ('Doe', 'Doe', 'Doe', 'Doe','Doe', 'Doe', 'Doe', 'Doe','Doe', 'Doe', 'Doe', 'Doe','Doe', 'Doe', 'Doe', 'Doe');