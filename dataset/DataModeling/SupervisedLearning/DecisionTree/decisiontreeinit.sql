SET client_min_messages TO WARNING;
DROP TABLE IF EXISTS madlibtestdata.dt_emptytable, madlibtestdata.dt_golf_nullclass, madlibtestdata.dt_nursery_nullclass, madlibtestdata.dt_golf_sameclass, madlibtestdata.dt_golf_duplicate, madlibtestdata.dt_golf_duplicate, madlibtestdata.dt_golf_duplicateid, madlibtestdata.dt_golf_conflict, madlibtestdata.dt_nursery_conflict, madlibtestdata.dt_tablealldatatypes, madlibtestdata.dt_golfwithclass;

CREATE TABLE madlibtestdata.dt_emptytable(id int, c1 int, c2 int, class text);
CREATE TABLE madlibtestdata.dt_golf_nullclass as select id, outlook, temperature, humidity, windy, NULL::TEXT as class FROM madlibtestdata.dt_golf;
CREATE TABLE madlibtestdata.dt_nursery_nullclass as select * FROM madlibtestdata.dt_nursery;
UPDATE madlibtestdata.dt_nursery_nullclass set class = NULL WHERE id < 100;

CREATE TABLE madlibtestdata.dt_golf_sameclass as select id, outlook, temperature, humidity, windy, 'Play'::TEXT as class FROM madlibtestdata.dt_golf;
CREATE TABLE madlibtestdata.dt_golf_duplicate as select id, outlook, temperature, humidity, windy, class FROM madlibtestdata.dt_golf;
INSERT into madlibtestdata.dt_golf_duplicate select id+14, outlook, temperature, humidity, windy, class FROM madlibtestdata.dt_golf;

CREATE TABLE madlibtestdata.dt_golf_duplicateid as select id, outlook, temperature, humidity, windy, class FROM madlibtestdata.dt_golf;
INSERT into madlibtestdata.dt_golf_duplicateid select id, outlook, temperature, humidity, windy, class FROM madlibtestdata.dt_golf;

CREATE TABLE madlibtestdata.dt_golf_conflict as select id, outlook, temperature, humidity, windy, case when class = ' Play' THEN ' Do not Play' ELSE ' Play' END AS class FROM madlibtestdata.dt_golf;
INSERT into madlibtestdata.dt_golf_conflict select id+14, outlook, temperature, humidity, windy, class FROM madlibtestdata.dt_golf;

CREATE TABLE madlibtestdata.dt_nursery_conflict as select * FROM madlibtestdata.dt_nursery;
INSERT into  madlibtestdata.dt_nursery_conflict select id+20000, parents,has_nurs,form,children,housing,finance,social,health,'recommend' as class FROM madlibtestdata.dt_nursery WHERE id < 100;

CREATE TABLE madlibtestdata.dt_golfwithclass as select id, outlook as class, temperature, humidity, windy, class as realclass FROM madlibtestdata.dt_golf;
-----------------------All data type table-------------------
CREATE TABLE madlibtestdata.dt_tablealldatatypes (id INT,id1 BOOL,id2 INET,id3 CIDR,id4 MACADDR,id5 BIT(8),id6 BIT VARYING(10),id7 BOOLEAN,id8 BYTEA,id9 BIGINT,id10 INT4,id11 MONEY,id12 SMALLINT,id13 FLOAT8,id14 REAL,id15 NUMERIC,id16 CHAR(4),id17 VARCHAR(16),id18 TEXT,id19 "char",id20 CHAR,id21 DATE,id22 TIME,id23 TIMETZ,id24 TIMESTAMP,id25 TIMESTAMPTZ,id26 INTERVAL,id27 POINT,id28 PATH,id29 LSEG,id30 CIRCLE,id31 POLYGON,id32 BOX)  ;

INSERT INTO  madlibtestdata.dt_tablealldatatypes VALUES (0,TRUE,'192.168.0.1'::INET,'192.168.0.1/32'::CIDR,'12:34:56:78:90:AB'::MACADDR,B'11001111'::BIT(8),B'11001111'::BIT VARYING(10),FALSE::BOOLEAN,E'\\000'::BYTEA,(-9223372036854775808)::BIGINT,(-2147483648)::INT4,'$-21474836.48'::MONEY,(-32768)::SMALLINT,123456789.98765432::FLOAT8,39.333::REAL,1234567890.0987654321::NUMERIC,'SSSS'::CHAR(4),'ASDFGHJKL'::VARCHAR(16),'QAZWSXEDCRFVTGB'::TEXT,'a'::"char",'B'::CHAR,'2011-08-12'::DATE,'10:00:52.14'::TIME,'2011-08-12 10:00:52.14'::TIMETZ,'2011-08-12 10:00:52.14'::TIMESTAMP,'2011-08-12 10:00:52.14'::TIMESTAMPTZ,'1 DAY 12 HOURS 59 MIN 10 SEC'::INTERVAL,'(1,2)'::POINT,'(1,1),(2,2)'::PATH,'((1,1),(2,2))'::LSEG,'<(1,1),2>'::CIRCLE,'(0,0),(1,1),(2,0)'::POLYGON,'((1,2),(3,4))'::BOX);

INSERT INTO  madlibtestdata.dt_tablealldatatypes VALUES (1,FALSE,NULL,NULL,NULL,NULL,NULL,TRUE::BOOLEAN,NULL,9223372036854775807::BIGINT,2147483647::INT4,'$21474836.47'::MONEY,32767::SMALLINT,'NAN'::FLOAT8,'NAN'::REAL,'NAN'::NUMERIC,NULL,NULL,NULL,null::"char",NULL::CHAR,'EPOCH'::DATE,'ALLBALLS'::TIME,NULL,NULL,NULL,'1 12:59:10'::INTERVAL,NULL,NULL,NULL,NULL,NULL,NULL);

INSERT INTO  madlibtestdata.dt_tablealldatatypes VALUES (2,TRUE,'192.168.0.1'::INET,'192.168.0.1/32'::CIDR,'12:34:56:78:90:AB'::MACADDR,B'11001111'::BIT(8),B'11001111'::BIT VARYING(10),NULL,E'\\000'::BYTEA,123456789::BIGINT,1234,'$1234'::MONEY,1234::SMALLINT,'-INFINITY'::FLOAT8,'-INFINITY'::REAL,2::NUMERIC^128 / 3::NUMERIC^129,'SSSS'::CHAR(4),'ASDFGHJKL'::VARCHAR(16),'QAZWSXEDCRFVTGB'::TEXT,'a'::"char",'B'::CHAR,NULL,NULL,'2011-08-12 10:00:52.14'::TIMETZ,'2011-08-12 10:00:52.14'::TIMESTAMP,'2011-08-12 10:00:52.14'::TIMESTAMPTZ,NULL,'(1,2)'::POINT,'(1,1),(2,2)'::PATH,'((1,1),(2,2))'::LSEG,'<(1,1),2>'::CIRCLE,'(0,0),(1,1),(2,0)'::POLYGON,'((1,2),(3,4))'::BOX);

INSERT INTO  madlibtestdata.dt_tablealldatatypes VALUES (3,FALSE,NULL,NULL,NULL,NULL,NULL,FALSE::BOOLEAN,NULL,NULL,NULL,NULL,NULL,'+INFINITY'::FLOAT8,'+INFINITY'::REAL,2::NUMERIC^128,NULL,NULL,NULL,null::"char",NULL::CHAR,'2011-08-12'::DATE,'10:00:52.14'::TIME,NULL,NULL,NULL,'1 DAY 12 HOURS 59 MIN 10 SEC'::INTERVAL,NULL,NULL,NULL,NULL,NULL,NULL);

INSERT INTO  madlibtestdata.dt_tablealldatatypes VALUES (4,TRUE,'192.168.0.1'::INET,'192.168.0.1/32'::CIDR,'12:34:56:78:90:AB'::MACADDR,B'11001111'::BIT(8),B'11001111'::BIT VARYING(10),TRUE::BOOLEAN,E'\\000'::BYTEA,(-9223372036854775808)::BIGINT,(-2147483648)::INT4,'$-21474836.48'::MONEY,(-32768)::SMALLINT,NULL,NULL,0.5::NUMERIC,'SSSS'::CHAR(4),'ASDFGHJKL'::VARCHAR(16),'QAZWSXEDCRFVTGB'::TEXT,'a'::"char",'B'::CHAR,'EPOCH'::DATE,'ALLBALLS'::TIME,'2011-08-12 10:00:52.14'::TIMETZ,'2011-08-12 10:00:52.14'::TIMESTAMP,'2011-08-12 10:00:52.14'::TIMESTAMPTZ,'1 12:59:10'::INTERVAL,'(1,2)'::POINT,'(1,1),(2,2)'::PATH,'((1,1),(2,2))'::LSEG,'<(1,1),2>'::CIRCLE,'(0,0),(1,1),(2,0)'::POLYGON,'((1,2),(3,4))'::BOX);

INSERT INTO  madlibtestdata.dt_tablealldatatypes VALUES (5,FALSE,NULL,NULL,NULL,NULL,NULL,NULL,NULL,9223372036854775807::BIGINT,2147483647::INT4,'$21474836.47'::MONEY,32767::SMALLINT,123456789.98765432::FLOAT8,39.333::REAL,NULL,NULL,NULL,NULL,null::"char",NULL::CHAR,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);

ALTER TABLE madlibtestdata.dt_emptytable OWNER TO madlibtester;
ALTER TABLE  madlibtestdata.dt_golf_nullclass OWNER TO madlibtester;
ALTER TABLE  madlibtestdata.dt_nursery_nullclass OWNER TO madlibtester;
ALTER TABLE madlibtestdata.dt_golf_sameclass OWNER TO madlibtester;
ALTER TABLE madlibtestdata.dt_golf_duplicate OWNER TO madlibtester;
ALTER TABLE madlibtestdata.dt_golf_duplicate OWNER TO madlibtester;
ALTER TABLE madlibtestdata.dt_golf_conflict OWNER TO madlibtester;
ALTER TABLE madlibtestdata.dt_nursery_conflict OWNER TO madlibtester;
ALTER TABLE madlibtestdata.dt_tablealldatatypes OWNER TO madlibtester;
ALTER TABLE madlibtestdata.dt_golfwithclass OWNER TO madlibtester;
ALTER TABLE madlibtestdata.dt_golf_duplicateid OWNER TO madlibtester;
