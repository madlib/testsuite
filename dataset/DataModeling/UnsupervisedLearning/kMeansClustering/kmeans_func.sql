CREATE LANGUAGE plpythonu;

DROP FUNCTION IF EXISTS madlibtestdata.dist_norm4(x FLOAT8[], y FLOAT8[]);
DROP FUNCTION IF EXISTS madlibtestdata.squared_dist_invalid_signature_datatype(x TEXT, y TEXT);
DROP FUNCTION IF EXISTS madlibtestdata.squared_dist_invalid_signature_numparam(x FLOAT8[], y FLOAT8[], p FLOAT8);
DROP AGGREGATE IF EXISTS madlibtestdata.median(FLOAT8);
DROP FUNCTION IF EXISTS madlibtestdata.array_median(FLOAT8[]);

CREATE OR REPLACE FUNCTION madlibtestdata.dist_norm4(x FLOAT8[], y FLOAT8[])
RETURNS FLOAT8
RETURNS NULL ON NULL INPUT
LANGUAGE plpythonu
VOLATILE
AS $$
    import math

    dist = 0.0
    for i in range(0, len(x)):
        dist += math.pow(math.fabs(x[i]-y[i]), 4.00)
    dist = pow(dist, 0.25)

    return dist
$$;

CREATE OR REPLACE FUNCTION madlibtestdata.squared_dist_invalid_signature_datatype(x TEXT, y TEXT)
RETURNS TEXT
RETURNS NULL ON NULL INPUT
LANGUAGE plpythonu
VOLATILE
AS $$
    return x+y
$$;

CREATE OR REPLACE FUNCTION madlibtestdata.squared_dist_invalid_signature_numparam(x FLOAT8[], y FLOAT8[], p FLOAT8)
RETURNS FLOAT8
RETURNS NULL ON NULL INPUT
LANGUAGE plpythonu
VOLATILE
AS $$
    return p
$$;


DROP TABLE IF EXISTS madlibtestdata.km_us_census_1990_rows_10_times;
CREATE TABLE madlibtestdata.km_us_census_1990_rows_10_times(pid int8, position float8[]);
INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid, position FROM madlibtestdata.km_us_census_1990;
INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+2458285, position FROM madlibtestdata.km_us_census_1990;
INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+4916570, position FROM madlibtestdata.km_us_census_1990;
INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+7374855, position FROM madlibtestdata.km_us_census_1990;
INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+9833140, position FROM madlibtestdata.km_us_census_1990;
INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+12291425, position FROM madlibtestdata.km_us_census_1990;
INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+14749710, position FROM madlibtestdata.km_us_census_1990;
INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+17207995, position FROM madlibtestdata.km_us_census_1990;
INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+19666280, position FROM madlibtestdata.km_us_census_1990;
INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+22124565, position FROM madlibtestdata.km_us_census_1990;

DROP TABLE IF EXISTS madlibtestdata.km_us_census_1990_rows_100_times;
CREATE TABLE madlibtestdata.km_us_census_1990_rows_100_times(pid int8, position float8[]);
INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+24582850, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+49165700, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+73748550, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+98331400, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+122914250, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+147497100, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+172079950, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+196662800, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+221245650, position FROM madlibtestdata.km_us_census_1990_rows_10_times;

DROP TABLE IF EXISTS madlibtestdata.km_p53_columns_10_times;
CREATE TABLE madlibtestdata.km_p53_columns_10_times AS SELECT pid, array_cat(array_cat(array_cat(array_cat(position, position),array_cat(position, position)),array_cat(array_cat(position, position),array_cat(position, position))),array_cat(position, position)) AS position FROM madlibtestdata.km_p53;

DROP TABLE IF EXISTS madlibtestdata.km_p53_columns_100_times;
CREATE TABLE madlibtestdata.km_p53_columns_100_times AS SELECT pid, array_cat(array_cat(array_cat(array_cat(position, position),array_cat(position, position)),array_cat(array_cat(position, position),array_cat(position, position))),array_cat(position, position)) AS position FROM madlibtestdata.km_p53_columns_10_times;

DROP TABLE IF EXISTS madlibtestdata.km_tfidf3_rows_2_times;
CREATE TABLE madlibtestdata.km_tfidf3_rows_2_times(pid int8, position float8[]);
INSERT INTO madlibtestdata.km_tfidf3_rows_2_times SELECT pid, position FROM madlibtestdata.km_tfidf3;
INSERT INTO madlibtestdata.km_tfidf3_rows_2_times SELECT pid+703250, position FROM madlibtestdata.km_tfidf3;

DROP TABLE IF EXISTS madlibtestdata.km_tfidf3_rows_4_times;
CREATE TABLE madlibtestdata.km_tfidf3_rows_4_times(pid int8, position float8[]);
INSERT INTO madlibtestdata.km_tfidf3_rows_4_times SELECT pid, position FROM madlibtestdata.km_tfidf3_rows_2_times;
INSERT INTO madlibtestdata.km_tfidf3_rows_4_times SELECT pid+1406500, position FROM madlibtestdata.km_tfidf3_rows_2_times;

DROP TABLE IF EXISTS madlibtestdata.km_tfidf3_columns_2_times;
CREATE TABLE madlibtestdata.km_tfidf3_columns_2_times AS SELECT pid, array_cat(position, position) AS position FROM madlibtestdata.km_tfidf3;

DROP TABLE IF EXISTS madlibtestdata.km_tfidf3_columns_4_times;
CREATE TABLE madlibtestdata.km_tfidf3_columns_4_times AS SELECT pid, array_cat(position, position) AS position FROM madlibtestdata.km_tfidf3_columns_2_times;

