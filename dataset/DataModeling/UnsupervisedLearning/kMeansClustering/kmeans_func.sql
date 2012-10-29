CREATE LANGUAGE plpythonu;

DROP FUNCTION IF EXISTS madlibtestdata.dist_norm4(x FLOAT8[], y FLOAT8[]);
DROP FUNCTION IF EXISTS madlibtestdata.squared_dist_invalid_signature_datatype(x TEXT, y TEXT);
DROP FUNCTION IF EXISTS madlibtestdata.squared_dist_invalid_signature_numparam(x FLOAT8[], y FLOAT8[], p FLOAT8);

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

ALTER FUNCTION madlibtestdata.dist_norm4(x FLOAT8[], y FLOAT8[]) OWNER TO madlibtester;
ALTER FUNCTION madlibtestdata.squared_dist_invalid_signature_datatype(x TEXT, y TEXT) OWNER TO madlibtester;
ALTER FUNCTION madlibtestdata.squared_dist_invalid_signature_numparam(x FLOAT8[], y FLOAT8[], p FLOAT8) OWNER TO madlibtester;



DROP AGGREGATE IF EXISTS madlibtestdata.array_avg_agg(FLOAT8[]) CASCADE;
DROP FUNCTION IF EXISTS madlibtestdata.array_avg_t(FLOAT8[], FLOAT8[]) CASCADE;
DROP FUNCTION IF EXISTS madlibtestdata.array_avg_f(FLOAT8[]) CASCADE;

CREATE OR REPLACE FUNCTION madlibtestdata.array_avg_t(FLOAT8[], FLOAT8[])
RETURNS FLOAT8[]
RETURNS NULL ON NULL INPUT
LANGUAGE sql 
IMMUTABLE
AS $$
    SELECT array_append( array_agg(e.val+COALESCE(s.val,0.0) ORDER BY e.idx ASC), COALESCE($1[array_upper($2,1)+1],0.0)+1.0)
    FROM (SELECT generate_series(1, array_upper($2,1)) AS idx, unnest($1[1:array_upper($2,1)]) AS val) AS s RIGHT OUTER JOIN
         (SELECT generate_series(1, array_upper($2,1)) AS idx, unnest($2) AS val) AS e
    ON s.idx=e.idx;
$$;

CREATE OR REPLACE FUNCTION madlibtestdata.array_avg_f(FLOAT8[])
RETURNS FLOAT8[]
RETURNS NULL ON NULL INPUT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE WHEN (COALESCE($1[array_upper($1,1)],0.0)::INT4 = 0) THEN NULL
                ELSE array_agg(s.val/$1[array_upper($1,1)] ORDER BY s.idx ASC)
           END
    FROM (SELECT generate_series(1, array_upper($1,1)) AS idx, unnest($1) AS val) AS s
    WHERE s.idx < array_upper($1,1);
$$;

CREATE AGGREGATE madlibtestdata.array_avg_agg (
    BASETYPE = FLOAT8[],
    SFUNC = madlibtestdata.array_avg_t,
    STYPE = FLOAT8[],
    FINALFUNC = madlibtestdata.array_avg_f,
    INITCOND = '{}'
);

ALTER AGGREGATE madlibtestdata.array_avg_agg(FLOAT8[]) OWNER TO madlibtester;
ALTER FUNCTION madlibtestdata.array_avg_t(FLOAT8[], FLOAT8[]) OWNER TO madlibtester;
ALTER FUNCTION madlibtestdata.array_avg_f(FLOAT8[]) OWNER TO madlibtester;



-- DROP TABLE IF EXISTS madlibtestdata.km_us_census_1990_rows_10_times;
-- CREATE TABLE madlibtestdata.km_us_census_1990_rows_10_times(pid int8, position float8[]);
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid, position FROM madlibtestdata.km_us_census_1990;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+2458285, position FROM madlibtestdata.km_us_census_1990;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+4916570, position FROM madlibtestdata.km_us_census_1990;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+7374855, position FROM madlibtestdata.km_us_census_1990;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+9833140, position FROM madlibtestdata.km_us_census_1990;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+12291425, position FROM madlibtestdata.km_us_census_1990;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+14749710, position FROM madlibtestdata.km_us_census_1990;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+17207995, position FROM madlibtestdata.km_us_census_1990;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+19666280, position FROM madlibtestdata.km_us_census_1990;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_10_times SELECT pid+22124565, position FROM madlibtestdata.km_us_census_1990;
-- ALTER TABLE madlibtestdata.km_us_census_1990_rows_10_times OWNER TO madlibtester;

-- DROP TABLE IF EXISTS madlibtestdata.km_us_census_1990_rows_100_times;
-- CREATE TABLE madlibtestdata.km_us_census_1990_rows_100_times(pid int8, position float8[]);
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+24582850, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+49165700, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+73748550, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+98331400, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+122914250, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+147497100, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+172079950, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+196662800, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
-- INSERT INTO madlibtestdata.km_us_census_1990_rows_100_times SELECT pid+221245650, position FROM madlibtestdata.km_us_census_1990_rows_10_times;
-- ALTER TABLE madlibtestdata.km_us_census_1990_rows_100_times OWNER TO madlibtester;

-- DROP TABLE IF EXISTS madlibtestdata.km_p53_columns_10_times;
-- CREATE TABLE madlibtestdata.km_p53_columns_10_times AS SELECT pid, array_cat(array_cat(array_cat(array_cat(position, position),array_cat(position, position)),array_cat(array_cat(position, position),array_cat(position, position))),array_cat(position, position)) AS position FROM madlibtestdata.km_p53;
-- ALTER TABLE madlibtestdata.km_p53_columns_10_times OWNER TO madlibtester;

-- DROP TABLE IF EXISTS madlibtestdata.km_p53_columns_100_times;
-- CREATE TABLE madlibtestdata.km_p53_columns_100_times AS SELECT pid, array_cat(array_cat(array_cat(array_cat(position, position),array_cat(position, position)),array_cat(array_cat(position, position),array_cat(position, position))),array_cat(position, position)) AS position FROM madlibtestdata.km_p53_columns_10_times;
-- ALTER TABLE madlibtestdata.km_p53_columns_100_times OWNER TO madlibtester;

-- DROP TABLE IF EXISTS madlibtestdata.km_tfidf3_rows_2_times;
-- CREATE TABLE madlibtestdata.km_tfidf3_rows_2_times(pid int8, position float8[]);
-- INSERT INTO madlibtestdata.km_tfidf3_rows_2_times SELECT pid, position FROM madlibtestdata.km_tfidf3;
-- INSERT INTO madlibtestdata.km_tfidf3_rows_2_times SELECT pid+703250, position FROM madlibtestdata.km_tfidf3;
-- ALTER TABLE madlibtestdata.km_tfidf3_rows_2_times OWNER TO madlibtester;

-- DROP TABLE IF EXISTS madlibtestdata.km_tfidf3_rows_4_times;
-- CREATE TABLE madlibtestdata.km_tfidf3_rows_4_times(pid int8, position float8[]);
-- INSERT INTO madlibtestdata.km_tfidf3_rows_4_times SELECT pid, position FROM madlibtestdata.km_tfidf3_rows_2_times;
-- INSERT INTO madlibtestdata.km_tfidf3_rows_4_times SELECT pid+1406500, position FROM madlibtestdata.km_tfidf3_rows_2_times;
-- ALTER TABLE madlibtestdata.km_tfidf3_rows_4_times OWNER TO madlibtester;

-- DROP TABLE IF EXISTS madlibtestdata.km_tfidf3_columns_2_times;
-- CREATE TABLE madlibtestdata.km_tfidf3_columns_2_times AS SELECT pid, array_cat(position, position) AS position FROM madlibtestdata.km_tfidf3;
-- ALTER TABLE madlibtestdata.km_tfidf3_columns_2_times OWNER TO madlibtester;

-- DROP TABLE IF EXISTS madlibtestdata.km_tfidf3_columns_4_times;
-- CREATE TABLE madlibtestdata.km_tfidf3_columns_4_times AS SELECT pid, array_cat(position, position) AS position FROM madlibtestdata.km_tfidf3_columns_2_times;
-- ALTER TABLE madlibtestdata.km_tfidf3_columns_4_times OWNER TO madlibtester;

