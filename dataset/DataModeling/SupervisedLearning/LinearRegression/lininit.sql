SET client_min_messages TO WARNING;
DROP TABLE IF EXISTS madlibtestdata.lin_noobservation_oi;
CREATE TABLE madlibtestdata.lin_noobservation_oi (x float8[],y float8);
ALTER TABLE madlibtestdata.lin_noobservation_oi OWNER TO madlibtester;
SET client_min_messages TO WARNING;
DROP TABLE IF EXISTS madlibtestdata.lin_noobservation_wi;
CREATE TABLE madlibtestdata.lin_noobservation_wi (x float8[],y float8);
ALTER TABLE madlibtestdata.lin_noobservation_wi OWNER TO madlibtester;


SET client_min_messages TO WARNING;DROP TABLE IF EXISTS madlibtestdata.lin_singleobservation_oi;
CREATE TABLE madlibtestdata.lin_singleobservation_oi (x float8[],y float8);
COPY madlibtestdata.lin_singleobservation_oi FROM STDIN NULL '?';
{5.0, 2.0}	3.0
\.
ALTER TABLE madlibtestdata.lin_singleobservation_oi OWNER TO madlibtester;SET client_min_messages TO WARNING;DROP TABLE IF EXISTS madlibtestdata.lin_singleobservation_wi;
CREATE TABLE madlibtestdata.lin_singleobservation_wi (x float8[],y float8);
COPY madlibtestdata.lin_singleobservation_wi FROM STDIN NULL '?';
{1, 5.0, 2.0}	3.0
\.
ALTER TABLE madlibtestdata.lin_singleobservation_wi OWNER TO madlibtester;

SET client_min_messages TO WARNING;DROP TABLE IF EXISTS madlibtestdata.lin_redundantobservations_oi;
CREATE TABLE madlibtestdata.lin_redundantobservations_oi (x float8[],y float8);
COPY madlibtestdata.lin_redundantobservations_oi FROM STDIN NULL '?';
{2.0,3.0}	5.0
{2.0,3.0}	5.0
{4.0,6.0}	10.0
\.
ALTER TABLE madlibtestdata.lin_redundantobservations_oi OWNER TO madlibtester;SET client_min_messages TO WARNING;DROP TABLE IF EXISTS madlibtestdata.lin_redundantobservations_wi;
CREATE TABLE madlibtestdata.lin_redundantobservations_wi (x float8[],y float8);
COPY madlibtestdata.lin_redundantobservations_wi FROM STDIN NULL '?';
{1,2.0,3.0}	5.0
{1,2.0,3.0}	5.0
{1,4.0,6.0}	10.0
\.
ALTER TABLE madlibtestdata.lin_redundantobservations_wi OWNER TO madlibtester;

--not yet done
DROP SEQUENCE IF EXISTS madlibtestdata.lin_communities_agg_seq;
CREATE SEQUENCE madlibtestdata.lin_communities_agg_seq MAXVALUE 2 CYCLE MINVALUE 0;
DROP TABLE IF EXISTS madlibtestdata.lin_communities_oi_agg, madlibtestdata.lin_communities_wi_agg;
CREATE TABLE madlibtestdata.lin_communities_oi_agg AS SELECT nextval('madlibtestdata.lin_communities_agg_seq') AS id, madlibtestdata.lin_communities_oi.* FROM madlibtestdata.lin_communities_oi;
CREATE TABLE madlibtestdata.lin_communities_wi_agg AS SELECT nextval('madlibtestdata.lin_communities_agg_seq') AS id, madlibtestdata.lin_communities_wi.* FROM madlibtestdata.lin_communities_wi;

ALTER TABLE madlibtestdata.lin_communities_oi_agg OWNER TO madlibtester;
ALTER TABLE madlibtestdata.lin_communities_wi_agg OWNER TO madlibtester;
