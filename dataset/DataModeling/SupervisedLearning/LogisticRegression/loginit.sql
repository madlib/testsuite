SET client_min_messages TO WARNING;DROP TABLE IF EXISTS madlibtestdata.log_noobservation;
CREATE TABLE madlibtestdata.log_noobservation (x float8[],y boolean);
ALTER TABLE madlibtestdata.log_noobservation OWNER TO madlibtester;

SET client_min_messages TO WARNING;DROP TABLE IF EXISTS madlibtestdata.log_singleobservation;
CREATE TABLE madlibtestdata.log_singleobservation (x float8[],y boolean);
COPY madlibtestdata.log_singleobservation FROM STDIN NULL '?' ;
{2, 1}	f
\.
ALTER TABLE madlibtestdata.log_singleobservation OWNER TO madlibtester;

SET client_min_messages TO WARNING;DROP TABLE IF EXISTS madlibtestdata.log_redundantobservations;
CREATE TABLE madlibtestdata.log_redundantobservations (x float8[],y boolean);
COPY madlibtestdata.log_redundantobservations FROM STDIN NULL '?' ;
{2.0,1}	f
{2.0,1}	f
{4.0,1}	f
\.
ALTER TABLE madlibtestdata.log_redundantobservations OWNER TO madlibtester;

