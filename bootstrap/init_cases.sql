
DROP TABLE IF EXISTS benchmark.analyticstool CASCADE;
DROP TABLE IF EXISTS benchmark.testitems CASCADE;
DROP TABLE IF EXISTS benchmark.testsuites CASCADE;
CREATE TABLE benchmark.testsuites(
       suitename       text,
       testtype        text,
       casenum         int,
       comments        text,
       sameparameters  text,
       primary key(suitename));

CREATE TABLE benchmark.testitems(
       itemname        text,
       suitename       text,
       casename        text,
       algorithmic     text,
       method          text,
       parameters      text,
       varyparavalue   text,
       varyparaname    text,
       dataset         text,
       rownum          int,
       primary key(itemname));

CREATE  OR REPLACE VIEW benchmark.testreport as
(SELECT runid,
        suitename,
        casename,
        platform,
        bool_and(testresult like 'PASS%') as teststatus
 from benchmark.detailtestreport AS dtr,
      benchmark.testitems as ti
 WHERE dtr.itemname = ti.itemname
 GROUP BY suitename, casename, runid,platform
 ORDER BY runid, suitename, casename);
