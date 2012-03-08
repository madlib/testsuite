DROP SCHEMA IF EXISTS benchmark CASCADE;
CREATE SCHEMA benchmark;

CREATE TABLE benchmark.testitemseq(id int,  runid int);
INSERT INTO benchmark.testitemseq VALUES (0, 0);

CREATE TABLE benchmark.testsuites(
       suitename       text,
       testtype	       text,
       casenum         int,
       comments        text,
       sameparameters  text);
 
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
       rownum          int);

CREATE TABLE benchmark.testitemresult(
       itemname               text, 
       runid                 int,
       iteration             int,
       resultkind            text, 
       resultlocation        text,
       elapsedtime           bigint,
       issuccessful          bool,
       result_info           text,
       command               text,
       isverification        bool,
       starttimestamp        timestamp default current_timestamp,
       evaluation_function   float default null);

CREATE TABLE benchmark.testitemresultbaseline(
       itemname            text,
       algorithmic         text,
       method              text,        
       elapsedtime         bigint,
       evaluation_function float,
       issuccessful        bool,
       result_info         text);

CREATE TABLE benchmark.testinfo(
       runid               text,
       cases_count         int,
       platform            text,
       madlib_version      text,
       starttime           timestamp default current_timestamp);




CREATE OR REPLACE VIEW benchmark.detailtestreport as
(SELECT runid,
 CASE WHEN trb.itemname IS NOT NULL
      THEN trb.itemname
      ELSE tr.itemname
 END AS itemname,
 CASE WHEN trb.itemname IS NULL
      THEN 'NEW CASES'

      WHEN tr.itemname IS NULL
      THEN 'FAILED : NO RESULT OR REMOVE CASES'

      WHEN  ((trb.issuccessful = tr.issuccessful AND (trb.itemname not like 'dt%' or trb.itemname like 'dt_ctas_normalsize_%' or trb.itemname like '%_c45_display%')) OR
            (tr.resultlocation like 'PG%' AND tr.itemname like '%_ctas_%' AND tr.issuccessful = true))
      THEN  'PASSED'

      WHEN  trb.issuccessful <> tr.issuccessful
      THEN 'FAILED'

      WHEN  trb.evaluation_function IS NOT NULL AND (trb.evaluation_function - tr.evaluation_function) > 0.0001
      THEN 'FAILED : Evaluation function such as score, gof decreased'

      WHEN  trb.evaluation_function IS NULL AND trb.itemname like 'dt%' and trb.itemname like '%negative%' and substr(trb.result_info,1,80) = substr(tr.result_info,1,80)
      THEN 'PASSED'

      WHEN  trb.evaluation_function IS NULL AND trb.result_info <> tr.result_info
      THEN 'FAILED : Expected result not matched'

      WHEN ((trb.evaluation_function IS NOT NULL AND ((trb.evaluation_function - tr.evaluation_function) between -0.0001 and 0.0001))
          OR (trb.evaluation_function IS NULL AND trb.result_info = tr.result_info))
      THEN 'PASSED'

      WHEN  trb.evaluation_function IS NOT NULL AND (trb.evaluation_function - tr.evaluation_function) < 0.0001
      THEN 'PASSED : Evaluation function such as score, gof increased'

      WHEN  trb.evaluation_function IS NOT NULL AND tr.evaluation_function IS NULL
      THEN 'FAILED'

      ELSE 'CASES NEED TO BE INVESTIGATE'
 END AS testresult,
 CASE WHEN trb.itemname IS NOT NULL AND trb.issuccessful = tr.issuccessful AND  ((tr.elapsedtime::float8 / trb.elapsedtime::float8) between 0.8 and 1.2)
      THEN 'PERFORMANCE: No Regression'
      WHEN trb.itemname IS NOT NULL AND trb.issuccessful = tr.issuccessful AND  ((tr.elapsedtime::float8 / trb.elapsedtime::float8)  <  0.8 )
      THEN 'PERFORMANCE: BETTER'
      WHEN trb.itemname IS NOT NULL AND trb.issuccessful = tr.issuccessful AND  ((tr.elapsedtime::float8 / trb.elapsedtime::float8) > 1.2)
      THEN 'PERFORMANCE: WORSE'
      ELSE NULL
  END AS perf_status,
      
 tr.resultlocation as platform
 FROM benchmark.testitemresultbaseline as trb
  RIGHT JOIN  benchmark.testitemresult as tr
     on trb.itemname = tr.itemname
 ORDER BY runid);


CREATE OR REPLACE VIEW benchmark.testreport as
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

CREATE TABLE benchmark.evaluation_decision_tree(
      training_table text,
      training_size int,
      score_table text,
      score_size int,
      source  text,  --- R, Weka, Mahout
      missing_value   boolean,
      featurenum int,
      cotinuousnum int,
      split_critetion text,
      score float) DISTRIBUTED randomly;

CREATE TABLE benchmark.evaluation_kmeans(
      test_table text,
      test_size int,
      source  text,  --- R, Weka, Mahout
      k_value int,
      gof float) DISTRIBUTED randomly;

CREATE TABLE benchmark.evaluation_linear_regression (
	source text, 
	datasets text, 
	coef double precision[]) DISTRIBUTED randomly;

CREATE TABLE benchmark.evaluation_logistic_regression (
	source text, 
	datasets text, 
	coef double precision[]) DISTRIBUTED randomly;

