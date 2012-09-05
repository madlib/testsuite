
DROP TABLE IF EXISTS benchmark.analyticstool CASCADE;
DROP TABLE IF EXISTS benchmark.testitems CASCADE;
DROP TABLE IF EXISTS benchmark.testsuites CASCADE;
DROP TABLE IF EXISTS benchmark.testitemresultbaseline CASCADE;

CREATE TABLE benchmark.testitemresultbaseline(
       itemname            text,
       algorithmic         text,
       method              text,
       elapsedtime         bigint,
       evaluation_function float,
       issuccessful        bool,
       result_info         text,
       analysis_tool       varchar(128));

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


CREATE OR REPLACE VIEW benchmark.testresultreport AS
(SELECT runid, ti.suitename, ti.casename,
 CASE WHEN trb.itemname IS NOT NULL
      THEN trb.itemname
      ELSE tr.itemname
 END AS itemname,
 CASE 
      WHEN tr.issuccessful IS NULL
      THEN 'SKIPPED'

      WHEN trb.itemname IS NULL
      THEN 'NEW CASES'

      WHEN tr.itemname IS NULL OR ti.itemname IS NULL
      THEN 'FAILED : NO RESULT OR REMOVE CASES'

      WHEN  trb.issuccessful <> tr.issuccessful OR (trb.evaluation_function IS NOT NULL AND tr.evaluation_function IS NULL)
      THEN 'FAILED'

      WHEN  trb.evaluation_function = 0 AND tr.evaluation_function = 0 
      THEN 'PASSED'
      WHEN  trb.evaluation_function = 0 AND tr.evaluation_function <> 0 
      THEN 'FAILED'

      WHEN  trb.itemname not like 'rf%' AND trb.itemname not like '%cross_validate%' and trb.evaluation_function IS NOT NULL AND (trb.evaluation_function - tr.evaluation_function) > 0.0001
      THEN 'FAILED : Evaluation function such as score, gof decreased: baseline is '::text  || ((trb.evaluation_function)::decimal(6,5))::text  || ' and runtime is '::text  || ((tr.evaluation_function)::decimal(6,5))::text

      WHEN  (trb.itemname like 'rf%'  OR  trb.itemname like '%cross_validate%' ) and trb.evaluation_function IS NOT NULL AND (tr.evaluation_function / trb.evaluation_function) < 0.7
      THEN 'FAILED : Evaluation function of random forest decreased: baseline is '::text  || ((trb.evaluation_function)::decimal(6,5))::text  || ' and runtime is '::text  || ((tr.evaluation_function)::decimal(6,5))::text

      WHEN  (trb.issuccessful = tr.issuccessful AND (trb.itemname like 'kmeans_%' 
                                      or trb.itemname like 'logistic_%'  or trb.itemname like 'linear_%'  
                                      or trb.itemname like '%_c45_display%' 
                                      or (trb.itemname like 'rf%' and trb.itemname not like '%negative%' and (trb.itemname like '%_rf_train%' or trb.itemname like '%_rf_display%'))
                                                                                                 )
                      ) 
      THEN  'PASSED'

           
      WHEN  trb.evaluation_function IS NULL AND (trb.itemname like 'dt%' or trb.itemname like 'rf%' ) and trb.itemname like '%negative%' and (tr.issuccessful = trb.issuccessful or substr(trb.result_info,1,80) = substr(tr.result_info,1,80) or tr.result_info like '%invalid input syntax for type double precision:%' or tr.result_info like '%each feature in feature_col_names must be a column of the training table%' or tr.result_info like '%each feature in continuous_feature_names must be in the feature_col_names%')
      THEN 'PASSED'

      WHEN  trb.evaluation_function IS NULL AND trb.result_info <> tr.result_info
      THEN 'FAILED : Expected result not matched'

      WHEN (trb.itemname not like 'rf%' AND trb.itemname not like '%cross_validate%'  AND (trb.evaluation_function IS NOT NULL AND ((trb.evaluation_function - tr.evaluation_function) between -0.0001 and 0.0001)))
          OR ((trb.itemname like 'rf%' OR  trb.itemname like '%cross_validate%' ) AND (trb.evaluation_function IS NOT NULL AND ((tr.evaluation_function / tr.evaluation_function) between 0.7 and 1.3 )))
          OR (trb.evaluation_function IS NULL AND trb.result_info = tr.result_info)
      THEN 'PASSED'


      WHEN  trb.itemname not like 'rf%' AND trb.itemname not like '%cross_validate%' AND trb.evaluation_function IS NOT NULL AND (trb.evaluation_function - tr.evaluation_function) < 0.0001
      THEN 'PASSED : Evaluation function such as score, gof increased: baseline is '::text  || ((trb.evaluation_function)::decimal(6,5))::text  || ' and runtime is '::text  || ((tr.evaluation_function)::decimal(6,5))::text 

      WHEN  (trb.itemname like 'rf%' OR  trb.itemname like '%cross_validate%' ) AND trb.evaluation_function IS NOT NULL AND (tr.evaluation_function / tr.evaluation_function) > 1.3
      THEN 'PASSED : Evaluation function of random forest increased: baseline is '::text  || ((trb.evaluation_function)::decimal(6,5))::text  || ' and runtime is '::text  || ((tr.evaluation_function)::decimal(6,5))::text 

      ELSE 'CASES NEED TO BE INVESTIGATE'
 END AS testresult,
      tr.resultlocation as platform,
      tr.elapsedtime as runtime,
      trb.elapsedtime as basetime,
      tr.starttimestamp,
      tr.result_info as trresult, 
     trb.result_info as trbresult

 FROM benchmark.testitemresultbaseline as trb
      FULL JOIN 
       benchmark.testitemresult as tr 
      ON trb.itemname = tr.itemname
      FULL JOIN 
      benchmark.testitems as ti
      ON trb.itemname = ti.itemname
 ORDER BY runid);


CREATE OR REPLACE VIEW benchmark.summaryreport AS
(SELECT runid, suitename, itemname, testresult,
 CASE WHEN testresult like 'PASSED%' 
 THEN             
      CASE WHEN itemname like '%c45_clean%' OR basetime < 1000 OR runtime < 1000 OR itemname like 'kmeans_random%'
      THEN 'PERFORMANCE: Not justification functions'
      
      WHEN (basetime::float8 < 5000 and ((runtime::float8 / basetime::float8) between 0.5 and 2))
              OR (basetime::float8 >= 5000 and ((runtime::float8 / basetime::float8) between 0.8 and 1.2)) 
       THEN 'PERFORMANCE: No Regression'
       
       WHEN (basetime::float8 < 5000  and ((runtime::float8 / basetime::float8)  <  0.5 ))
                OR(basetime::float8 > 5000  and ((runtime::float8 / basetime::float8)  <  0.8 )) 
       THEN 'PERFORMANCE: BETTER: base time is ' || (basetime/1000)::text || ' seconds and run time is ' || (runtime/1000)::text || ' seconds'
       
       WHEN (basetime::float8 < 5000  and ((runtime::float8 / basetime::float8)  > 2 ))
                OR(basetime::float8 > 5000  and ((runtime::float8 / basetime::float8)  > 1.2)) 
       THEN 'PERFORMANCE: WORSE: base time is ' || (basetime/1000)::text || ' seconds and run time is ' || (runtime/1000)::text || ' seconds'

       
        ELSE 'PERFORMANCE: UNKNOWN'
       END
  ELSE 'PERFORMANCE: UNKNOWN'
  END AS perf_status, 
      platform,
      runtime as elapsedtime,
      starttimestamp
 FROM benchmark.testresultreport
 ORDER BY runid);

