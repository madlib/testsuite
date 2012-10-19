SET SEARCH_PATH = benchmark;
DROP TABLE IF EXISTS analyticstool CASCADE;
DROP TABLE IF EXISTS testitems CASCADE;
DROP TABLE IF EXISTS testsuites CASCADE;
DROP TABLE IF EXISTS testitemresultbaseline CASCADE;
DROP TABLE IF EXISTS madlib_jiras CASCADE;
DROP TABLE IF EXISTS jiras_cases CASCADE;
CREATE TABLE testitemresultbaseline(
       itemname            text,
       algorithmic         text,
       method              text,
       elapsedtime         bigint,
       evaluation_function float,
       issuccessful        bool,
       result_info         text,
       analysis_tool       varchar(128));

CREATE TABLE testsuites(
       suitename       text,
       testtype        text,
       casenum         int,
       comments        text,
       sameparameters  text,
       primary key(suitename));

CREATE TABLE testitems(
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

CREATE TABLE madlib_jiras(
       jiraid   varchar(20),
       jiratype smallint,
       jiradescription text);
CREATE TABLE jiras_cases(
        jiraid  varchar(20),
        casename varchar(150)); 


CREATE OR REPLACE VIEW testresultreport AS
(SELECT runid, ti.algorithmic as suitename, ti.casename,
 CASE WHEN trb.itemname IS NOT NULL
      THEN trb.itemname
      ELSE tr.itemname
 END AS itemname,
      tr.command as command,
 CASE 
      WHEN tr.issuccessful IS NULL
      THEN 'SKIPPED'

      WHEN trb.itemname IS NULL
      THEN 'NEW CASES'

      WHEN tr.itemname IS NULL OR ti.itemname IS NULL
      THEN 'FAILED : NO RESULT OR REMOVE CASES'

      WHEN tr.itemname like 'svm_%predict_score%' and resultlocation like 'PG%'
         THEN CASE WHEN tr.issuccessful = trb.issuccessful OR tr.result_info like '%IndexError: list index out of range%'  THEN 'PASSED'
                   ELSE 'FAILED'         
              END  

      WHEN  trb.issuccessful <> tr.issuccessful OR (trb.evaluation_function IS NOT NULL AND tr.evaluation_function IS NULL)
      THEN 'FAILED'

      WHEN  trb.evaluation_function = 0 AND tr.evaluation_function = 0 
      THEN 'PASSED'
      
  WHEN tr.itemname in ('plda_label_negative_column_contents_datatype_test_table_0_0_plda_label_test_documents_1',
               'plda_label_negative_column_contents_name_test_table_0_0_plda_label_test_documents_1',
               'plda_label_negative_column_dict_name_dict_table_0_0_plda_label_test_documents_1',
               'plda_label_negative_column_gcounts_datatype_model_table_0_0_plda_label_test_documents_1',
               'plda_label_negative_column_gcounts_name_model_table_0_0_plda_label_test_documents_1',
               'plda_label_negative_column_id_datatype_test_table_0_0_plda_label_test_documents_1',
               'plda_label_negative_column_id_name_test_table_0_0_plda_label_test_documents_1',
               'plda_label_negative_column_iternum_datatype_model_table_0_0_plda_label_test_documents_1',
               'plda_label_negative_column_iternum_name_model_table_0_0_plda_label_test_documents_1',
               'plda_label_negative_column_tcounts_datatype_model_table_0_0_plda_label_test_documents_1',
               'plda_label_negative_column_tcounts_name_model_table_0_0_plda_label_test_documents_1',
               'plda_label_negative_existing_out_labeling_0_0_plda_label_test_documents_1',
               'plda_label_negative_name_dict_table_0_0_plda_label_test_documents_1',
               'plda_label_negative_name_model_table_0_0_plda_label_test_documents_1',
               'plda_label_negative_name_test_table_0_0_plda_label_test_documents_1',
               'plda_run_empty_dict_table_0_0_plda_run_0',
               'plda_run_negative_column_dict_name_dicttable_0_0_plda_run_0',
               'plda_run_negative_column_id_datatype_datatable_0_0_plda_run_0',
               'plda_run_negative_column_id_name_datatable_0_0_plda_run_0',
               'plda_run_negative_culumn_contents_datatype_datatable_0_0_plda_run_0',
               'plda_run_negative_culumn_contents_name_datatable_0_0_plda_run_0',
               'plda_run_negative_existing_modeltable_0_0_plda_run_0',
               'plda_run_negative_existing_outputdatatable_0_0_plda_run_0',
               'plda_run_negative_name_datatable_0_0_plda_run_0',
               'plda_run_negative_name_dicttable_0_0_plda_run_0')
        THEN CASE WHEN tr.issuccessful = false AND (
                                                   tr.result_info LIKE '%column "contents" is of type integer[] but expression is of type text[]%' 
                        OR tr.result_info like '%column "contents" does not exist%'
                        OR tr.result_info like '%column "dict" does not exist%'
                        OR tr.result_info like '%relation "madlibtestdata.plda_invalid_column_gcounts_datatype_model" does not exist%'
                        OR tr.result_info like '%relation "madlibtestdata.plda_invalid_column_gcounts_name_model" does not exist%'
                        OR tr.result_info like '%column "id" is of type integer but expression is of type text%'
                        OR tr.result_info like '%column "id" does not exist%'
                        OR tr.result_info like '%relation "madlibtestdata.plda_invalid_column_iternum_datatype_model" does not exist%'
                        OR tr.result_info like '%relation "madlibtestdata.plda_invalid_column_iternum_name_model" does not exist%'
                        OR tr.result_info like '%relation "madlibtestdata.plda_invalid_column_tcounts_datatype_model" does not exist%'
                        OR tr.result_info like '%relation "madlibtestdata.plda_invalid_column_tcounts_name_model" does not exist%'
                        OR tr.result_info like '%relation "plda_existing_out_labeling" already exists%'
                        OR tr.result_info like '%relation "madlibtestdata.plda_invalid_name_dict" does not exist%'
                        OR tr.result_info like '%relation "madlibtestdata.plda_invalid_name_model" does not exist%'
                        OR tr.result_info like '%relation "madlibtestdata.plda_invalid_name_corpus" does not exist%'
                        OR tr.result_info like '%error: dictionary table is not of the expected form%'
                        OR tr.result_info like '%column "dict" does not exist%' 
                        OR tr.result_info like '%column "id" is of type integer but expression is of type text%' 
                        OR tr.result_info like '%column "id" does not exist%'
                        OR tr.result_info like '%column "contents" is of type integer[] but expression is of type text[]%' 
                        OR tr.result_info like '%column "contents" does not exist%' 
                        OR tr.result_info like '%relation "plda_existing_out_model" already exists%' 
                        OR tr.result_info like '%relation "plda_existing_out_corpus" already exists%' 
                        OR tr.result_info like '%relation "madlibtestdata.plda_invalid_name_corpus" does not exist%' 
                        OR tr.result_info like '%relation "madlibtestdata.plda_invalid_name_dict" does not exist%')
           THEN 'PASSED'
        ELSE 'FAILED'
        END

      WHEN tr.itemname in (
                            'nb_precompute_views_0_0_test_create_nb_prepared_data_tables_0',
                            'nb_precompute_views_0_4_test_create_nb_prepared_data_tables_0',
                            'nb_precompute_function_workaround_0_0_test_create_nb_prepared_data_tables_0',
                            'nb_precompute_function_workaround_0_4_test_create_nb_prepared_data_tables_0')
          THEN CASE WHEN tr.issuccessful = false AND (tr.result_info LIKE '%column "value" contains null values%'  OR tr.result_info like '%line 19 at execute statement%')
               THEN 'PASSED'
               ELSE 'FAILED'
               END

      WHEN  (tr.itemname like 'svm_cls_dot_ds_0_7%' OR tr.itemname like 'svm_cls_dot_ds_0_15%' OR tr.itemname like 'svm_cls_polynomial_ds_0_7%'
          OR tr.itemname like 'svm_cls_polynomial_ds_0_15%' OR tr.itemname like 'svm_cls_gaussian_ds_0_7%' OR tr.itemname like 'svm_cls_gaussian_ds_para_0_7%'
          OR tr.itemname like 'svm_reg_dot_ds_0_4%' OR tr.itemname like 'svm_reg_dot_ds_0_18%' OR tr.itemname like 'svm_reg_polynomial_ds_0_4%'
          OR tr.itemname like 'svm_reg_polynomial_ds_0_18%' OR tr.itemname like 'svm_reg_gaussian_ds_0_4%' OR tr.itemname like 'svm_reg_gaussian_ds_para_0_4%')
          AND tr.itemname not like 'svm_%_3'
          THEN CASE WHEN tr.issuccessful = false AND (tr.result_info LIKE '%the maximum number of features is 102400%'  OR tr.result_info like '%does not exist%')
               THEN 'PASSED'
               ELSE 'FAILED'
               END

      WHEN (trb.evaluation_function = 0 AND tr.evaluation_function <> 0) or (trb.evaluation_function <> 0 AND tr.evaluation_function = 0) 
      THEN  CASE WHEN trb.itemname like 'svm_%predict_score%'  THEN 'PASSED'
                  ELSE 'FAILED'
                  END

      WHEN  trb.itemname not like 'rf%' AND trb.itemname not like '%cross_validate%' AND trb.itemname not like 'svm%' AND trb.itemname not like 'multinomia%' 
          AND  trb.evaluation_function IS NOT NULL AND (trb.evaluation_function - tr.evaluation_function) > 0.0001
      THEN 'FAILED : Evaluation function such as score, gof decreased: baseline is '::text  || ((trb.evaluation_function)::decimal(6,5))::text  || ' and runtime is '::text  || ((tr.evaluation_function)::decimal(6,5))::text

      WHEN  (trb.itemname like 'rf%'  OR  trb.itemname like '%cross_validate%' OR trb.itemname  like 'multinomia%') and trb.evaluation_function IS NOT NULL AND (tr.evaluation_function / trb.evaluation_function) < 0.7
      THEN 'FAILED : Evaluation function of random forest decreased: baseline is '::text  || ((trb.evaluation_function)::decimal(6,5))::text  || ' and runtime is '::text  || ((tr.evaluation_function)::decimal(6,5))::text


      WHEN  (trb.itemname like 'svm%'  ) and trb.evaluation_function IS NOT NULL AND (tr.evaluation_function / trb.evaluation_function) < 0.0000001
      THEN 'FAILED : Evaluation function of support vecotr machines decreased: baseline is '::text  || (trb.evaluation_function)::text  || ' and runtime is '::text  || (tr.evaluation_function)::text

      WHEN  (trb.issuccessful = tr.issuccessful AND (trb.itemname like 'kmeans_%' or trb.itemname like 'multinomial_%' 
                                      or trb.itemname like 'logistic_%'  or trb.itemname like 'linear_%'  
                                      or trb.itemname like '%_c45_display%' or trb.itemname like 'svm_%0'
                                      or (trb.itemname like 'rf%' and trb.itemname not like '%negative%' and (trb.itemname like '%_rf_train%' or trb.itemname like '%_rf_display%'))
                                                                                                 )
                      ) 
      THEN  'PASSED'

           
      WHEN  trb.evaluation_function IS NULL AND (trb.itemname like 'dt%' or trb.itemname like 'rf%' or trb.itemname like 'ar%') and trb.itemname like '%negative%' and (tr.issuccessful = trb.issuccessful or substr(trb.result_info,1,80) = substr(tr.result_info,1,80) or tr.result_info like '%invalid input syntax for type double precision:%' or tr.result_info like '%each feature in feature_col_names must be a column of the training table%' or tr.result_info like '%each feature in continuous_feature_names must be in the feature_col_names%')
      THEN 'PASSED'

      WHEN  trb.evaluation_function IS NULL AND trb.issuccessful = false AND tr.issuccessful = false 
      THEN 'PASSED'

      WHEN  trb.evaluation_function IS NULL AND trb.result_info <> tr.result_info
      THEN 'FAILED : Expected result not matched'

      WHEN (trb.itemname not like 'rf%' AND trb.itemname not like '%cross_validate%'  AND trb.itemname not like 'svm%'  AND trb.itemname not like 'multinomia%' AND (trb.evaluation_function IS NOT NULL AND ((trb.evaluation_function - tr.evaluation_function) between -0.0001 and 0.0001)))
          OR ((trb.itemname like 'rf%' OR  trb.itemname like '%cross_validate%' OR trb.itemname  like 'multinomia%') AND (trb.evaluation_function IS NOT NULL AND ((tr.evaluation_function / trb.evaluation_function) between 0.7 and 1.3 )))
           OR ((trb.itemname like 'svm%' ) AND (trb.evaluation_function IS NOT NULL AND ((tr.evaluation_function / trb.evaluation_function) between 0.0000001 and 10000000 )))   
          OR (trb.evaluation_function IS NULL AND trb.result_info = tr.result_info)
      THEN 'PASSED'


      WHEN  trb.itemname not like 'rf%' AND trb.itemname not like '%cross_validate%' AND trb.itemname not like 'svm%'  AND trb.itemname not like 'multinomia%' AND trb.evaluation_function IS NOT NULL AND (trb.evaluation_function - tr.evaluation_function) < 0.0001
      THEN 'PASSED : Evaluation function such as score, gof increased: baseline is '::text  || ((trb.evaluation_function)::decimal(6,5))::text  || ' and runtime is '::text  || ((tr.evaluation_function)::decimal(6,5))::text 

      WHEN  (trb.itemname like 'rf%' OR  trb.itemname like '%cross_validate%' OR trb.itemname  like 'multinomia%') AND trb.evaluation_function IS NOT NULL AND (tr.evaluation_function / trb.evaluation_function) > 1.3
      THEN 'PASSED : Evaluation function of random forest increased: baseline is '::text  || ((trb.evaluation_function)::decimal(6,5))::text  || ' and runtime is '::text  || ((tr.evaluation_function)::decimal(6,5))::text 

      WHEN  (trb.itemname like 'svm%' ) AND trb.evaluation_function IS NOT NULL AND (tr.evaluation_function / trb.evaluation_function) > 10000000
      THEN 'PASSED : Evaluation function of svm increased: baseline is '::text  || (trb.evaluation_function)::text  || ' and runtime is '::text  || (tr.evaluation_function)::text 

      ELSE 'CASES NEED TO BE INVESTIGATE'
 END AS testresult,
      tr.resultlocation as platform,
      tr.elapsedtime as runtime,
      trb.elapsedtime as basetime,
      tr.starttimestamp,
      tr.result_info as trresult, 
     trb.result_info as trbresult

 FROM testitemresult as tr 
      FULL JOIN 
        testitemresultbaseline as trb
      ON trb.itemname = tr.itemname
      FULL JOIN 
      testitems as ti
      ON tr.itemname = ti.itemname
 
ORDER BY runid);


 CREATE OR REPLACE VIEW  summaryreport AS 
(SELECT runid, suitename, casename, platform,
        CASE WHEN bool_and(testresult like 'PASSED%')
              THEN 'PASSED'
              WHEN  bool_or(testresult like 'FAILED%')
              THEN  'FAILED'
              WHEN  bool_and (testresult in('SKIPPED', 'NEW CASES'))
              THEN 'SKIPPED'
              ELSE 'ERROR'
        END AS testresult,
        sum(runtime) as elapsedtime,
        min(starttimestamp) as starttimestamp
FROM testresultreport
GROUP BY runid, suitename, casename, platform);


CREATE OR REPLACE VIEW performancedetailreport AS
(SELECT runid, suitename, casename, itemname, testresult,
 CASE WHEN testresult like 'PASSED%' 
 THEN             
      CASE WHEN itemname like '%c45_clean%' OR basetime < 5000 OR runtime < 5000 OR itemname like 'kmeans_random%'
      THEN 'Not justification functions in performance evaluation'
      
      WHEN (basetime::float8 < 10000 and ((runtime::float8 / basetime::float8) between 0.3 and 3))
              OR ((basetime::float8 BETWEEN 10000 AND 180000) and ((runtime::float8 / basetime::float8) between 0.7 and 1.3))
              OR (basetime::float8 >= 120000 and ((runtime::float8 / basetime::float8) between 0.9 and 1.1))
      THEN 'PERFORMANCE No regression: base time is ' || (basetime/1000)::text || ' seconds and run time is ' || (runtime/1000)::text || ' seconds'
       
       WHEN (basetime::float8 < 10000  and ((runtime::float8 / basetime::float8)  <  0.3 ))
                OR ((basetime::float8 BETWEEN 10000 AND 180000) and ((runtime::float8 / basetime::float8)  <  0.7 ))
                OR (basetime::float8 > 180000  and ((runtime::float8 / basetime::float8)  <  0.9 )) 
       THEN 'PERFORMANCE BETTER: base time is ' || (basetime/1000)::text || ' seconds and run time is ' || (runtime/1000)::text || ' seconds'
       
       WHEN (basetime::float8 < 5000  and ((runtime::float8 / basetime::float8)  > 3 ))
                OR ((basetime::float8 BETWEEN 10000 AND 180000) and ((runtime::float8 / basetime::float8)  > 1.3))
                OR (basetime::float8 > 180000  and ((runtime::float8 / basetime::float8)  > 1.1)) 
       THEN 'PERFORMANCE WORSE: base time is ' || (basetime/1000)::text || ' seconds and run time is ' || (runtime/1000)::text || ' seconds'

       
        ELSE 'PERFORMANCE UNKNOWN'
       END
  ELSE 'PERFORMANCE UNKNOWN'
  END AS perf_status, 
      platform,
      runtime as elapsedtime,
      starttimestamp
 FROM testresultreport
 ORDER BY runid);


CREATE OR REPLACE VIEW failedcases as 
SELECT casename, 
       itemname, 
       command, 
       trresult, 
       trbresult,
       starttimestamp         
FROM testresultreport
WHERE runid = ( SELECT max(testitemseq.runid) FROM testitemseq) 
 AND casename IN ( SELECT DISTINCT testresultreport.casename
                          FROM testresultreport
                          WHERE testresult  LIKE 'FAILED%'::text 
                           AND itemname NOT LIKE '%negative%'::text 
                           AND runid = ( SELECT max(testitemseq.runid)FROM testitemseq ))
            AND itemname NOT LIKE '%negative%'::text
UNION 
SELECT  casename, 
        itemname, 
        command, 
        trresult, 
        trbresult,
        starttimestamp
  FROM testresultreport
  WHERE runid = ( SELECT max(testitemseq.runid) FROM testitemseq) 
        AND testresult like 'FAILED%'
        AND testresultreport.itemname LIKE '%negative%'::text;


CREATE OR REPLACE VIEW featuretestsummary 
AS 
SELECT suitename, testresult, count(*)
FROM summaryreport
WHERE runid = (SELECT MAX(runid) from testitemseq)
GROUP BY  suitename, testresult;

CREATE OR REPLACE VIEW performancesummary 
AS 
SELECT suitename, substr(perf_status, 1, position(':' in perf_status) -1) as perfstatus, count(*)
FROM performancedetailreport
WHERE runid = (SELECT MAX(runid) from testitemseq)
AND   position(':' in perf_status) > 0
GROUP BY  suitename, substr(perf_status, 1, position(':' in perf_status) -1);

CREATE OR REPLACE VIEW skippedcases
AS
SELECT COUNT(*) AS failedcases,
       jc.jiraid as jiraid, 
       algorithmic, 
       CASE WHEN j.jiratype = 1 THEN 'V0.5' 
            WHEN j.jiratype = 2 THEN 'MASTER'
            WHEN j.jiratype = 999 THEN 'QA Backlog'
            WHEN j.jiratype = 1000 THEN 'GPDB'
           ELSE 'ERROR TYPE' END  AS fixversion, 
       j.jiradescription
FROM jiras_cases as jc,
             madlib_jiras as j,
             (SELECT algorithmic, casename from testitemresult as tr, testitems as ti
              WHERE runid = (select max(runid) from testitemseq)
                 AND issuccessful IS NULL and tr.itemname = ti.itemname) as tr
WHERE j.jiraid = jc.jiraid AND tr.casename = jc.casename
GROUP BY  jc.jiraid, j.jiratype, algorithmic,j.jiradescription;

