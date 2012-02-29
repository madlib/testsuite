1. -------- Verify DT result correct
-- DT00: It is to verify score when score table is the same as training table dt_scoreeqtrain%
-- DT02: It is to verify DT result correctness on normal size data sets  dt_largesize%%
-- DT02: It is to verify DT result correctness on large size data sets dt_normalsize%
-- DT03: It is to verify DT result correctness on sparse distribution data sets dt_sparse%
-- DT04: It is to verify DT result correctness on continuous features  dt_continuous%
-- DT04: It is to verify DT result correctness on discrete features dt_discrete%

SELECT  rscore.training_table, rscore.score_table,madlibscore,
        split_criterion_name,rscore,
       (madlibscore - rscore) AS difference
FROM (SELECT training_table_name, 
        scoring_table_name, 
        score as madlibscore, 
        split_criterion_name
    FROM  benchmark.decision_tree_c45_train as train,
		(SELECT suitename, itemname, casename,rownum
		from benchmark.testitems
		where  suitename like 'dt_scoreeqtrain%' or
		 suitename like 'dt_largesize%' or
		 suitename like 'dt_normalsize%' or
		 suitename like 'dt_sparse%' or
		 suitename like 'dt_continuous%' or
		 suitename like 'dt_discrete%' or
		 suitename like 'dt_ctas_normalsize%') as subtest1,
		benchmark.decision_tree_c45_score as score,
		(SELECT itemname, casename
		from benchmark.testitems
		where   suitename like 'dt_scoreeqtrain%' or
		 suitename like 'dt_largesize%' or
		 suitename like 'dt_normalsize%' or
		 suitename like 'dt_sparse%' or
		 suitename like 'dt_continuous%' or
		 suitename like 'dt_discrete%' or
		 suitename like 'dt_ctas_normalsize%' ) as subtest2 
    WHERE subtest1.itemname = train.testitemname
		AND subtest2.itemname = score.testitemname
		AND subtest1.casename = subtest2.casename
		AND score.runid = 1 and train.runid = 1
		) AS madlibresult
RIGHT JOIN
(
	SELECT training_table, score_table, max(score) as rscore
	FROM benchmark.evaluation_decision_tree as rscore
	GROUP BY training_table, score_table
) as rscore
ON rscore. training_table = madlibresult.training_table_name and rscore.score_table = madlibresult.scoring_table_name
ORDER BY training_table_name, score_table, split_criterion_name;


2. Special Data sets
-- DT05: Test DT result correctness on special data sets such as empty table, all rows are same class, duplicate rows, conflict rows  dt_specialdataset%
SELECT  training_table_name, scoring_table_name, score, split_criterion_name,rownum
from 
benchmark.decision_tree_c45_train as train,
(select suitename, itemname, casename,rownum
from benchmark.testitems
where suitename like 'dt_specialdataset%') as subtest1,
benchmark.decision_tree_c45_score as score,
(select itemname, casename
from benchmark.testitems
 where suitename like 'dt_specialdataset%') as subtest2
where subtest1.itemname = train.testitemname
and subtest2.itemname = score.testitemname
and subtest1.casename = subtest2.casename
and score.runid = 1 and train.runid = 1
order by training_table_name, scoring_table_name,split_criterion_name;


--- 3.---- DT07: It is to verify the training trees are the same when the "validation table name" is NULL or "training table" respectively
select testitemname, training_table_name, tree_nodes, tree_depth 
FROM benchmark.decision_tree_c45_train as train
where training_table_name 
      in ('madlibtestdata.dt_connect','madlibtestdata.dt_krkopt','madlibtestdata.dt_labor_neg','madlibtestdata.dt_car') 
and (testitemname like 'dt_validateeqtrain_%' or
     testitemname like 'dt_normalsize%'or 
    testitemname like 'dt_scoreeqtrain_%'  ) 
and runid = 1
order  by training_table_name, testitemname like '%gini%', testitemname like '%infogain%';

-- 4 -----DT08: It is to verify the training trees should be expected result when validation table is test data
SELECT  training_table_name, validation_table_name, score1.scoring_table_name as score_train_name,score1.score  as score_train, 
        score2.scoring_table_name as score_train_name,score2.score  as score_train, 
        split_criterion_name
from (select *
      from benchmark.decision_tree_c45_train
      where testitemname like 'dt_validateeqtest%') as train, 
  	(select *
	from benchmark.decision_tree_c45_score as score
	where testitemname like '%c45_score_1%'
	and testitemname like '%dt_validateeqtest%') as score1, 
	 (select *
	from benchmark.decision_tree_c45_score as score
	where testitemname like '%c45_score_2%'
	and testitemname like '%dt_validateeqtest%') as score2,
	(select suitename, itemname, casename,rownum
	from benchmark.testitems
	where suitename like 'dt_validateeqtest%') as subtest1,
	(select itemname, casename
	from benchmark.testitems
	where suitename like 'dt_validateeqtest%') as subtest2,
    (select itemname, casename
	from benchmark.testitems
	where suitename like 'dt_validateeqtest%') as subtest3
where subtest1.itemname = train.testitemname
	and subtest2.itemname = score1.testitemname
	and subtest3.itemname = score2.testitemname
	and subtest1.casename = subtest2.casename
	and subtest1.casename = subtest3.casename
	and score1.runid = 1 and score2.runid = 1 and train.runid = 1
order by training_table_name,split_criterion_name, validation_table_name;


-- 3. DT10: It is to Verify "prune_confidence_level" can affect tree format correctly :dt_pruneconf_infogain_16
select training_table_name, tree_nodes,tree_depth,split_criterion_name,confidence_level 
from benchmark.decision_tree_c45_train 
where testitemname like 'dt_pruneconf_%' 
order by testitemname;

-- 4.DT11: It is to Verify "max_iterations" can affect tree format correctly
select training_table_name, tree_nodes,tree_depth,split_criterion_name,max_iterations
from benchmark.decision_tree_c45_train 
where testitemname like 'dt_maxiter_%'
order by testitemname;

-- 5. DT12: It is to Verify "max_tree_depth" can affect tree format correctly
select training_table_name, tree_nodes,tree_depth,split_criterion_name,max_tree_depth
from benchmark.decision_tree_c45_train 
where testitemname like 'dt_maxdepth__%'
order by testitemname;


-- 6  DT13: It is to Verify "min_percent_mode" can affect tree format correctly
select training_table_name, tree_nodes,tree_depth,split_criterion_name,min_percent_mode
from benchmark.decision_tree_c45_train 
where testitemname like 'dt_minmode__%'
order by testitemname;






-- 7 DT14: It is to Verify "min_percent_split" can affect tree format correctly
select training_table_name, tree_nodes,tree_depth,split_criterion_name,min_percent_split
from benchmark.decision_tree_c45_train 
where testitemname like 'dt_minsplit__%'
order by testitemname;




-- DT018:It is to ensure datatype as discrete feature 
select *
from benchmark.decision_tree_c45_train
where testitemname like 'dt_datatype_%'
order by testitemname;



