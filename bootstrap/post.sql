UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT avg_score
 FROM benchmark.decision_tree_c45_cross_validate AS dt
 WHERE dt.runid = ts.runid AND dt.testitemname=ts.itemname)
WHERE itemname like 'dt%' and itemname like '%cross_validate%';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT avg_score
 FROM benchmark.random_forest_rf_cross_validate AS rf
 WHERE rf.runid = ts.runid AND rf.testitemname=ts.itemname)
WHERE itemname like 'rf%' and itemname like '%cross_validate%';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT score
 FROM benchmark.random_forest_rf_score AS rf
 WHERE rf.runid = ts.runid AND rf.testitemname=ts.itemname)
WHERE itemname like 'rf%' and itemname not like '%cross_validate%';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT score
 FROM benchmark.decision_tree_c45_score AS dts
 WHERE dts.runid = ts.runid AND dts.testitemname=ts.itemname)
WHERE itemname like 'dt%' and itemname not like '%cross_validate%';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT silhouette
 FROM benchmark.kmeans_kmeans_cset AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'kmeans%';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT silhouette
 FROM benchmark.kmeans_kmeans_plusplus AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'kmeans%';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT silhouette
 FROM benchmark.kmeans_kmeans_random AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'kmeans%';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT silhouette
 FROM benchmark.kmeans_kmeans_canopy AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'kmeans%';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT silhouette
 FROM benchmark.kmeans_kmeans_new_cset_ctas AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'kmeans_new_cset%';

-- update benchmark.testitemresult
-- set evaluation_function=substring(substring(result_info from 'simple_silhouette .*') from E'[0-1]+.[0-9]+$')::float where itemname~'kmeans_new_cset.*';
