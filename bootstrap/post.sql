SET SEARCH_PATH = benchmark;


UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT mlogr_precision_socre
 FROM benchmark.multinomial_logistic_regression_mlogr_precision_score AS scoretbl
 WHERE scoretbl.runid = ts.runid AND scoretbl.testitemname=ts.itemname)
WHERE itemname like 'multinomial_logistic_regression%_baseline%';


UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT score
 FROM benchmark.svm_classification_svm_cls_predict_batch_score AS scoretbl
 WHERE scoretbl.runid = ts.runid AND scoretbl.testitemname=ts.itemname)
WHERE itemname like 'svm_cls%_predict_batch_score%';


UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT score
 FROM benchmark.svm_classification_svm_cls_predict_score AS scoretbl
 WHERE scoretbl.runid = ts.runid AND scoretbl.testitemname=ts.itemname)
WHERE itemname like 'svm_cls%predict_score%';


UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT score
 FROM benchmark.svm_novelty_detection_svm_nd_predict_batch_score AS scoretbl
 WHERE scoretbl.runid = ts.runid AND scoretbl.testitemname=ts.itemname)
WHERE itemname like 'svm_no%predict_batch_score%';


UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT score
 FROM benchmark.svm_novelty_detection_svm_nd_predict_score AS scoretbl
 WHERE scoretbl.runid = ts.runid AND scoretbl.testitemname=ts.itemname)
WHERE itemname like 'svm_no%predict_score%';


UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT score
 FROM benchmark.svm_regression_svm_reg_predict_batch_score AS scoretbl
 WHERE scoretbl.runid = ts.runid AND scoretbl.testitemname=ts.itemname)
WHERE itemname like 'svm_reg%predict_batch_score%' ;


UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT score
 FROM benchmark.svm_regression_svm_reg_predict_score AS scoretbl
 WHERE scoretbl.runid = ts.runid AND scoretbl.testitemname=ts.itemname)
WHERE itemname like 'svm_reg%predict_score%';



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
(SELECT num_centroids
 FROM benchmark.kmeans_rewrite_km_random_seeding_default AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT num_centroids
 FROM benchmark.kmeans_rewrite_km_random_seeding_default_initialcentroids AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT num_centroids
 FROM benchmark.kmeans_rewrite_km_pp_seeding_default AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT num_centroids
 FROM benchmark.kmeans_rewrite_km_pp_seeding_default_fndist AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT num_centroids
 FROM benchmark.kmeans_rewrite_km_pp_seeding_default_fndist_initialcentroids AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_getsilhouette AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_default AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_default_fndist AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_default_fndist_aggcentroid AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_default_fndist_aggcentroid_maxiter AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_default_fndist_aggcentroid_maxiter_minfrac AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_default_fndist_aggcentroid_maxiter_minfrac_ce AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_pp_default AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_pp_default_fndist AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_pp_default_fndist_aggcentroid AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_pp_default_fndist_aggcentroid_maxiter AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_pp_default_fndist_aggcentroid_maxiter_minfrac AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_random_default AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_random_default_fndist AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_random_default_fndist_aggcentroid AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_random_default_fndist_aggcentroid_maxiter AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

UPDATE benchmark.testitemresult AS ts
SET evaluation_function =
(SELECT simple_silhouette
 FROM benchmark.kmeans_rewrite_km_random_default_fndist_aggcentroid_maxiter_min AS kmeans
 WHERE kmeans.runid = ts.runid AND kmeans.testitemname=ts.itemname)
WHERE itemname like 'km_';

