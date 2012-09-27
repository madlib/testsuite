DROP SCHEMA IF EXISTS benchmark CASCADE;
CREATE SCHEMA benchmark;
SET SEARCH_PATH = benchmark;
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

CREATE TABLE benchmark.testinfo(
       runid               text,
       cases_count         int,
       platform            text,
       madlib_version      text,
       starttime           timestamp default current_timestamp);




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





----kmeans:
INSERT INTO benchmark.evaluation_kmeans 
(test_table,test_size,source,k_value,gof) 
values ('madlibtestdata.km_abalone',4177,'R',6,0.4547123);
INSERT INTO benchmark.evaluation_kmeans 
(test_table,test_size,source,k_value,gof) 
values ('madlibtestdata.km_movement_libras',360,'R',6,0.1951174);
INSERT INTO benchmark.evaluation_kmeans 
(test_table,test_size,source,k_value,gof) 
values ('madlibtestdata.km_water_treatment',527,'R',6,0.442349);
INSERT INTO benchmark.evaluation_kmeans 
(test_table,test_size,source,k_value,gof) 
values ('madlibtestdata.km_wine',177,'R',6,0.5405609);
INSERT INTO benchmark.evaluation_kmeans 
(test_table,test_size,source,k_value,gof) 
values ('madlibtestdata.km_winequality_red',1599,'R',6,0.3916135);

----linear regression:
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_auto_mpg_oi', array[-0.705487,0.021874,-0.039557,-0.006032,-0.079616,0.583610,1.309954]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_auto_mpg_wi', array[-17.218435,-0.493376,0.019896,-0.016951,-0.006474,0.080576,0.750773,1.426140]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_communities_oi', array[-3.124503,0.973050,-0.025253,0.064145,-0.027793,0.169358,-0.017517,-0.494983,0.109149,0.376792,3.506583,-0.155418,-0.198623,-0.470151,-0.257177,-0.697171,-0.558924,0.269108,0.100415,0.693640,-0.796613,0.356591,-0.303316,0.061507,-0.136136,0.133744,-0.079940,-0.436892,0.576913,-0.099798,-0.697285,-0.393966,0.241185,0.994971,-0.270966,0.055242,0.713352,0.994890,0.310328,-0.055955,0.296219,-1.037503,-0.210928,-1.023207,-0.491196,0.263585,0.164164,-0.269843,0.100518,-0.119482,-0.030250,-0.224538,0.406379,-0.567223,-0.559568,0.912557,-0.328681,-0.346347,2.923250,-2.982492,0.393223,0.360529,0.763151,-1.263448,-0.663366,0.795514,-0.718224,-1.379557,0.629470,0.520894,-0.015313,0.132743,0.098042,1.941370,-0.026174,0.063683,0.073514,0.046059,-0.241893,0.033776,-0.706070,0.314195,-0.995046,0.260813,-0.135037,1.504289,-0.282187,-0.203637,-0.079025,0.001507,0.227624,0.796386,-0.196151,0.166611,0.017539,0.192309,-0.145739,-34.194131,0.104202,0.924177,-0.107974,0.220858,0.025374,33.450413,-0.154905,0.033564,0.160504,0.275097,0.107606,-0.309670,-0.290514,0.012957,-0.036414,0.118890,0.034466,-0.137377,0.104034,0.447416,-0.134423,0.007548,-0.040586,-0.200604]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_communities_wi', array[2.299346,-3.477014,1.118523,-0.042317,0.022103,-0.084169,0.151692,-0.239641,-0.870986,0.381891,0.199093,3.802921,-0.199798,-0.386196,-0.596946,-0.308686,-0.751876,-0.721007,0.238305,0.064647,0.871977,-0.876057,0.355919,-0.302640,0.016152,-0.147430,0.137916,-0.072551,-0.392185,0.525168,-0.110777,-0.696948,-0.376918,0.208605,0.906994,-0.260103,0.040638,0.683254,0.937680,0.421753,-0.132579,0.576898,-1.592595,-0.199961,-0.975888,-0.736243,0.291224,0.213487,-0.339775,0.186612,-0.068671,-0.095884,-0.208372,0.477527,-0.605230,-0.571753,0.906122,-0.414670,-0.195200,2.654012,-2.795003,0.261752,0.238634,0.758129,-1.104997,-0.458464,0.444974,-0.920028,-1.243047,0.654071,0.457273,-0.026487,0.118449,0.109620,1.630849,-0.009382,0.032321,0.005267,0.021123,-0.227375,0.201986,-0.982578,0.413837,-0.951067,0.205816,-0.120935,1.569722,-0.300825,-0.243984,-0.081724,-0.031032,0.240223,0.778883,-0.273786,0.123774,-0.020392,0.242067,-1.220194,-32.540494,-0.817199,0.796148,-0.170271,0.295684,0.006914,31.900611,-0.145067,-0.013067,0.127931,0.227881,0.093053,-0.301982,-0.313247,0.027384,-0.033136,0.129993,0.024892,-0.150993,0.134012,0.666780,-0.164493,0.009451,-0.041778,-0.285623]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_Concrete_oi', array[0.11335,0.09623,0.07932,-0.18224,0.26473,0.01029,0.01133,0.11400]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_Concrete_wi', array[-23.33121, 0.11980, 0.10387, 0.08793,-0.14992, 0.29222,0.01809, 0.02019, 0.11422]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_flare_oi', array[-0.0005741,-0.0086832,0.0088243,0.0025365,-0.0338446,0.0748605,0.0061311]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_flare_wi', array[-0.201490,0.003269,0.003603,0.009727,0.009361,0.001819,0.145274,0.018895]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_forestfires_oi', array[1.913487,0.522545,-0.101162,0.079401,-0.003287,-0.681862,0.777438,-0.242524,1.524466,-3.296335]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_forestfires_wi', array[-6.369315,1.907945,0.569181,-0.039200,0.077335,-0.003295,-0.713739,0.800213,-0.230645,1.557431,-3.404037]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_housing_oi', array[-0.092897,0.048715,-0.004060,2.853999,-2.868436,5.928148,-0.007269,-0.968514,0.171151,-0.009396,-0.392191,0.014906,-0.416304]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_housing_wi', array[3.646e+01,-1.080e-01,4.642e-02,2.056e-02,2.687e+00,-1.777e+01,3.810e+00,6.922e-04,-1.476e+00,3.060e-01,-1.233e-02,-9.527e-01,9.312e-03,-5.248e-01]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_machine_oi', array[-0.031209,0.013927,0.004849,0.412111,-0.676074,1.041648]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_machine_wi', array[-66.48138, 0.06596, 0.01431, 0.00659, 0.49447,-0.17234,1.20117]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_o_ring_erosion_only_oi', array[0.635708,-0.055069,0.003429,-0.016734]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_o_ring_erosion_only_wi', array[3.814247,'NaN'::float8,-0.055069,0.003429,-0.016734]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_o_ring_erosion_or_blowby_oi', array[0.587849,-0.051386,0.001757,0.014293]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_o_ring_erosion_or_blowby_wi', array[3.527093,'NaN'::float8,-0.051386,0.001757,0.014293]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_parkinsons_updrs_oi', array[7.081e-04,7.928e-03,1.675e-05,1.580e-03,-6.412e-04,1.821e+01,9.337e+02,5.677e+01,-2.174e-02,-2.737e+01,-2.067e+00,3.047e-01,9.369e+01,-1.365e+00,5.266e-01,-3.119e+01,-2.603e-01,-4.764e-03,7.874e-02,2.029e-01]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_parkinsons_updrs_wi', array[2.150e-01,3.447e-04,4.928e-03,1.237e-05,1.433e-03,-6.920e-04,1.884e+01,9.692e+02,7.881e+01,-2.414e-01,-3.489e+01,-1.671e+00,2.797e-01,1.334e+02,-1.407e+00,5.220e-01,-4.466e+01,-3.562e-01,-8.261e-03,1.114e-02,1.015e-01]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_servo_oi', array[0.5075,-0.3815]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_servo_wi', array[6.0134, -1.3609,0.4065]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_slump_oi', array[0.105592,0.031938,0.095472,-0.093745,0.284412,-0.001827,0.017192]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_slump_wi', array[139.78150, 0.06141,-0.02971, 0.05053,-0.23270, 0.10315,-0.05562,-0.03908]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_winequality_red_oi', array[0.004194,-1.099743,-0.184146,0.007071,-1.911419,0.004548,-0.003319,4.529146,-0.522898,0.887076,0.297023]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_winequality_red_wi', array[21.965208,0.024991,-1.083590,-0.182564,0.016331,-1.874225,0.004361,-0.003265,-17.881164,-0.413653,0.916334,0.276198]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_winequality_white_oi', array[-0.0505906,-1.9585102,-0.0293492,0.0249884,-0.9425824,0.0047908,-0.0008776,2.0420461,0.1683951,0.4164536,0.3656334]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_winequality_white_wi', array[1.502e+02,6.552e-02,-1.863e+00,2.209e-02,8.148e-02,-2.473e-01,3.733e-03,-2.857e-04,-1.503e+02,6.863e-01,6.315e-01,1.935e-01]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_singleobservation_oi', array[2.5,'NaN'::float8]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_singleobservation_wi', array[5,'NaN'::float8,'NaN'::float8]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_redundantobservations_oi', array[2.5,'NaN'::float8]::double precision[]);
INSERT INTO benchmark.evaluation_linear_regression values ('R', 'madlibtestdata.lin_redundantobservations_wi', array[1.026e-15,2.500e+00,'NaN'::float8]::double precision[]);




----logistic regression:
INSERT INTO benchmark.evaluation_logistic_regression values ('R', 'madlibtestdata.log_breast_cancer_wisconsin', array[0.45751,-0.09757,0.51090,0.22664,0.12815,-11.76916,-10.02613,-6.38382,-9.26560,-7.56327,-6.88371,-7.96898,9.34762,-7.95165,-8.25147,8.38956,0.54742,0.14284,0.42332]::double precision[]);
INSERT INTO benchmark.evaluation_logistic_regression values ('R', 'madlibtestdata.log_ticdata2000', array[0.065296,-0.175293,-0.036243,0.209379,-0.273175,-0.111356,-0.015259,-0.009634,-0.062309,0.245514,0.099065,0.162091,-0.052672,-0.089714,-0.046976,0.030217,-0.067661,-0.178444,0.076187,0.038062,-0.122351,0.124460,0.029639,0.087399,0.034644,0.001720,0.022059,0.105367,-0.009632,-0.808495,-0.772210,0.203983,0.176253,0.115631,-0.211918,-0.272568,0.109785,0.128446,0.093957,0.108515,-0.172171,0.095021,0.070646,0.594149,-0.278587,-0.442395,0.232654,12.166136,-0.081735,-2.106501,1.009410,0.717051,-5.469988,0.220171,-0.240524,-0.440200,1.461161,0.839852,0.238545,-8.709084,-0.185037,0.364253,-1.072293,-0.132734,-0.922634,0.418792,0.295587,-0.043149,-73.069779,0.236147,-4.501938,-1.342426,-2.355075,-1.001620,-1.066825,0.484111,0.388286,-3.220763,-3.286782,-0.406705,10.560940,2.520574,0.236120,1.947685,0.961970]::double precision[]);
INSERT INTO benchmark.evaluation_logistic_regression values ('R', 'madlibtestdata.log_wdbc', array[2.758e+03,-5.757e+01,-8.241e+01,-2.069e+01,-2.095e+04,2.532e+04,-1.362e+04,-1.379e+04,9.675e+03,-3.126e+04,-1.571e+03,1.469e+02,5.361e+02,-4.876e+01,4.997e+04,-4.304e+04,3.564e+04,-1.430e+05,4.460e+04,3.317e+05,-7.276e+02,-2.992e+01,-2.663e+01,4.871e+00,2.295e+03,3.375e+03,-2.845e+03,-2.752e+03,-9.730e+03,-2.453e+04]::double precision[]);
INSERT INTO benchmark.evaluation_logistic_regression values ('R', 'madlibtestdata.log_wpbc', array[-0.07867,-4.75085,-0.55311,0.18325,0.03192,274.63990,-18.78740,-26.18040,-26.14847,48.69332,-262.71936,-24.10346,-5.68894,5.02708,-0.05901,518.33296,223.84631,-198.58071,-348.60039,208.51486,298.48135,4.19352,0.47164,-0.32056,-0.01126,-37.29096,-27.22491,26.41491,-11.17975,-30.60805,83.95447]::double precision[]);
INSERT INTO benchmark.evaluation_logistic_regression values ('R', 'madlibtestdata.log_redundantobservations', array[23.57,-70.70]::double precision[]);

