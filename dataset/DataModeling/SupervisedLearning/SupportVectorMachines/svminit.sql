CREATE language PLPGSQL;
CREATE language PLPYTHONU;

--postload.sql--
set search_path = madlibtestdata;

create table svm_largedim(id int, ind float8[], label float8);
insert into  svm_largedim select 1,array_agg(c1),1 from generate_series(1,102401) c1;
insert into  svm_largedim select 2,array_agg(c1),-1 from generate_series(1,102401) c1;
alter table svm_largedim owner to madlibtester;

create table svm_a9a_in as select * from svm_a9a where label = 1;
create table svm_a9a_out as select * from svm_a9a where label = -1;

create table svm_rcv1_binary_in as select * from svm_rcv1_binary where label = 1;
create table svm_rcv1_binary_out as select * from svm_rcv1_binary where label = -1;

create table svm_epsilon_in as select * from svm_epsilon where label = 1;
create table svm_epsilon_out as select * from svm_epsilon where label = -1;

create table svm_recordlink_in as select * from svm_recordlink where label = 1;
create table svm_recordlink_out as select * from svm_recordlink where label = -1;

create table svm_realsim_in as select * from svm_realsim where label = 1;
create table svm_realsim_out as select * from svm_realsim where label = -1;

create table svm_ijcnn_in as select * from svm_ijcnn where label = 1;
create table svm_ijcnn_out as select * from svm_ijcnn where label = -1;

create table svm_splice_in as select * from svm_splice where label = 1;
create table svm_splice_out as select * from svm_splice where label = -1;

create table svm_largedim_in as select * from svm_largedim where label = 1;
create table svm_largedim_out as select * from svm_largedim where label = -1;


alter table svm_a9a_in owner to madlibtester;
alter table svm_a9a_out owner to madlibtester;

alter table svm_rcv1_binary_in owner to madlibtester;
alter table svm_rcv1_binary_out owner to madlibtester;

alter table svm_epsilon_in owner to madlibtester;
alter table svm_epsilon_out owner to madlibtester;

alter table svm_recordlink_in owner to madlibtester;
alter table svm_recordlink_out owner to madlibtester;

alter table svm_realsim_in owner to madlibtester;
alter table svm_realsim_out owner to madlibtester;

alter table svm_ijcnn_in owner to madlibtester;
alter table svm_ijcnn_out owner to madlibtester;

alter table svm_splice_in owner to madlibtester;
alter table svm_splice_out owner to madlibtester;

alter table svm_largedim_in owner to madlibtester;
alter table svm_largedim_out owner to madlibtester;

--classification.sql--
set search_path = madlibtestdata;
CREATE OR REPLACE FUNCTION madlibtestdata.svm_cls_predict_score(model_table TEXT, input_table TEXT, parallel bool, linear bool)
RETURNS float AS $$

#### fetch x and y ###################
fetchy="SELECT ind, label from %s order by id limit 1;" % (input_table)
plpy.info(fetchy)
point_value = plpy.execute(fetchy)
y = point_value[0]["label"]
x_point_str = str(point_value[0]["ind"])
plpy.info(str(x_point_str))
x_point_str  = x_point_str.replace('[','{')
x_point_str  = x_point_str.replace(']','}')
x_point_str  = x_point_str.replace('None','Null')
plpy.info(str(y)) 
plpy.info(str(x_point_str)) 
   
#### fetch fx ###################
if parallel:
    if linear:
        predict_sql = "SELECT  prediction  FROM madlib.lsvm_predict_combo ('%s', '%s') where model = 'avg' ;" % (model_table, x_point_str)
    else:
        predict_sql = "SELECT  prediction  FROM madlib.svm_predict_combo ('%s', '%s') where model = 'avg' ;" % (model_table, x_point_str)
else: 
    if linear:
        predict_sql = "SELECT madlib.lsvm_predict ('%s', '%s') as prediction;" % (model_table, x_point_str) 
    else:
        predict_sql = "SELECT madlib.svm_predict ('%s', '%s') as prediction;" % (model_table, x_point_str) 
plpy.info(predict_sql)
fx_values = plpy.execute(predict_sql)
fx = fx_values[0]["prediction"]
plpy.info(str(fx))
avg_value = fx * y
if avg_value > 0:
   return 1
else:
   return 0

$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_cls_predict_batch_score(model_table TEXT, output_table text, input_table TEXT, parallel bool, linear bool)
RETURNS float AS $$
#### fetch fx ###################
if linear:
    predict_sql = "SELECT  madlib.lsvm_predict_batch('%s', 'ind', 'id', '%s', '%s', %s) ;" % (input_table, model_table, output_table, parallel)
else:
    predict_sql = "SELECT  madlib.svm_predict_batch('%s', 'ind', 'id', '%s', '%s', %s) ;" % (input_table, model_table, output_table, parallel)
plpy.info(predict_sql)
plpy.execute(predict_sql)

if parallel:
    outputstr = "SELECT id, CASE WHEN (SUM(CASE WHEN prediction > 0 THEN 1 ELSE 0 END) * 2) > COUNT(*) THEN 1 ELSE -1 END as prediction FROM %s GROUP BY id" % (output_table)
else:
    outputstr = "SELECT id, prediction as prediction FROM %s" % (output_table)

distsql = "SELECT SUM(CASE WHEN prediction * label > 0 then 1 else 0 end)::float8/COUNT(*)::float8  AS score FROM  %s as input, (%s) as output where input.id = output.id;" % (input_table, outputstr)
plpy.info(distsql)
dist_value = plpy.execute(distsql)
avg_score = dist_value[0]["score"]
return avg_score
$$ LANGUAGE plpythonu;

--kernelfunc.sql--
ALTER FUNCTION madlibtestdata.svm_cls_predict_score(model_table TEXT, input_table TEXT, parallel bool, linear bool) owner to madlibtester;
ALTER FUNCTION madlibtestdata.svm_cls_predict_batch_score(model_table TEXT, output_table text, input_table TEXT, parallel bool, linear bool) owner to madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_polynomial(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	RETURN madlib.svm_polynomial($1,$2,3);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_polynomial(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_abalone(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
        RETURN madlib.svm_gaussian($1,$2,1/8);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_abalone(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_bodyfat(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
        RETURN  madlib.svm_gaussian($1,$2,1/14);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_bodyfat(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_cadata(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	RETURN  madlib.svm_gaussian($1,$2,1/8);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_cadata(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_cpusmall(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
         RETURN madlib.svm_gaussian($1,$2,1/12);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_cpusmall(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_largedim(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
         RETURN madlib.svm_gaussian($1,$2,1/4272227);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_largedim(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_etfidf(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN  
         RETURN madlib.svm_gaussian($1,$2,1/150360);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_etfidf(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_eunite(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
         RETURN madlib.svm_gaussian($1,$2,1/16);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_eunite(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_housing(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
          RETURN madlib.svm_gaussian($1,$2,1/13);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_housing(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_mg(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
     	   RETURN madlib.svm_gaussian($1,$2,1/6);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_mg(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_mpg(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
    	   RETURN madlib.svm_gaussian($1,$2,1/7);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_mpg(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_pyrim(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	   RETURN madlib.svm_gaussian($1,$2,1/27);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_pyrim(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_space(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/6);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_space(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_triazines(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/60);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_triazines(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_yp(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/90);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_yp(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_a9a(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/123);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_a9a(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_rcv1_binary(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/47236);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_rcv1_binary(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_epsilon(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/2000);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_epsilon(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_recorklink(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/9);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_recorklink(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_realsim(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/20958);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_realsim(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_ijcnn(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/22);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_ijcnn(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_splice(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/60);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_splice(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_largedim(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/3231961);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_largedim(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_a9a_in(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
            RETURN madlib.svm_gaussian($1,$2,1/123);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_a9a_in(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_rcv1_binary_in(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/47236);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_rcv1_binary_in(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_epsilon_in(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/2000);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_epsilon_in(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_recorklink_in(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/9);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_recorklink_in(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_realsim_in(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/20958);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_realsim_in(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_ijcnn_in(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/22);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_ijcnn_in(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_splice_in(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	    RETURN madlib.svm_gaussian($1,$2,1/60);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_splice_in(FLOAT[],FLOAT[]) OWNER TO madlibtester;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_gaussian_largedim_in(FLOAT[],FLOAT[]) RETURNS FLOAT AS $$
BEGIN
	     RETURN madlib.svm_gaussian($1,$2,1/3231961);
END
$$ language PLPGSQL;

ALTER FUNCTION madlibtestdata.svm_gaussian_largedim_in(FLOAT[],FLOAT[]) OWNER TO madlibtester;

--novelty.sql--
SET SEARCH_PATH = madlibtestdata;
CREATE OR REPLACE FUNCTION madlibtestdata.svm_nd_predict_score(model_table TEXT,  input_table TEXT, parallel bool)
RETURNS float AS $$

#### fetch x and y ###################
fetchy="SELECT ind, label from %s order by id limit 1;" % (input_table)
plpy.info(fetchy)
point_value = plpy.execute(fetchy)
y = point_value[0]["label"]
x_point_str = str(point_value[0]["ind"])
plpy.info(str(x_point_str))
x_point_str  = x_point_str.replace('[','{')
x_point_str  = x_point_str.replace(']','}')
x_point_str  = x_point_str.replace('None','Null')
plpy.info(str(y)) 
plpy.info(str(x_point_str)) 
   
#### fetch fx ###################
if parallel:
    predict_sql = "SELECT  prediction FROM madlib.svm_predict_combo ('%s', '%s') where model = 'avg' ;" % (model_table, x_point_str)
else: 
    predict_sql = "SELECT madlib.svm_predict ('%s', '%s') as prediction;" % (model_table, x_point_str) 
plpy.info(predict_sql)
fx_values = plpy.execute(predict_sql)
fx = fx_values[0]["prediction"]
plpy.info(str(fx))
avg_value = fx * y
if avg_value > 0:
    return 1
else:
    return 0
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION madlibtestdata.svm_nd_predict_batch_score(model_table TEXT, output_table text, input_table TEXT, parallel bool)
RETURNS float AS $$
  
#### fetch fx ###################
predict_sql = "SELECT  madlib.svm_predict_batch('%s', 'ind', 'id', '%s', '%s', %s) ;" % (input_table, model_table, output_table, parallel)
plpy.info(predict_sql)
plpy.execute(predict_sql)

if parallel:
    outputstr = "SELECT id, CASE WHEN (SUM(CASE WHEN prediction > 0 THEN 1 ELSE 0 END) * 2) > COUNT(*) THEN 1 ELSE -1 END as prediction FROM %s GROUP BY id" % (output_table)
else:
    outputstr = "SELECT id, prediction as prediction FROM %s" % (output_table)

distsql = "SELECT SUM(CASE WHEN prediction * label > 0 then 1 else 0 end)::float8/COUNT(*)::float8  AS score FROM  %s as input, (%s) as output where input.id = output.id;" % (input_table, outputstr)
plpy.info(distsql)
dist_value = plpy.execute(distsql)
avg_score = dist_value[0]["score"]
return avg_score
$$ LANGUAGE plpythonu;


ALTER FUNCTION  madlibtestdata.svm_nd_predict_batch_score(model_table TEXT, output_table text, input_table TEXT, parallel bool) OWNER TO madlibtester;
ALTER FUNCTION  madlibtestdata.svm_nd_predict_score(model_table TEXT, input_table TEXT, parallel bool) OWNER TO madlibtester;


--regression.sql--
CREATE OR REPLACE FUNCTION madlibtestdata.svm_reg_predict_score(model_table TEXT, input_table TEXT, parallel bool)
RETURNS float AS $$
#### fetch x and y ###################
fetchy="SELECT ind, label from %s order by id limit 1;" % (input_table)
plpy.info(fetchy)
point_value = plpy.execute(fetchy)
y = point_value[0]["label"]
x_point_str = str(point_value[0]["ind"])
plpy.info(str(x_point_str))
x_point_str  = x_point_str.replace('[','{')
x_point_str  = x_point_str.replace(']','}')
x_point_str  = x_point_str.replace('None','Null')
plpy.info(str(y)) 
plpy.info(str(x_point_str)) 
   
#### fetch fx ###################
if parallel:
    predict_sql = "SELECT  prediction FROM madlib.svm_predict_combo ('%s', '%s') where model = 'avg' ;" % (model_table, x_point_str)
else: 
    predict_sql = "SELECT madlib.svm_predict ('%s', '%s') as prediction;" % (model_table, x_point_str) 
plpy.info(predict_sql)
fx_values = plpy.execute(predict_sql)
fx = fx_values[0]["prediction"]
plpy.info(str(fx))
avg_value = (fx - y) * (fx - y)
return avg_value;
$$ LANGUAGE plpythonu;


 
CREATE OR REPLACE FUNCTION madlibtestdata.svm_reg_predict_batch_score(model_table TEXT, output_table text, input_table TEXT, parallel bool)
RETURNS float AS $$
 
predict_sql = "SELECT  madlib.svm_predict_batch('%s', 'ind', 'id', '%s', '%s', %s) ;" % (input_table, model_table, output_table, parallel)
plpy.info(predict_sql)
plpy.execute(predict_sql)
if parallel:
    outputstr = "SELECT id, avg(prediction) as prediction FROM %s GROUP BY id" % (output_table)
else:
    outputstr = "SELECT id, prediction as prediction FROM %s" % (output_table)
distsql = "SELECT AVG((prediction  - label)*(prediction  - label))  AS score FROM  %s as input, (%s) as output where input.id = output.id;" % ( input_table, outputstr)
plpy.info(distsql)
dist_value = plpy.execute(distsql)
avg_dist = dist_value[0]["score"]
return avg_dist
$$ LANGUAGE plpythonu;
ALTER  FUNCTION madlibtestdata.svm_reg_predict_score(model_table TEXT, input_table TEXT, parallel bool) OWNER TO madlibtester;
ALTER  FUNCTION madlibtestdata.svm_reg_predict_batch_score(model_table TEXT, output_table text, input_table TEXT, parallel bool) OWNER TO madlibtester;
