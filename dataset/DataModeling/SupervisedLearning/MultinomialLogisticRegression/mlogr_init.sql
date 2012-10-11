-----------------------------------------------------------------------------
----------------- mlogr_largedim --------------------------------------------
-----------------------------------------------------------------------------
DROP TABLE IF EXISTS madlibtestdata.mlogr_largedim CASCADE;
create table madlibtestdata.mlogr_largedim(id int, x float8[], y int);
insert into  madlibtestdata.mlogr_largedim select 0,array_agg(c1),0 from generate_series(1,65536) c1;
alter table madlibtestdata.mlogr_largedim owner to madlibtester;

-----------------------------------------------------------------------------
----------------- mlogr_emptyobservation ------------------------------------
-----------------------------------------------------------------------------
DROP TABLE IF EXISTS madlibtestdata.mlogr_emptyobservation CASCADE;
create table madlibtestdata.mlogr_emptyobservation(id int, x float8[], y int);
alter table madlibtestdata.mlogr_emptyobservation owner to madlibtester;

-----------------------------------------------------------------------------
----------------- mlogr_singleobservation ------------------------------------
-----------------------------------------------------------------------------
DROP TABLE IF EXISTS madlibtestdata.mlogr_singleobservation CASCADE;
create table madlibtestdata.mlogr_singleobservation(id int, x float8[], y int);
alter table madlibtestdata.mlogr_singleobservation owner to madlibtester;

insert into madlibtestdata.mlogr_singleobservation values(1,'{1,1,2}',0);

-----------------------------------------------------------------------------
----------------- mlogr_redundantobservations ------------------------------------
-----------------------------------------------------------------------------
DROP TABLE IF EXISTS madlibtestdata.mlogr_redundantobservations CASCADE;
create table madlibtestdata.mlogr_redundantobservations(id int, x float8[], y int);
alter table madlibtestdata.mlogr_redundantobservations owner to madlibtester;

insert into madlibtestdata.mlogr_redundantobservations values(1,'{1,1,2}',0);
insert into madlibtestdata.mlogr_redundantobservations values(1,'{1,1,2}',0);
insert into madlibtestdata.mlogr_redundantobservations values(1,'{1,2,3}',1);


-----------------------------------------------------------------------------
----------------- madlibtestdata.mlogr_commonx_diffy_observations ------------------------------------
-----------------------------------------------------------------------------
DROP TABLE IF EXISTS madlibtestdata.mlogr_commonx_diffy_observations CASCADE;
create table madlibtestdata.mlogr_commonx_diffy_observations(id int, x float8[], y int);
alter table madlibtestdata.mlogr_commonx_diffy_observations owner to madlibtester;

insert into madlibtestdata.mlogr_commonx_diffy_observations values(1,'{1,1}',0);
insert into madlibtestdata.mlogr_commonx_diffy_observations values(1,'{1,1}',1);

-----------------------------------------------------------------------------
----------- madlibtestdata.mlogr_incorrecty_discontinuous_observations ------
-----------------------------------------------------------------------------
DROP TABLE IF EXISTS madlibtestdata.mlogr_incorrecty_discontinuous_observations CASCADE;
create table madlibtestdata.mlogr_incorrecty_discontinuous_observations(id int, x float8[], y int);
alter table madlibtestdata.mlogr_incorrecty_discontinuous_observations owner to madlibtester;

insert into madlibtestdata.mlogr_incorrecty_discontinuous_observations values(1,'{1,1,2}',0);
insert into madlibtestdata.mlogr_incorrecty_discontinuous_observations values(1,'{1,2,3}',2);
insert into madlibtestdata.mlogr_incorrecty_discontinuous_observations values(1,'{1,3,4}',4);


-----------------------------------------------------------------------------
----------- madlibtestdata.mlogr_incorrecty_nonzero_observations ------------
-----------------------------------------------------------------------------
DROP TABLE IF EXISTS madlibtestdata.mlogr_incorrecty_nonzero_observations CASCADE;
create table madlibtestdata.mlogr_incorrecty_nonzero_observations(id int, x float8[], y int);
alter table madlibtestdata.mlogr_incorrecty_nonzero_observations owner to madlibtester;

insert into madlibtestdata.mlogr_incorrecty_nonzero_observations values(1,'{1,1,2}',1);
insert into madlibtestdata.mlogr_incorrecty_nonzero_observations values(1,'{1,2,3}',2);
insert into madlibtestdata.mlogr_incorrecty_nonzero_observations values(1,'{1,3,4}',3);

-----------------------------------------------------------------------------
----------------- madlibtestdata.mlogr_incorrecty_negative_observations -----
-----------------------------------------------------------------------------
DROP TABLE IF EXISTS madlibtestdata.mlogr_incorrecty_negative_observations CASCADE;
create table madlibtestdata.mlogr_incorrecty_negative_observations(id int, x float8[], y int);
alter table madlibtestdata.mlogr_incorrecty_negative_observations owner to madlibtester;

insert into madlibtestdata.mlogr_incorrecty_negative_observations values(1,'{1,1,2}',-1);
insert into madlibtestdata.mlogr_incorrecty_negative_observations values(1,'{1,2,3}',0);
insert into madlibtestdata.mlogr_incorrecty_negative_observations values(1,'{1,3,4}',1);

-----------------------------------------------------------------------------
-------- madlibtestdata.mlogr_incorrecty_decimal_observations ---------------
-----------------------------------------------------------------------------
DROP TABLE IF EXISTS madlibtestdata.mlogr_incorrecty_decimal_observations CASCADE;
create table madlibtestdata.mlogr_incorrecty_decimal_observations(id int, x float8[], y float8);
alter table madlibtestdata.mlogr_incorrecty_decimal_observations owner to madlibtester;

insert into madlibtestdata.mlogr_incorrecty_decimal_observations values(1,'{1,1,2}',0.1);
insert into madlibtestdata.mlogr_incorrecty_decimal_observations values(1,'{1,2,3}',0.2);
insert into madlibtestdata.mlogr_incorrecty_decimal_observations values(1,'{1,3,4}',0.3);

-----------------------------------------------------------------------------
-------------------- madlibtestdata.U&"~!@#$%^*()_+-=:{[}]|,.?/Table" ------
-----------------------------------------------------------------------------
DROP TABLE IF EXISTS madlibtestdata.U&"~!@#$%^*()_+-=:{[}]|,.?/Table" CASCADE;
create table madlibtestdata.U&"~!@#$%^*()_+-=:{[}]|,.?/Table"(id int, x float8[], y int);
alter table  madlibtestdata.U&"~!@#$%^*()_+-=:{[}]|,.?/Table"owner to madlibtester;

insert into madlibtestdata.U&"~!@#$%^*()_+-=:{[}]|,.?/Table" values(1,'{1,1,2}',0);
insert into madlibtestdata.U&"~!@#$%^*()_+-=:{[}]|,.?/Table" values(1,'{1,2,3}',1);
insert into madlibtestdata.U&"~!@#$%^*()_+-=:{[}]|,.?/Table" values(1,'{1,3,4}',2);


-----------------------------------------------------------------------------
-------------------- U&"~!@#$%^*()_+-=:{[}]|\,.?/Schema".testtable  ---------
-----------------------------------------------------------------------------
--create schema
create schema U&"~!@#$%^*()_+-=:{[}]|,.?/Schema";
alter schema U&"~!@#$%^*()_+-=:{[}]|,.?/Schema" owner TO madlibtester;

DROP TABLE IF EXISTS U&"~!@#$%^*()_+-=:{[}]|,.?/Schema".testtable CASCADE;
create table U&"~!@#$%^*()_+-=:{[}]|,.?/Schema".testtable(id int, x float8[], y int);
alter table  U&"~!@#$%^*()_+-=:{[}]|,.?/Schema".testtable owner to madlibtester;

insert into U&"~!@#$%^*()_+-=:{[}]|,.?/Schema".testtable values(1,'{1,1,2}',0);
insert into U&"~!@#$%^*()_+-=:{[}]|,.?/Schema".testtable values(1,'{1,2,3}',1);
insert into U&"~!@#$%^*()_+-=:{[}]|,.?/Schema".testtable values(1,'{1,3,4}',2);

-----------------------------------------------------------------------------
-------------------- madlibtestdata.testtable -------------------------------
-----------------------------------------------------------------------------
DROP TABLE IF EXISTS madlibtestdata.testtable CASCADE;
create table madlibtestdata.testtable(id int, x float8[], U&"~!@#$%^*()_+-=:{[}]|,.?/Column" int);
alter table  madlibtestdata.testtable owner to madlibtester;

insert into madlibtestdata.testtable values(1,'{1,1,2}',0);
insert into madlibtestdata.testtable values(1,'{1,2,3}',1);
insert into madlibtestdata.testtable values(1,'{1,3,4}',2);

CREATE OR REPLACE FUNCTION madlibtestdata.mlogr_get_category(list_x TEXT, list_coef TEXT)
RETURNS int as $$
from math import exp

L_x = list_x[1:-1].split(",")
L_Coef = list_coef[1:-1].split(",")

length = len(L_x)
sum = 0

# use the first element as the pivot
list_ef = []
list_ef.append(1)

if len(L_Coef) % length != 0:
    return -1
for i in range(len(L_Coef)/length):
    f = 0
    for j in range(i*length, (i+1)*length):
	f += float(L_Coef[j]) * float(L_x[j%length])
    ef = exp(f)
    list_ef.append(ef)
    sum += ef

max_prob = 0
max_category = 0
for i in range(len(list_ef)):
    tmp_prob = list_ef[i]/(1+sum)
    if tmp_prob > max_prob:
	max_category = i
	max_prob = tmp_prob

return max_category

$$ LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION madlibtestdata.mlogr_precision_score(source_table TEXT, dependent_varname TEXT, number_of_categories INT, independent_varname TEXT, max_iteration INT, optimizer TEXT,convergence_threshold FLOAT8)
RETURNS FLOAT8 as $$
strSql= "select coef from madlib.mlogregr('%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (source_table, dependent_varname, number_of_categories, independent_varname, max_iteration, optimizer, convergence_threshold)
CACHE_LIMIT = 2000;

Q_Result = plpy.execute(strSql)
L_Coef = Q_Result[0]['coef']


num_correct = 0  #counter
num_false = 0    #counter

#classification
strSql = "select max(id) as max_id from %s;" % (source_table)
Q_Result_count = plpy.execute(strSql)
max_id = int(Q_Result_count[0]['max_id'])
max_id_bak = max_id

#Get result according to CACHE_LIMIT
while max_id >= 0:
    if max_id < CACHE_LIMIT-1:
	strSql = "select * from %s where id >= 0 and id <= %s;" % (source_table, max_id )
	Q_Result_data = plpy.execute(strSql)
	for i in range(len(Q_Result_data)):
	    strSql_tmp = "select * from madlibtestdata.mlogr_get_category('%s', '%s')" % (str(Q_Result_data[i]['x']), str(L_Coef))
	    Q_Result_category = plpy.execute(strSql_tmp)
	    tmp_category = int(Q_Result_category[0]['mlogr_get_category'])
	    if Q_Result_data[i]['y'] == tmp_category:
		num_correct += 1
	    else:
		num_false += 1

    else:
	strSql = "select * from %s where id > %s and id <= %s;" % (source_table, (max_id - CACHE_LIMIT), max_id)
	Q_Result_data = plpy.execute(strSql)
        for i in range(len(Q_Result_data)):
	    strSql_tmp = "select * from madlibtestdata.mlogr_get_category('%s', '%s')" % (str(Q_Result_data[i]['x']), str(L_Coef))
            Q_Result_category = plpy.execute(strSql_tmp)
            tmp_category = int(Q_Result_category[0]['mlogr_get_category'])
            if Q_Result_data[i]['y'] == tmp_category: 
                num_correct += 1
            else:
                num_false += 1
	
    max_id = max_id - CACHE_LIMIT	

#compute precision
#if num_correct+num_false != max_id +1:
return float(num_correct) / float(num_correct + num_false) 

$$ LANGUAGE plpythonu;

ALTER FUNCTION madlibtestdata.mlogr_precision_score(source_table TEXT, dependent_varname TEXT, number_of_categories INT, independent_varname TEXT, max_iteration INT, optimizer TEXT,convergence_threshold FLOAT8) OWNER TO madlibtester;
ALTER FUNCTION madlibtestdata.mlogr_get_category(list_x TEXT, list_coef TEXT) OWNER TO madlibtester;
