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
grant usage on schema U&"~!@#$%^*()_+-=:{[}]|,.?/Schema" TO madlibtester;

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

