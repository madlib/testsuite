DROP TABLE IF EXISTS benchmark.analyticstool cascade;

CREATE TABLE benchmark.analyticstool(username varchar(128),superuser varchar(128),kind varchar(128),master_dir text,name varchar(128),database varchar(128),toolversion varchar(128),host varchar(128),madlibversion varchar(128),env text,port int,segmentnum smallint);

INSERT INTO benchmark.analyticstool(username,superuser,kind,master_dir,name,database,toolversion,host,madlibversion,env,port,segmentnum)VALUES('iyerr3','iyerr3','greenplum','/Users/iyerr3/greenplum-db-data/master/gpseg-1','GPDB42','madmark','4.2.0.0','127.0.0.1','0.5','/Users/iyerr3/greenplum-db-devel/greenplum_path.sh',5432,2);
