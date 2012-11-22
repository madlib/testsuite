"""
Start up and/or shut down the database.
"""
import os, sys, subprocess, socket, time
import run_sql

DataSchema              =   'madlibtestdata'
ResultSchema            =   'madlibtestresult'
MadlibSchema            =   'madlib'


class dbManager:
    """Start up and/or shut down the database."""

    def __init__(self, db_conf):
        """Parse AnalyticsTool.xml, get the configuration information of analytics tool
    
        return map {name of tool to it's configuration}, and the configuration is name-value map
        """
        self.db_conf = db_conf
        self.cur_db =  db_conf['name']

    def start(self):
        """Start up db."""
        conf  = self.db_conf
        source_path = 'source ' + conf['env'] + '&& '
        master_dir = conf['master_dir']
        if conf['kind'].lower() == 'postgres':
            subprocess.call(source_path + 'pg_ctl start -D ' + master_dir, shell = True)
            time.sleep(10)
        elif conf['kind'].lower() == 'greenplum':
            subprocess.call(source_path + 'gpstart -a -d ' + master_dir, shell = True)

    def stop(self):
        """Shut down db."""
        conf  = self.db_conf
        source_path = 'source ' + conf['env'] + '&& '
        master_dir = conf['master_dir']
        if conf['kind'] == 'postgres':
            subprocess.call(source_path + 'pg_ctl stop -D ' + master_dir, shell = True)
            time.sleep(10)
        elif conf['kind'] == 'greenplum':
            subprocess.call(source_path + 'gpstop -a -d ' + master_dir, shell = True)

    def getDBsqlArgs(self):

        dbconf = self.db_conf
        args = []
        if 'username' in dbconf:
            args.extend(['-U', dbconf['username']])
        if 'host' in dbconf:
            args.extend(['-h', dbconf['host']])
        if 'port' in dbconf:
            args.extend(['-p', dbconf['port']])
        if 'database' in dbconf:
            args.extend(['-d', dbconf['database']])

        return args

    def getDBenv(self):
    
        return self.db_conf['env']

    def getDBConnection(self):

        dbconf = self.db_conf
        try:
            if 'schema' in dbconf:
                return dbconf['username'] + "@" + dbconf['host'] + ":" + dbconf['port'] + "/" \
                         + dbconf['database'] + ":"  + dbconf['schema']
            else:
                return dbconf['username'] + "@" + dbconf['host'] + ":" + dbconf['port'] + "/" \
                         + dbconf['database']

        except KeyError:
            sys.exit("ERROR: Incomplete database configuration information.")
        except Exception, exp:
            print str(exp)
            sys.exit()
    
    def initDB(self):
        """Recreate the DB."""
        conf  = self.db_conf

        hostname = conf['host']
        username = conf['username']
        superuser = conf['superuser']
        dbname = conf['database']
        dbtemplate = 'template1'
        port = conf['port']

        print "###Init test DB start ###\n"
        # 0. Update pg_hba to allow madlibtester to access all databases
        self.addUserPGHBA(username, conf["master_dir"], conf['kind'])
        # clear up
        try :
            sql = 'DROP SCHEMA %s CASCADE'%ResultSchema
            run_sql.runSQL(sql, logusername = superuser, logport = port, logdatabase = dbname, \
                             onErrorStop = False, source_path = conf['env'])
         
            sql = 'DROP SCHEMA %s CASCADE'%DataSchema
            run_sql.runSQL(sql, logusername = superuser, logport = port, logdatabase = dbname, \
                             onErrorStop = False, source_path = conf['env'])
        except Exception, e:
            print e
            print 'Database "%s" does not exist \nCreate the new database "%s"'%(dbname,dbname)

        sql = 'DROP USER %s CASCADE'%username
        run_sql.runSQL(sql, logusername = superuser, logport = port, logdatabase = dbtemplate, \
                         onErrorStop = False, source_path = conf['env'])

        # 1. Create non super user with super user
        sql = 'CREATE USER %s WITH CREATEDB'%username
        run_sql.runSQL(sql, logusername = superuser, logport = port, logdatabase = dbtemplate, \
                          onErrorStop = False, source_path = conf['env'])

        # 2. Create database non super user
        sql = 'CREATE DATABASE %s '%dbname
        run_sql.runSQL(sql, logusername = superuser, logport = port, logdatabase = dbtemplate, \
                         onErrorStop = False,  source_path = conf['env'])
        
        # 3. Change database owner to non super user
        sql = 'ALTER DATABASE %s OWNER TO %s'%(dbname,username) 
        run_sql.runSQL(sql, logusername = superuser, logport = port, logdatabase = dbtemplate, \
                        onErrorStop = False, source_path = conf['env'])

        # 4. Create schema with non super user and created database
        sql = 'CREATE SCHEMA %s'%ResultSchema
        run_sql.runSQL(sql, logusername = username, logport = port, logdatabase = dbname, \
                         onErrorStop = False,  source_path = conf['env'])

        # 5. Create schema to store test data
        sql = 'CREATE SCHEMA %s'%DataSchema
        run_sql.runSQL(sql, logusername = username, logport = port, logdatabase = dbname, \
                         onErrorStop = False, source_path = conf['env'])
        print "###Success Init result DB ###\n"

    def addUserPGHBA(self, username, master_data_dir, kind = 'greenplum'):
        """Add user authentication entry in $MASTER_DATA_DIRECTORY/pg_hba.conf"""
        pghbastrs = []
        pghbastrs.extend( ["local    all  %s     trust"%username] )
        pghbastrs.extend( ["host    all  %s     %s/28   trust"%(username, ip)
                                                   for ip in getIpv4Addr() ] )
        f = master_data_dir + '/pg_hba.conf'
        pg_hba_lines = [ lin.strip() for lin in open(f).readlines() ]
        fh = open(f, "a")
        for pghbastr in pghbastrs:
            if pghbastr.strip() in pg_hba_lines:
                continue
            else:
                fh.write("\n%s" % (pghbastr))

        fh.close()
        if kind.upper() == 'GREENPLUM':
            os.system("gpstop -u -d %s"%master_data_dir)
        elif kind.upper() == 'POSTGRES':
            os.system("pg_ctl reload -D %s -s"%master_data_dir)

def getIpv4Addr():
    host_name = socket.gethostname()
    host_info = socket.getaddrinfo(host_name, None)
    ipv4_list = ['127.0.0.1']
    ipv4_list.extend([ ip for ip in list(set([(ai[4][0])
                    for ai in host_info])) if ip.find(".") > 0])
    return ipv4_list

def main():
    db_manager = dbManager()
    db_manager.info()
    db_manager.info('GPDB_4.2.0_24')

if __name__ == '__main__':
    main()
