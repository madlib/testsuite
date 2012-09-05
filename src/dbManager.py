"""
Start up and/or shut down the database.
"""

srcPath = './generator/'

import os, sys, subprocess

sys.path.append('../bin/')
from utility import *
from generator.analytics_tool import *
from generator.run_sql import *


DataSchema              =   'madlibtestdata'
ResultSchema            =   'madlibtestresult'
MadlibSchema            =   'madlib'


class dbManager:
    """Start up and/or shut down the database."""

    def __init__(self, xmlPath):
        """Parse AnalyticsTool.xml, get the configuration information of analytics tool
    
        return map {name of tool to it's configuration}, and the configuration is name-value map
        """
        try:
            analyticsTools = AnalyticsTools(xmlPath)
            analyticsTools.parseTools()
        except Exception, exp:
            print "Error when parsing analyticsTools: " + str(exp)
            sys.exit()
        self.dbs = analyticsTools.analyticsTools
        self.cur_db = None

    def getNames(self):
        return self.dbs.iterkeys()

    def info(self, name = None):
        """Show info of db."""
        if name is None:
            print [key for (key, _) in self.dbs.items()]
        else:
            if not name in self.dbs:
                sys.exit('Database ' + name + ' does not exist.')
            conf  = self.dbs[name]
            print conf

    def start(self, name):
        """Start up db."""
        if not name in self.dbs:
            sys.exit('Database ' + name + ' does not exist.')
        conf  = self.dbs[name]
        source_path = 'source ' + conf['env'] + '&& '
        master_dir = conf['master_dir']
        if conf['kind'] == 'postgres':
            subprocess.call(source_path + 'pg_ctl start -D ' + master_dir, shell = True)
            time.sleep(10)
        elif conf['kind'] == 'greenplum':
            subprocess.call(source_path + 'gpstart -a -d ' + master_dir, shell = True)
        self.cur_db = name

    def stop(self, name):
        """Shut down db."""
        if not name in self.dbs:
            sys.exit('Database ' + name + ' does not exist.')
        conf  = self.dbs[name]
        source_path = 'source ' + conf['env'] + '&& '
        master_dir = conf['master_dir']
        if conf['kind'] == 'postgres':
            subprocess.call(source_path + 'pg_ctl stop -D ' + master_dir, shell = True)
            time.sleep(10)
        elif conf['kind'] == 'greenplum':
            subprocess.call(source_path + 'gpstop -a -d ' + master_dir, shell = True)
        self.cur_db = None

    def loadSQL(self, file):
        """Load contents within a sql file into db."""
        if self.cur_db is None:
            sys.exit('Database instance ' + self.cur_db + ' is not running.')
        conf  = self.dbs[self.cur_db]
        source_path = 'source ' + conf['env']
        cmd = "psql -q -p %s -d %s -f %s" % (conf['port'], conf['database'], file)
        subprocess.check_call(source_path + '&& ' + cmd, shell = True)

    def runSQL(self, sql):
        """Run a sql at current db."""
        if self.cur_db is None:
            sys.exit('Database instance ' + self.cur_db + ' is not running.')
        conf = self.dbs[self.cur_db]
        cmd = "psql -p %s -d %s -c '%s'" % (conf['port'], conf['database'], sql)
        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell = True).communicate()[0]

    def initDB(self):
        """Recreate the DB."""
        conf  = self.dbs[self.cur_db]
        add_user = AddUser('madlibtester')

        hostname = conf['host']
        username = conf['username']
        superuser = conf['superuser']
        dbname = conf['database']
        dbtemplate = 'template1'
        port = conf['port']

        # 0. Update pg_hba to allow madlibtester to access all databases
        add_user.addUserPGHBA(conf["master_dir"], conf['kind'])
        # clear up
        stmts = []
        stmts.append('DROP DATABASE %s '%dbname)
        stmts.append('DROP USER %s '%username)
        add_user.run_sql_stmts('source %s'%conf['env'], stmts, hostname, port, dbtemplate, superuser)

        # 1. Create non super user with super user
        stmts = []
        stmts.append('CREATE USER %s WITH CREATEDB'%username)
        add_user.run_sql_stmts('source %s'%conf['env'], stmts, hostname, port, dbtemplate, superuser)

        # 2. Create database with non super user
        stmts = []
        stmts.append('CREATE DATABASE %s '%dbname)
        add_user.run_sql_stmts('source %s'%conf['env'], stmts, hostname, port, dbtemplate, username)

        # 3. Create schema with non super user and created database
        stmts = []
        stmts.append('CREATE SCHEMA %s'%ResultSchema)
        add_user.run_sql_stmts('source %s'%conf['env'], stmts, hostname, port, dbname, username)

        # 4. Create schema to store test data
        stmts = []
        stmts.append('CREATE SCHEMA %s'%DataSchema)
        add_user.run_sql_stmts('source %s'%conf['env'], stmts, hostname, port, dbname, username)


def main():
    db_manager = dbManager()
    db_manager.info()
    db_manager.info('GPDB_4.2.0_24')

if __name__ == '__main__':
    main()
