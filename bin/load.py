#!/usr/bin/env python
"""Insert test data into database. """
from xml.dom.minidom import parse, parseString
import os
import sys
import subprocess
import time

from utility import *

AnalyticsTool           =   '../testspec/metadata/analyticstool.xml'
DataSchema              =   'madlibtestdata'
ResultSchema            =   'madlibtestresult'
MadlibSchema            =   'madlib'
GeneratorSrcDir         =   '../src/generator/'

if __name__ == '__main__':    
    sys.path.append(GeneratorSrcDir)
  #  import env
    
    add_user = AddUser('madlibtester')

    master_names = []
    tools = AnalyticsTools(AnalyticsTool)
    tools.parserTools()
    for name, value in tools.analyticsTools.items():
        master_names.append(name)

    for name in master_names:
        if tools.analyticsTools[name]['kind'].upper() == 'POSTGRES':
            
            os.system('source %s && pg_ctl start -D %s'%(tools.analyticsTools[name]['env'], tools.analyticsTools[name]['master_dir'] ))
            time.sleep(10)
	
            hostname = tools.analyticsTools[name]['host']
            username = tools.analyticsTools[name]['username']
            superusername = tools.analyticsTools[name]['superusername']
            dbname = tools.analyticsTools[name]['database']
            dbtemplate = 'template1'
            port = tools.analyticsTools[name]['port']

            # 0. Update pg_hba to allow madlibtester to access all databases
            add_user.addUserPGHBA(tools.analyticsTools[name]["master_dir"], tools.analyticsTools[name]['kind'])
            # clear up
            stmts = []
            stmts.append('DROP DATABASE %s '%dbname)
            stmts.append('DROP USER %s '%username)
            add_user.run_sql_stmts('source %s'%tools.analyticsTools[name]['env'], stmts, hostname, port, dbtemplate, superusername)


            # 1. Create non super user with super user
            stmts = []
            stmts.append('CREATE USER %s WITH CREATEDB'%username)
            add_user.run_sql_stmts('source %s'%tools.analyticsTools[name]['env'], stmts, hostname, port, dbtemplate, superusername)

            # 2. Create database with non super user
            stmts = []
            stmts.append('CREATE DATABASE %s '%dbname)
            add_user.run_sql_stmts('source %s'%tools.analyticsTools[name]['env'], stmts, hostname, port, dbtemplate, username)

            # 3. Create schema with non super user and created database
            stmts = []
#            stmts.append('CREATE SCHEMA %s'%DataSchema)
            stmts.append('CREATE SCHEMA %s'%ResultSchema)
            add_user.run_sql_stmts('source %s'%tools.analyticsTools[name]['env'], stmts, hostname, port, dbname, username)

            # 4. Load data
            sql_source = tools.analyticsTools[name]['sql_source']
            os.system('gunzip -c %s > %s'%(sql_source, sql_source[:(len(sql_source)-3)]))
            os.system('source %s && psql -h %s -d %s -p %s -f %s'%(tools.analyticsTools[name]['env'], hostname, dbname, port, sql_source[:(len(sql_source)-3)]))
            os.system('rm %s'%sql_source[:(len(sql_source)-3)])
          
            # Tear Down
            os.system('source %s && pg_ctl stop -D %s'%(tools.analyticsTools[name]['env'], tools.analyticsTools[name]['master_dir']))
            
        else:
        
            os.system('source %s && gpstart -a -d %s'%(tools.analyticsTools[name]['env'], tools.analyticsTools[name]['master_dir'] ))

            hostname = tools.analyticsTools[name]['host']
            username = tools.analyticsTools[name]['username']
            superusername = tools.analyticsTools[name]['superusername']
            dbname = tools.analyticsTools[name]['database']
            dbtemplate = 'template1'
            port = tools.analyticsTools[name]['port']

            # 0. Update pg_hba to allow madlibtester to access all databases
            add_user.addUserPGHBA(tools.analyticsTools[name]["master_dir"])
            # clear up
            stmts = []
            stmts.append('DROP DATABASE %s '%dbname)
            stmts.append('DROP USER %s '%username)
            add_user.run_sql_stmts('source %s'%tools.analyticsTools[name]['env'], stmts, hostname, port, dbtemplate, superusername)


            # 1. Create non super user with super user
            stmts = []
            stmts.append('CREATE USER %s WITH CREATEDB'%username)
            add_user.run_sql_stmts('source %s'%tools.analyticsTools[name]['env'], stmts, hostname, port, dbtemplate, superusername)

            # 2. Create database with non super user
            stmts = []
            stmts.append('CREATE DATABASE %s '%dbname)
            add_user.run_sql_stmts('source %s'%tools.analyticsTools[name]['env'], stmts, hostname, port, dbtemplate, username)

            # 3. Create schema with non super user and created database
            stmts = []
            stmts.append('CREATE SCHEMA %s'%DataSchema)
            stmts.append('CREATE SCHEMA %s'%ResultSchema)
            add_user.run_sql_stmts('source %s'%tools.analyticsTools[name]['env'], stmts, hostname, port, dbname, username)

            # 4. Load data
            sql_source = tools.analyticsTools[name]['sql_source']
            os.system('gunzip -c %s > %s'%(sql_source, sql_source[:(len(sql_source)-3)]))
            os.system('source %s && psql -h %s -d %s -p %s -f %s'%(tools.analyticsTools[name]['env'], hostname, dbname, port, sql_source[:(len(sql_source)-3)]))
            os.system('rm %s'%sql_source[:(len(sql_source)-3)])

            src_db_conn = username + '@' + hostname + ':' + port + '/' + dbname
            dest_db_conn = getResultDBConnection()
            data_stat_cmd = './dataset_stat.py --src src_db_conn  --dest dest_db_conn'

            # Tear Down
            os.system('source %s && gpstop -a -d %s'%(tools.analyticsTools[name]['env'], tools.analyticsTools[name]['master_dir']))


