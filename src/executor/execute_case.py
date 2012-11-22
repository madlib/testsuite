#!/usr/bin/env python
"""This file contains method to execute case in database."""
import os, sys, subprocess, time

sys.path.append('../')
from utility import run_sql, tools, file_path, dbManager
Path = file_path.Path()

class TestCaseExecutor:
    """Test Case Executor"""

    def __init__(self, cases, cur_dbconf, platform):
        """
        param cases: cases to execute.
        param cur_dbconf: current test DB configuration.
        """
        self.cases     =  cases
        self.init_sql  = "drop schema if exists madlibtestresult cascade; create schema madlibtestresult;"
        self.version = None
        self.cur_dbconf = cur_dbconf 
        self.platform = platform
        self.dbManager = dbManager.dbManager(cur_dbconf)
        self.psql_append = self.dbManager.getDBsqlArgs()
 
    def __executeCaseFile(self, file_name, run_id, restart = False):
        """Execute case file with specific greenplum/postgres configuration.
    
        param file_name, file name of case
        param run_id, the index of this run
        param restart, dose the test need to restart greenplum
        """

        connection_str = self.dbManager.getDBConnection()
        exe_append  =   " -C "              +       connection_str \
                     + " --run_id "         +       str(run_id) \
                     + " --analyticstool "  +       self.platform
        lines = open(file_name).readlines()
        
        for line in lines:
            line = line.strip()
            if line and line[0] != '#':
                if line.startswith('psql '):
                    #this is a psql command, need to add connection options
                    line = line.replace("psql ","")
                    run_sql.runSQL(line, psqlArgs = self.psql_append, source_path = self.cur_dbconf['env'])
                else:
                    #this is a executor command, need to add run_id and connection options
                    if restart:
                        self.dbManager.stop()
                        self.dbManager.start()
                    subprocess.call(line + exe_append, shell = True)

    def __executeCaseWithTool(self, test_case_path, run_id, restart = False):
        """Execute case files with specific greenplum/postgres configuration
        
        param test_case_path, path of all test case
        param run_id, the index of this run
        param restart, dose the test need to restart greenplum
        """
        for case_name in self.cases:
            case_name = case_name.strip()
            case_file = os.path.join(test_case_path, case_name)+'.case'
            self.__executeCaseFile(case_file, run_id, restart)

   
    def executeCase(self, test_case_path, run_id, restart = False):
        """Execute case files with specific greenplum/postgres configuration
        
        param test_case_path, path of all test case
        param run_id, the index of this run
        param restart, dose the test need to restart greenplum
        """  
        self.dbManager.start()
        run_sql.runSQL(self.init_sql, psqlArgs = self.psql_append, source_path = self.cur_dbconf['env'])

        self.__executeCaseWithTool(test_case_path, run_id, restart)
        
        version_sql = 'select madlib.version();' 
        output = run_sql.runSQL(version_sql, psqlArgs = self.psql_append, source_path = self.cur_dbconf['env'])
        try:
            temp_list = output.split(',')
            self.version = temp_list[2]
        except Exception:
            self.version = "WRONG"
       
        self.dbManager.stop()

