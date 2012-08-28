#!/usr/bin/env python
"""This file contains method to execute case in database."""
import os
import sys
import time
import commands

from run_sql import *
from file_path import *
from analytics_tool import *
from test_config import *

class TestCaseExecutor:
    """Test Case Executor"""

    def __init__(self, cases, platform):
        """change param list to only cases and platform.
    
        param cases: cases to execute.
        param platform: platform chosen to run case. i.e. GPDB_4.2.0
        """
        self.case_dir  =  Path.casePath
        self.cases     =  cases
        self.platform  = platform
        self.init_sql  = """psql -c 'drop schema if exists madlibtestresult cascade; create schema madlibtestresult;' """
        self.version = None
        self.tools_conf_map = None
 
    def __parseAnalyticsToolXML(self, xml_file):
        """Parse AnalyticsTool.xml, get the configuration information of analytics tool
    
        return map {name of tool to it's configuration}, and the configuration is name-value map
        """
        try:
            analyticsTools = AnalyticsTools(xml_file)
            analyticsTools.parseTools()
        except Exception, exp:
            print str(exp)
            print "Error when parsing analyticsTools"
            sys.exit()
        self.tools_conf_map = analyticsTools.analyticsTools
        return analyticsTools.analyticsTools
    
    def __run_cmd(self, cmd):
        """Run shell command."""
        os.system(cmd)
    
    def __operateTool(self, name, operation):
        """Manipulate the analytics tool, such as start or stop
    
        param name, name of the analytics tool
        param operation, str should be 'start' or 'stop'
        """
        if not name in self.tools_conf_map:
            sys.exit('ERROR: Wrong platform name.')
        conf = self.tools_conf_map[name]
        source_path = 'source ' + conf['env'] + '&& '
        master_dir = conf['master_dir']
        if conf['kind'] == 'postgres':
            if operation == 'start':
                self.__run_cmd(source_path + 'pg_ctl start -D ' + master_dir)
                time.sleep(10)
            elif operation == 'stop':
                self.__run_cmd(source_path + 'pg_ctl stop -D ' + master_dir)
        elif conf['kind'] == 'greenplum':
            if operation == 'start':
                self.__run_cmd(source_path + 'gpstart -a -d ' + master_dir)
            elif operation == 'stop':
                self.__run_cmd(source_path + 'gpstop -a -d ' + master_dir)
    
    def __executeInitCase(self, analytics_tool):
        """Execute initial sql to drop output schema, and than create it.
        
        param analytics_tool, name of the analytics tool.
        """
        conf = self.tools_conf_map[analytics_tool]
        source_path = 'source ' + conf['env'] + ' && '
        username, hostname, port, database \
            = conf['username'], conf['host'], conf['port'], conf['database']
        psql_append =  " -U "               +       username \
                     + " -h "               +       hostname \
                     + " -p "               +       port \
                     + " -d "               +       database
        cmd = source_path + self.init_sql + psql_append
        self.__run_cmd(cmd)
        
    def __executeCaseFile(self, file_name, analytics_tool, run_id, restart = False):
        """Execute case file with specific greenplum/postgres configuration.
    
        param file_name, file name of case
        param analytics_tool, name of the analytics tool
        param run_id, the index of this run
        param restart, dose the test need to restart greenplum
        """

        conf = self.tools_conf_map[analytics_tool]
        source_path = 'source ' + conf['env'] + '&& '
        username, hostname, port, database \
            = conf['username'], conf['host'], conf['port'], conf['database']
        psql_append =  " -U "               +       username \
                     + " -h "               +       hostname \
                     + " -p "               +       port \
                     + " -d "               +       database
        connection_str = username + '@' + hostname + ':' + port + '/' + database
        exe_append  =   " -C "              +       connection_str \
                     + " --run_id "         +       str(run_id) \
                     + " --analyticstool "  +       analytics_tool
        lines = open(file_name).readlines()
        for line in lines:
            line = line.strip()
            if line and line[0] != '#':
                if line.startswith('psql '):
                    #this is a psql command, need to add connection options
                    cmd = source_path + line + psql_append
                else:
                    #this is a executor command, need to add run_id and connection options
                    if restart:
                        self.__operateTool(analytics_tool, 'stop')
                        self.__operateTool(analytics_tool, 'start')
                    cmd = line + exe_append
                self.__run_cmd(cmd)

    def __executeCaseWithTool(self, test_case_path, analytics_tool, run_id, restart = False):
        """Execute case files with specific greenplum/postgres configuration
        
        param test_case_path, path of all test case
        param analytics_tool, name of the analytics tool
        param run_id, the index of this run
        param restart, dose the test need to restart greenplum
        """
        for case_name in self.cases:
            case_name = case_name.strip()
            case_file = os.path.join(test_case_path, case_name)+'.case'
            self.__executeCaseFile(case_file, analytics_tool, run_id, restart)

    def insertOneSkip(self, case, run_id):
        sql = ""
        cmd = ""
        self.__run_cmd(cmd) 
   
    def executeStart(self, tool_xml_file):
        self.__parseAnalyticsToolXML(tool_xml_file)
        self.__operateTool(self.platform, 'start')
        self.__executeInitCase(self.platform)

    def executeOneCase(self, test_case_path,analytics_tool, case_name, run_id, restart = False):
        case_name = case_name.strip()
        case_file = os.path.join(test_case_path, case_name)+'.case'
        self.__executeCaseFile(case_file, analytics_tool, run_id, restart)

    def executeStop(self):
        conf = self.tools_conf_map[self.platform]
        source_path = 'source ' + conf['env'] + ' && '
        username, hostname, port, database \
            = conf['username'], conf['host'], conf['port'], conf['database']
        psql_append =  " -U "               +       username \
                     + " -h "               +       hostname \
                     + " -p "               +       port \
                     + " -d "               +       database
        version_sql = """psql -c 'select madlib.version();' """
        cmd =  source_path + version_sql + psql_append
        output = commands.getoutput(cmd)
        try:
            temp_list = output.split(',')
            self.version = temp_list[2]
        except Exception:
            self.version = "WRONG"

        self.__operateTool(self.platform, 'stop')

 
    def executeCase(self, test_case_path, tool_xml_file, run_id, restart = False):
        """Execute case files with specific greenplum/postgres configuration
        
        param test_case_path, path of all test case
        param tool_xml_file, name of the analytics tool xml file
        param run_id, the index of this run
        param restart, dose the test need to restart greenplum
        """  
        self.__parseAnalyticsToolXML(tool_xml_file)
        self.__operateTool(self.platform, 'start')
        self.__executeInitCase(self.platform)
        self.__executeCaseWithTool(test_case_path, self.platform, run_id, restart)

        conf = self.tools_conf_map[self.platform]
        source_path = 'source ' + conf['env'] + ' && '
        username, hostname, port, database \
            = conf['username'], conf['host'], conf['port'], conf['database']
        psql_append =  " -U "               +       username \
                     + " -h "               +       hostname \
                     + " -p "               +       port \
                     + " -d "               +       database
        version_sql = """psql -c 'select madlib.version();' """
        cmd =  source_path + version_sql + psql_append
        output = commands.getoutput(cmd)
        try:
            temp_list = output.split(',')
            self.version = temp_list[2]
        except Exception:
            self.version = "WRONG"
        
        self.__operateTool(self.platform, 'stop')

