#!/usr/bin/env python
"""Parse the root tag <multi_test_suites> of test case spec xml"""

from xml_parser import *
from test_config import *
from analytics_tool import *
from testsuite import *
from para_handler import *

class MultiTestSuite(Parser):
    def __init__(self, configer, analyticsTools, datasets, paraHandler, algorithm, preParas, \
            tsNodes, tsType, caseScheduleFileHd, caseSQLFileHd, tsSqlFileHd, tiSqlFileHd):
        """
        params:
            configer: configer class from test_config
            analyticsTools: all alalyticsTool info map
            datasets: dataset class to parse dataset xml
            paraHandler
            algorithm: name
            preParas: prepared parameters
            tsNodes: test suite node list
            tsType: test suite type
            caseScheduleFileHd: test case schedule file descriptor
            caseSQLFileHd: test case sql out file descriptor
            tsSqlFileHd: sql file of inserting test suite, as tsSqlF in __init__'s para
            tiSqlFileHd: sql file of inserting test case, as tiSqlF in __init__'s para        
        """
        self.configer           =   configer
        self.analyticsTools     =   analyticsTools 
        self.datasets           =   datasets
        self.algorithm          =   algorithm
        self.preParas           =   preParas      
        self.paraHandler        =   paraHandler
        self.tsNodes            =   tsNodes      
        self.tsType             =   tsType
        self.caseScheduleFileHd =   caseScheduleFileHd
        self.caseSQLFileHd      =   caseSQLFileHd
        self.testSuiteSqlHd     =   tsSqlFileHd
        self.testItemSqlHd      =   tiSqlFileHd
      
    def GenCases(self, debug):
        """Generate test case under this <multi_test_suites> tag."""
        for ts in self.tsNodes:         
            # Init a testsuite instance 
            testsuite = TestSuite(ts, self.tsType, self.configer, self.analyticsTools, self.datasets, \
                    self.paraHandler, self.algorithm, self.preParas, self.caseScheduleFileHd, \
                    self.caseSQLFileHd, self.testSuiteSqlHd, self.testItemSqlHd)
            testsuite.GenCases(debug)
