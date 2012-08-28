#!/usr/bin/env python
"""Parse <test_suite> tag. """
import os

from xml_parser import *
from test_config import *
from analytics_tool import *
from testcase import *
from para_handler import *

class TestSuite(Parser):
    
    def __init__(self, tsNode, tsType, configer, analyticsTools, datasets,paraHandler, \
                 algorithm, preParas, caseScheduleHd, caseSQLFileHd, tsSqlFileHd, tiSqlFileHd):
        """
        params: 
            tsNode: test suite node
            tsType: test suite type
            configer: configer class from test_config
            analyticsTools: all alalyticsTool info map
            datasets: dataset class to parse dataset xml
            paraHandler
            algorithm: name
            preParas: prepared parameters
            caseScheduleFileHd: test case schedule file descriptor
            caseSQLFileHd: test case sql out file descriptor
            tsSqlFileHd: sql file of inserting test suite, as tsSqlF in __init__'s para
            tiSqlFileHd: sql file of inserting test case, as tiSqlF in __init__'s para        
        """ 

        self.tsNode         =   tsNode
        self.tsName         =   Parser.getNodeVal(self, tsNode, "name")
        self.tsComments     =   Parser.getNodeVal(self, tsNode, "comments")
        self.tsExecuterate  =   Parser.getNodeVal(self, tsNode, "execute_rate")
        self.tsType         =   tsType
        self.configer       =   configer
        self.analyticsTools =   analyticsTools
        self.datasets       =   datasets
        self.paraHandler    =   paraHandler
        self.algorithm      =   algorithm
        self.preParas       =   preParas
        self.caseScheduleHd =   caseScheduleHd
        self.caseSQLFileHd  =   caseSQLFileHd
        self.testSuiteSqlHd =   tsSqlFileHd
        self.testItemSqlHd  =   tiSqlFileHd
        self.numExtendParams = 1
        # List of method's extent vary parameter pairs 
        self.varExpandParaPairList = []                
        
    def GenCases(self, debug):
        """Generate test case under this <test_suites> tag."""

         # generate varparameters pair 
        self.__varParaPairs()
        
        # write test case name and comments 
        self.caseScheduleHd.write("# TestSuite Name : " + self.tsName + "\n")
        self.caseScheduleHd.write("# " + self.tsComments + "\n")
        self.caseScheduleHd.write("# Start TestSuite ==========\n")
        self.caseSQLFileHd.write( "-- TestSuite Name : " + self.tsName + "\n")
        
        # generate case command for each test suite(including several method)
        mtdList = Parser.getNodeList(self, self.tsNode, "method")
        
        # testsuite exec iteration
        i = 0 
        for i in range(0, int(self.tsExecuterate)):
            for caseID in range(0, self.numExtendParams):
                caseName = self.tsName + '_' + str(0) + '_' + str(caseID)
                caseFileHD = open(os.path.join(Path.casePath, caseName + '.case'), 'a+')
                case = TestCase(self.configer, self.analyticsTools, self.datasets, \
                            self.algorithm, self.paraHandler, self.preParas, \
                            self.varExpandParaPairList, self.tsName, caseName, caseID, \
                            caseFileHD, self.caseScheduleHd, self.caseSQLFileHd, \
                            self.testSuiteSqlHd, self.testItemSqlHd)
                case.GenCase(mtdList, i, self.tsType, debug)
                if i==0:
                    self.caseScheduleHd.write(caseName + '\n')
            self.caseScheduleHd.write("\n")
            # end one testsuite
            if i==0: 
                self.__writeTestSuitesSql()
            
        self.caseSQLFileHd.write( "\n\n")
    
    def __varParaPairs(self):
        """Generate list_parameters combination from <list_parameter> tag
        , which is under <method> tag
        """

        mtdList = Parser.getNodeList(self, self.tsNode, "method")
        for mtd in  mtdList:
            mtdName = Parser.getNodeVal(self, mtd, "name")
            
            varParaList = []
            try:
                varParaList = Parser.getNodeList(self, mtd, "list_parameter")
            except Exception, exp:
                print 'No list_parameter'
                return
            if not varParaList:
                return
            
            varParaMap = {}
            # for the vary parameters in the different methods of one test case, 
            #    the number of parameter combination should be the same, 
            #    or should be only one.
            numExtendParams = 1
            for varParaNode in varParaList:
                varParaName = ""
                varParaValList = []
                try:
                    varParaName = Parser.getNodeVal(self, varParaNode, "name")
                    varParaValList = Parser.getNodeVals(self, varParaNode
                                                        , "value")
                except Exception:
                    pass  
             
                if varParaName and len(varParaValList):
                    varParaMap[varParaName] = varParaValList
                    # multiply numExtendParams with number of varParaValList
                    numExtendParams = numExtendParams * len(varParaValList)
                        
            #check whether numExtendParamsPerMethod are the same
            if self.numExtendParams == 1:
                self.numExtendParams = numExtendParams
            elif self.numExtendParams != numExtendParams:
                e = 'ERROR IN %s, %s: Different methods have the \
                    different number of vary parameters' % (self.tsName, mtdName)
                raise Exception(e)
   
            self.varExpandParaPairList.append( self.__recurse_expand(varParaMap) )
    
    def __recurse_expand(self, map_lists, head = {}):
        """recursively expand parameter combination
        
        params:
            map_lists is an map, each element is the name to [values] list pair
            head is an list of map, each element of the map is the name to value pair
        return: expanded parameter combination in list
        """

        #exit condition
        if len(map_lists) == 0:
            return head
        #exit condition
        elif len(map_lists) == 1:
            for name, var_list in map_lists.items():
                result_list = []
                for var in var_list:
                    head[name] = var
                    result_list.append(head.copy())
                    del head[name]
            return  result_list
        else:
            #recursive call
            expand_list = []
            expand_name = ''
            for name, var_list in map_lists.items():
                expand_list = var_list
                expand_name = name
            del map_lists[expand_name]
            new_head_list = []
            for var in expand_list:
                #add head list
                head[name] = var
                #copy head map to a new head list
                new_head_list.append(head.copy())
                #clear head list
                del head[name]
                
            result_list = []
            #recursive call for each element of new_head_list
            result_map_lists = [ self.__recurse_expand(map_lists, h) 
                                for h in new_head_list ]
            map_lists[name] = var_list
            
            result = []
            for r in result_map_lists:
                result.extend(r)
            return result

    def __writeTestSuitesSql(self):
        """write test suite sql statement"""

        tbName = self.configer.metaDBSchema + "." + "testsuites"
        samePara = self.__formSamePara(self.preParas)
        stmt = "INSERT INTO " + tbName + " VALUES (" \
            "'" + self.tsName + "'" + ", " +\
            "'" + self.tsType + "'" + ", " +\
            str(self.numExtendParams) + ", " +\
            "'" + self.tsComments + "'" + ", " +\
            "'" + samePara + "'" + \
            ");"
        self.testSuiteSqlHd.write(stmt + "\n\n")
    
    def __formSamePara(self, prePara):
        samePara = []
        for _, value in prePara.items():
            samePara.extend(value)
        return ','.join(samePara)
