#!/usr/bin/env python
"""generate test case from each <testsuite> tag, including using differenct iteration 
number and different vary parameter combinations
"""
import os
import pipes

from xml_parser import *
from file_path import *
from test_config import *
from analytics_tool import *
from para_handler import *
import template_executor


class TestCase(Parser):
    """
    @param configer, configer class from test_config
    @param analyticsTools, all alalyticsTool info map
    @param datasets, dataset class to parse dataset xml
    @param paraHandler
    @param algorithm, name
    @param preParas, prepared parameters
    @param varParaPairs, vary parameters combination
    @param tsName, test suite name
    @param caseName, test case name
    @param varyParaId, index of vary parameter combinations
    @param caseScheduleFileHd, test case schedule file descriptor
    @param caseSQLFileHd, test case sql out file descriptor
    @param tsSqlFileHd, sql file of inserting test suite, as tsSqlF in __init__'s para
    @param tiSqlFileHd, sql file of inserting test case, as tiSqlF in __init__'s para        
    """
    def __init__(self, configer, analyticsTools, datasets, algorithm, paraHandler,\
                 preParas, varParaPairs, tsName, caseName, varyParaId, \
                 caseFileHd, caseScheduleFileHd, caseSQLFileHd, tsSqlFileHd, tiSqlFileHd):
        """          
        params:
            configer: configer class from test_config
            analyticsTools: all alalyticsTool info map
            datasets: dataset class to parse dataset xml
            paraHandler          
            algorithm: name
            preParas: prepared parameters
            varParaPairs: vary parameters combination
            tsName: test suite name  
            caseName: test case name 
            varyParaId: index of vary parameter combinations
            caseScheduleFileHd: test case schedule file descriptor
            caseSQLFileHd: test case sql out file descriptor
            tsSqlFileHd: sql file of inserting test suite, as tsSqlF in __init__'s para
            tiSqlFileHd: sql file of inserting test case, as tiSqlF in __init__'s para        
        """ 

        self.configer           =   configer
        self.analyticsTools     =   analyticsTools
        self.datasets           =   datasets

        self.tsName             =   tsName
        self.caseName           =   caseName
        self.algorithm          =   algorithm
        self.paraHandler        =   paraHandler
        self.varParaPairs       =   varParaPairs
        self.preParas           =   preParas
        self.varyParaId         =   varyParaId

        self.caseFileHd         =   caseFileHd
        self.caseScheduleFileHd =   caseScheduleFileHd
        self.caseSQLFileHd      =   caseSQLFileHd
        self.testSuiteSqlH      =   tsSqlFileHd
        self.testItemSqlHd      =   tiSqlFileHd

    def __writeTestItemsSql(self, itemName, algorithm, method, \
            paras, varVal, varName, dataset, rownum):

        itemsTbName = self.configer.metaDBSchema + "." + "testitems"

        stmt = "INSERT INTO " + itemsTbName + " VALUES ( '" + itemName  + "' ," + \
            "'" + self.tsName       + "' ," +\
            "'" + self.caseName     + "' ," +\
            "'" + self.algorithm    + "' ," +\
            "'" + method            + "' ," +\
            "$_parasString$" \
                + paras             + "$_parasString$ ," +\
            "$_valueString$" \
                + varVal            + "$_valueString$ ," +\
            "'" + varName           + "' ," +\
            "'" + dataset           + "' ," +\
            str(rownum) + ");"
        self.testItemSqlHd.write(stmt + "\n\n")

    def GenCase(self, mtdList, exeIteration, tsType, debug):
        """generate each test case
    
        params:    
            mtdList: method node list
            exeIteration: number of this execution iteration
            tsType: test suite type
        """

        mtSeq = 0
        for mtd in  mtdList:

            caseItem = []
            caseItemPara = []
            caseItemRows = 0
            caseItemDataset = ""

            # test case start command, which is the path of template executor
            caseItem.append(Path.templateExecutorPath)

            # try to attach pre parameters, it is optional
            try :
                mtdName = Parser.getNodeVal(self, mtd , "name")

                #configuration for template executor
                caseItemPara.append("--method "+mtdName)
                caseItemPara.append("--algorithm "+self.algorithm)
                caseItemPara.append("--spec_file "+ os.path.join( 
                    os.getcwd(), Path.CfgSpecPath + Path.algorithmsSpecXml))

                caseItemPara.extend(self.preParas[mtdName])

            except Exception, exp:
                #print 'pre', str(exp)
                pass

            # try to attach parameters, it is optional
            try :
                paraList = Parser.getNodeList(self, mtd,"parameter")
                for para in paraList:
                    paraName = Parser.getNodeVal(self, para, "name")
                    paraVal  = Parser.getNodeVal(self, para, "value")
                    caseItemPara.extend(self.paraHandler.handle(paraName, 
                        paraVal, "para", mtdName))

                    if "dataset" == paraName :
                        caseItemDataset = paraVal
                    if "rows" == paraName :
                        caseItemRows = paraVal
            except Exception, exp:
                pass

            # try to attach var parameter, it is optional
            varValues = ''
            varNames = ''
            try :
                # varPara table for this method
                varParaTb = self.varParaPairs[mtSeq][self.varyParaId]
                varNames = ','.join(varParaTb.keys())
                varValues = ','.join(varParaTb.values())
                for varName, varValue in varParaTb.items():

                    if "dataset" == varName :
                        caseItemDataset = varValue
                        caseItemPara.append(self.paraHandler.handle(\
                                varName, varValue, "var", mtdName))

                    elif "rows" == varName :
                        caseItemRows = varValue
                        caseItemPara.append("--" + varName + " " + \
                                pipes.quote(self.paraHandler.handle(varName, varValue, "var", mtdName)))
                    else:
                        caseItemPara.append("--" + varName + " " + \
                                pipes.quote(self.paraHandler.handle(varName, varValue, "var", mtdName)))

            except Exception, exp:
                pass

            #basename = suite name + method name + vary param id + method id
            targetBaseName = self.caseName + "_" + mtdName + "_" + str(mtSeq)
            mtSeq = mtSeq + 1

            #madlib logger configuration
            caseItemPara.append("--iteration_id " + str(exeIteration))
            caseItemPara.append("--logger DbOut " + "--logger_conn " + self.configer.metaDB)
            caseItemPara.append("--target_base_name " + targetBaseName)

            caseItem.extend(caseItemPara)

            #write madlib executor to .case and generate sql file to .sql_out
            madlibCMD = " ".join(caseItem)
            self.caseFileHd.write(madlibCMD + "\n")

            if debug is True:
                try:
                    exe = template_executor.Executor(madlibCMD.split(), None)
                    exe.parseArgument()
                    if exeIteration == 0:
                        self.caseSQLFileHd.write( "-- method: " + targetBaseName + "\n")
                        self.caseSQLFileHd.write( exe.generateSQL() + "\n\n")
                except Exception, exp:
                    print str(exp)

            # try to add tear down operation, this step is optional
            try:
                teardown = Parser.getNodeVal(self, mtd, "tear_down")
                teardownSQL = "psql -c '" + teardown + "'" 
                self.caseFileHd.write(teardownSQL)
                self.caseSQLFileHd.write( teardownSQL )
            except Exception, exp:
                pass

            self.caseFileHd.write("\n")

            if exeIteration == 0:
                self.__writeTestItemsSql(targetBaseName, self.algorithm,
                mtdName, ",".join(caseItemPara), varValues, varNames, \
                caseItemDataset,caseItemRows)

        self.caseFileHd.write("\n")

