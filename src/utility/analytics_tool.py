#!/usr/bin/env python
"""Parse analytics_tools xml spec

The MADmark supports both postgres and greenplum.
"""
import sys
from xml_parser import Parser

class AnalyticsTools(Parser):
    """this file include analytics tool table description and tool configuration."""
    
    def __init__(self, fileName):
        """fileName: xml spec file name""" 
        Parser.__init__(self, fileName)
        self.analyticsTools = {}
        self.analyticsToolTb = None
        self.colsDesc = None
        self.sqlFileHd = None
        self.cmdFileHd = None

    def parseTools(self):
        """parse the xml file and generate sql file""" 
        try:
            analytics_tools = Parser.getNodeTag(self, self.xmlDoc, "analytics_tools")
            atList = Parser.getNodeList(self, analytics_tools, "analytics_tool")

            for at in atList:
                atDic = {}
                name = Parser.getNodeVal(self, at, "name")
                kind = Parser.getNodeVal(self, at, "kind")
               
                if kind.lower() in( "greenplum", "postgres"):
                    atDic["name"]           =   Parser.getNodeVal(self, at, "name")
                    atDic["kind"]           =   Parser.getNodeVal(self, at, "kind")
                    atDic["host"]           =   Parser.getNodeVal(self, at, "host")
                    atDic["port"]           =   Parser.getNodeVal(self, at, "port")
                    atDic["superuser"]  =   Parser.getNodeVal(self, at, "superuser")
                    atDic["database"]       =   Parser.getNodeVal(self, at, "database")
                    atDic["username"]       =   Parser.getNodeVal(self, at, "user")
                    atDic["master_dir"]     =   Parser.getNodeVal(self, at, "master_dir")
                    atDic["env"]    =   Parser.getNodeVal(self, at, "env")
                self.analyticsTools[name] = atDic

        except Exception, exp:
            print str(exp)
            print "Error when parsing analyticsTools"
            sys.exit()
  
    def generateSqlCmdfile(self, tbName, sqlFile, cmdFile):
        """generate sql file and initial command for each tool
        
        param tbName: analytics tool table name
        param sqlFile: sql file name for analytics tool
        param cmdFile: initial command file for each analytics tool
        """

        self.analyticsToolTb    = tbName
        #analyticsToolTb column description columnName:Type
        self.colsDesc           = {}      
        self.sqlFileHd          = open(sqlFile, "w")
        self.cmdFileHd          = open(cmdFile, "w")
        
        self.__createToolsTb()
        self.__insertToolsInfo()
        self.__generateInitialCMD()

    def __generateInitialCMD(self):
        """Generate initial command for each tool, such as drop result schema"""
        cmd = []
        gpstartCmd = 'gpstart -a -d %s \n'
        initialSchema = 'drop schema if exists madlibtestresult cascade; \
                        create schema madlibtestresult; \n'
        gpstopCmd = 'gpstop -a -d %s \n'
        for _, tool in self.analyticsTools.items():
            if tool["kind"] == 'greenplum':
                cmd.append(gpstartCmd % tool["master_dir"])
            elif tool["kind"] == 'postgres':
                pass
            cmd.append("psql -c '" + initialSchema + "'" + \
                                      " -h " + tool["host"] + \
                                      " -d " + tool["database"] + \
                                      " -U " + tool["username"] + \
                                      " -p " + tool["port"] + \
                                      "\n")
            if tool["kind"] == 'greenplum':
                cmd.append(gpstopCmd % tool["master_dir"])
            elif tool["kind"] == 'postgres':
                pass
        self.cmdFileHd.write('\n'.join(cmd))
        return cmd

    def __createToolsTb(self):
        """create the analytics tool table"""
        
        # drop table stmt
        stmt = "DROP TABLE IF EXISTS " + self.analyticsToolTb + " cascade;\n\n"
        self.sqlFileHd.write(stmt)

        analytics_tools = Parser.getNodeTag(self, self.xmlDoc, "analytics_tools")

        metadata = Parser.getNodeTag(self, analytics_tools, "metadata")
        colList = Parser.getNodeList(self, metadata, "column")

        for col in colList:
            colName = Parser.getNodeVal(self, col, "name")
            colType = Parser.getNodeVal(self, col, "type")
            self.colsDesc[colName] = colType

        # convert dict
        cols = self.dicToArray(self.colsDesc)

        # create table stmt
        stmt = "CREATE TABLE " + self.analyticsToolTb + "("
        stmt = stmt + ','.join(cols) + ");\n\n"
        self.sqlFileHd.write(stmt)

    def __insertToolsInfo(self):
        """insert the configuration of analytics to the table"""
        for _, tool in self.analyticsTools.items():
            stmt = self.__formInsertStmt(self.analyticsToolTb, tool)
            self.sqlFileHd.write(stmt + "\n")

    def dicToArray(self, dictory):
        """format the dict to array
        
        param dict, a dict
        return an array
        """

        array = []
        for key, value in dictory.items():
            array.append(key + ' '+ value)
        return array

    def __formInsertStmt(self, tbName, tableValue):
        """format the insert statement"""
        attrs = []
        values = []

        for attr, value in tableValue.items():
            attrs.append (attr)
            if self.colsDesc[attr] == "int" or self.colsDesc[attr] == "smallint":
                values.append(value)
            else:
                values.append('\'' + value + '\'')

        stmt = "INSERT INTO " + tbName + "("+ ','.join(attrs) + ")" + \
                  "VALUES" + "("+ ','.join(values) +");"
        return stmt
