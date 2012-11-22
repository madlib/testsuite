#!/usr/bin/env python
import os, sys
from xml_parser import Parser
import dbManager
 
class Configer(Parser):
    """Parse test config spec file, which includes the result database connection 
    and template executor dir
    """

    def __init__(self, fileName):
        Parser.__init__(self, fileName)
    
    def testconfig(self):
        """Parse logger database connection to store result."""
        try:
            configuration       =   Parser.getNodeTag(self, self.xmlDoc, "configuration")
            metadatadb          =   Parser.getNodeTag(self, configuration, "metadatadb")        
            self.user                =   Parser.getNodeVal(self, metadatadb, "user")
            self.host                =   Parser.getNodeVal(self, metadatadb, "host")
            self.port                =   Parser.getNodeVal(self, metadatadb, "port")
            self.database            =   Parser.getNodeVal(self, metadatadb, "database")
            self.resultDBSchema      =   Parser.getNodeVal(self, metadatadb, "schema")

            self.resultDBconf = {'username':self.user, 'host':self.host, 'port':self.port, \
                       'database':self.database, 'schema':self.resultDBSchema, 'name':'resultDB'}
            self.dbmanager = dbManager.dbManager(self.resultDBconf)
 
        except Exception, exp:
            print str(exp)
            print "Error when parsing testConfig"
            sys.exit()

    def getResultDBmanager(self):
        """Return the result DB manager"""
     
        return self.dbmanager

    def getResultSchema(self):
        """Return the result DB schema"""

        return self.resultDBSchema
    
    def getResultDBconf(self):
        """Return the result DB configuration"""

        return self.resultDBconf

if __name__ == '__main__':
    testConfig = Configer("../../testspec/metadata/testconfig.xml")
    testConfig.testconfig()
