#!/usr/bin/env python
from xml_parser import * 

class Configer(Parser):
    """Parse test config spec file, which includes the result database connection 
    and template executor dir
    """

    def __init__(self, fileName):
        Parser.__init__(self, fileName)
        self.start = ""
        self.metaDB = ""
        self.metaDBSchema = ""
    
    def testconfig(self):
        """Parse logger database connection to store result."""

        configuration       =   Parser.getNodeTag(self, self.xmlDoc, "configuration")
        metadatadb          =   Parser.getNodeTag(self, configuration, "metadatadb")        
        self.user                =   Parser.getNodeVal(self, metadatadb, "user")
        self.host                =   Parser.getNodeVal(self, metadatadb, "host")
        self.port                =   Parser.getNodeVal(self, metadatadb, "port")
        self.database            =   Parser.getNodeVal(self, metadatadb, "database")
        self.metaDBSchema   =   Parser.getNodeVal(self, metadatadb, "schema")
        
        try:
            self.passwd = Parser.getNodeVal(self, self.metadatadb, "passwd")
            self.metaDB = self.user + "/" + self.passwd + "@" + self.host + ":" + self.port + "/" \
                            + self.database + ":" + self.metaDBSchema
        except Exception:
            self.metaDB = self.user + "@" + self.host + ":" + self.port + "/" + self.database + ":" \
                            + self.metaDBSchema


if __name__ == '__main__':
    testConfig = Configer("testconfig.xml")
    testConfig.testconfig()
