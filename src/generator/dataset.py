#!/usr/bin/env python
"""Define a class to parse the dataset xml file."""
import sys
sys.path.append('../')

from utility.xml_parser import Parser

class Datasets(Parser):
    """Parse the dataset xml file."""
    
    def __init__(self, fileName):
        """fileName of dataset xml file.""" 
        Parser.__init__(self, fileName)
        self.num = 0   # number of datasets
        self.descs = {} # Key: datasetName  Value: dic(method : parameter)

    def getDataSets(self):
        """Return map of datasets.
    
        key: datasetName  value: dic(method : parameter)
        """
        datasets = Parser.getNodeTag(self, self.xmlDoc,"datasets")

        dsList = Parser.getNodeList(self, datasets, "dataset")

        self.num = len(dsList)
        for dsNode in dsList:
            self.__getDataSet(dsNode)
        return self.descs

    def __getDataSet(self, dsNode):
        desc = {}
        dsName = Parser.getNodeVal(self, dsNode, "name")
        dsRows = Parser.getNodeVal(self, dsNode, "rows")
        desc["rows"] = dsRows

        mtList = Parser.getNodeList(self, dsNode, "method")
        for mt in mtList :
            mtName = Parser.getNodeVal(self, mt, "name")
            paraList = Parser.getNodeList(self, mt, "parameter")

            paras = []
            for para in paraList:
                pName = Parser.getNodeVal(self, para, "name")
                pVal = Parser.getNodeVal(self, para, "value")
                paras.append("--" + pName + " " + pVal)
            desc[mtName] = " ".join(paras)  # method's parameter string

        self.descs[dsName] = desc
