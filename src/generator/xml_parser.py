#!/usr/bin/env python
"""
XML Parser utility
"""
from xml.dom.minidom import parse, parseString


class Parser:
    def __init__(self, fileName):
        try : 
            self.fileName = fileName
            self.xmlDoc = parse(fileName)
        except Exception:
            print "Xml \"%s\" Format is Invalid !" % (self.fileName)
            raise
        
    def getNodeTag(self, node, name):
        """ get node tag by node name 
        
        params:    
            node xml node
            name, tag name
        return xml node
        """

        return node.getElementsByTagName( name )[0]
    
    def getNodeVal(self, node, name):
        """ get node value by node name 
        
        params:
            node xml node
            name, tag name
        return text
        """     

        try:
            value = node.getElementsByTagName(name)[0].childNodes[0].data.strip().encode('ASCII')
        except Exception:
            value = None
        return value

    def getNodeVals(self, node, name):
        """ get node values by node name
   
        params:
            node xml node
            name, tag name
        return text
        """

        try:
            valsList = []
            alist = node.getElementsByTagName( name )
            for a in alist:
                para = a.firstChild.data
                valsList.append( para )
        except Exception:
            values = ''
        return valsList
    
    def getNodeList(self, node, name):
        """ get node list by node name

        params:
            node xml node
            name, tag name
        return xml node list
        """

        return node.getElementsByTagName( name )
