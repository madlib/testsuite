#!/usr/bin/env python
"""Store usually used pathes."""
import os

class Path:
    # config and test cases specification XML file Path
    caseSpecPath = "../../testspec/casespec/"
    CfgSpecPath   = "../../testspec/metadata/"
    
    # folder to put generated testcases, and related sql
    casePath = "../../testcase/"
    cfgPath = "../../bootstrap/"
    
    # config XML file name 
    testconfigXml = "testconfig.xml"
    datasetXml = "dataset.xml"
    analyticstoolXml = "analyticstool.xml"
    algorithmsSpecXml = "algorithmspec.xml"
    
    #template_executor path
    templateExecutorPath = os.path.join( os.getcwd(), 'template_executor.py' )

    
    
